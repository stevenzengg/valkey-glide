use rand::Rng;
use std::array;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;

pub const REQUEST_METRIC_PHASE_COUNT: usize = 12;
pub const CUSTOM_COMMAND: &str = "CUSTOM_COMMAND";

const MAX_SAMPLE_PERCENTAGE: u32 = 100;
const MAX_BUFFER_CAPACITY: usize = 1_000_000;
const MAX_ALLOWED_CUSTOM_COMMANDS: usize = 64;
const MAX_CUSTOM_COMMAND_LENGTH: usize = 64;

static REQUEST_METRICS_STATE: OnceLock<Arc<RequestMetricsState>> = OnceLock::new();
static NEXT_OPERATION_NAMESPACE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RequestMetricPhase {
    JniIngress = 1,
    ClientQueue = 2,
    CommandPrepare = 3,
    ConnectionWait = 4,
    PipelineQueue = 5,
    SocketWrite = 6,
    ResponseWait = 7,
    RetryBackoff = 8,
    CoreDecode = 9,
    CallbackQueue = 10,
    CallbackComplete = 11,
    Total = 12,
}

impl RequestMetricPhase {
    const ALL: [Self; REQUEST_METRIC_PHASE_COUNT] = [
        Self::JniIngress,
        Self::ClientQueue,
        Self::CommandPrepare,
        Self::ConnectionWait,
        Self::PipelineQueue,
        Self::SocketWrite,
        Self::ResponseWait,
        Self::RetryBackoff,
        Self::CoreDecode,
        Self::CallbackQueue,
        Self::CallbackComplete,
        Self::Total,
    ];

    const fn index(self) -> usize {
        self as usize - 1
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RequestMetricResult {
    Success = 1,
    Failure = 2,
    Timeout = 3,
    Cancelled = 4,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BoundedOperation(BoundedOperationKind);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundedOperationKind {
    Known(&'static str),
    AllowedCustom { index: u8, namespace: u64 },
    CustomCommand,
}

impl BoundedOperation {
    /// Creates an allocation-free identity for a command known at compile time.
    pub const fn known(operation: &'static str) -> Self {
        Self(BoundedOperationKind::Known(operation))
    }

    /// Creates the static fallback identity for an unknown or unlisted command.
    pub const fn custom_command() -> Self {
        Self(BoundedOperationKind::CustomCommand)
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum RequestMetricsConfigurationError {
    #[error("sample percentage must be between 0 and 100, got {percentage}")]
    InvalidSamplePercentage { percentage: u32 },
    #[error("buffer capacity must be between 1 and 1,000,000, got {capacity}")]
    InvalidCapacity { capacity: usize },
    #[error("at most 64 custom commands are allowed, got {count}")]
    TooManyAllowedCustomCommands { count: usize },
    #[error("custom command at index {index} must match [A-Z0-9_.-]{{1,64}}")]
    InvalidAllowedCustomCommand { index: usize },
    #[error("request metrics are already configured with a different capacity or allow-list")]
    ConfigurationMismatch,
    #[error("request metrics have not been configured")]
    NotConfigured,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum RequestMetricsDrainError {
    #[error("request metrics consumer mutex is poisoned")]
    ConsumerPoisoned,
}

/// Supplies a deterministic percentile to an instance-owned state.
///
/// Production callers should use [`RequestMetricsState::start`]. This hook is
/// deliberately scoped to an explicit state and cannot affect global sampling.
pub trait Sampler {
    fn sample_percentile(&mut self) -> u32;
}

struct ThreadLocalSampler;

impl Sampler for ThreadLocalSampler {
    fn sample_percentile(&mut self) -> u32 {
        rand::rng().random_range(0..MAX_SAMPLE_PERCENTAGE)
    }
}

pub struct RequestMetricsState {
    sample_percentage: AtomicU32,
    sender: SyncSender<RequestMetricSample>,
    receiver: Mutex<Receiver<RequestMetricSample>>,
    buffered_samples: AtomicUsize,
    dropped_samples: AtomicU64,
    allowed_custom_commands: Arc<[Box<[u8]>]>,
    capacity: usize,
    operation_namespace: u64,
}

impl RequestMetricsState {
    pub fn new(
        sample_percentage: u32,
        capacity: usize,
        allowed_custom_commands: &[&[u8]],
    ) -> Result<Arc<Self>, RequestMetricsConfigurationError> {
        let allowed_custom_commands =
            validate_configuration(sample_percentage, capacity, allowed_custom_commands)?;
        Ok(Arc::new(Self::from_validated(
            sample_percentage,
            capacity,
            allowed_custom_commands,
        )))
    }

    fn from_validated(
        sample_percentage: u32,
        capacity: usize,
        allowed_custom_commands: Arc<[Box<[u8]>]>,
    ) -> Self {
        let (sender, receiver) = sync_channel(capacity);
        Self {
            sample_percentage: AtomicU32::new(sample_percentage),
            sender,
            receiver: Mutex::new(receiver),
            buffered_samples: AtomicUsize::new(0),
            dropped_samples: AtomicU64::new(0),
            allowed_custom_commands,
            capacity,
            operation_namespace: NEXT_OPERATION_NAMESPACE.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Starts a sampled request with the process-local random sampler.
    pub fn start(
        self: &Arc<Self>,
        operation: BoundedOperation,
    ) -> Option<Arc<RequestMetricContext>> {
        self.start_pending().map(|pending| pending.bind(operation))
    }

    /// Starts a sampled request using an instance-scoped percentile source.
    pub fn start_with_sampler<S: Sampler>(
        self: &Arc<Self>,
        operation: BoundedOperation,
        sampler: &mut S,
    ) -> Option<Arc<RequestMetricContext>> {
        self.start_pending_with_sampler(sampler)
            .map(|pending| pending.bind(operation))
    }

    /// Selects a request before its bounded operation is available.
    pub fn start_pending(self: &Arc<Self>) -> Option<PendingRequestMetricContext> {
        self.start_pending_with_sampler(&mut ThreadLocalSampler)
    }

    /// Selects a request with an instance-scoped sampler before binding its operation.
    pub fn start_pending_with_sampler<S: Sampler>(
        self: &Arc<Self>,
        sampler: &mut S,
    ) -> Option<PendingRequestMetricContext> {
        let sample_percentage = self.sample_percentage.load(Ordering::Relaxed);
        if sample_percentage == 0 {
            return None;
        }
        if sample_percentage < MAX_SAMPLE_PERCENTAGE
            && sampler.sample_percentile() >= sample_percentage
        {
            return None;
        }

        Some(self.start_pending_preselected())
    }

    /// Starts a request already selected by a language binding without sampling again.
    pub fn start_pending_preselected(self: &Arc<Self>) -> PendingRequestMetricContext {
        PendingRequestMetricContext {
            started_at: Instant::now(),
            state: Arc::clone(self),
        }
    }

    /// Resolves custom bytes to a pre-interned allow-list slot or the static fallback.
    pub fn custom_operation(&self, operation: &[u8]) -> BoundedOperation {
        match self.allowed_custom_commands.binary_search_by(|candidate| {
            candidate
                .iter()
                .copied()
                .cmp(operation.iter().map(u8::to_ascii_uppercase))
        }) {
            Ok(index) => BoundedOperation(BoundedOperationKind::AllowedCustom {
                index: index as u8,
                namespace: self.operation_namespace,
            }),
            Err(_) => BoundedOperation(BoundedOperationKind::CustomCommand),
        }
    }

    /// Materializes an operation as bytes. UTF-8 conversion belongs to drain serialization.
    pub fn operation_bytes(&self, operation: BoundedOperation) -> &[u8] {
        match operation.0 {
            BoundedOperationKind::Known(operation) => operation.as_bytes(),
            BoundedOperationKind::AllowedCustom { index, namespace }
                if namespace == self.operation_namespace =>
            {
                self.allowed_custom_commands
                    .get(index as usize)
                    .map_or(CUSTOM_COMMAND.as_bytes(), Box::as_ref)
            }
            BoundedOperationKind::AllowedCustom { .. } => CUSTOM_COMMAND.as_bytes(),
            BoundedOperationKind::CustomCommand => CUSTOM_COMMAND.as_bytes(),
        }
    }

    /// Drains at most `max_samples`; queue-size fields are concurrent snapshots.
    pub fn drain(
        &self,
        max_samples: usize,
    ) -> Result<RequestMetricDrain, RequestMetricsDrainError> {
        let receiver = self
            .receiver
            .lock()
            .map_err(|_| RequestMetricsDrainError::ConsumerPoisoned)?;
        let initial_buffered = buffered_count_snapshot(&self.buffered_samples);
        let mut samples = Vec::with_capacity(max_samples.min(initial_buffered));

        for _ in 0..max_samples {
            match receiver.try_recv() {
                Ok(sample) => {
                    samples.push(sample);
                    decrement_buffered(&self.buffered_samples);
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }

        let dropped_samples = self.dropped_samples.swap(0, Ordering::Relaxed);
        let remaining_samples = buffered_count_snapshot(&self.buffered_samples) as u64;
        Ok(RequestMetricDrain {
            samples,
            dropped_samples,
            remaining_samples,
            has_more: remaining_samples > 0,
        })
    }

    fn has_same_fixed_configuration(
        &self,
        capacity: usize,
        allowed_custom_commands: &[Box<[u8]>],
    ) -> bool {
        self.capacity == capacity
            && self.allowed_custom_commands.as_ref() == allowed_custom_commands
    }

    fn update_sample_percentage(&self, sample_percentage: u32) {
        self.sample_percentage
            .store(sample_percentage, Ordering::Relaxed);
    }
}

/// A sampled request whose operation identity has not been parsed yet.
///
/// This value is intentionally move-only: consuming it to bind the operation makes
/// the one-time handoff explicit without synchronization or a second sampling decision.
pub struct PendingRequestMetricContext {
    started_at: Instant,
    state: Arc<RequestMetricsState>,
}

impl PendingRequestMetricContext {
    pub fn bind(self, operation: BoundedOperation) -> Arc<RequestMetricContext> {
        Arc::new(RequestMetricContext::new(
            operation,
            self.started_at,
            self.state,
        ))
    }

    /// Binds the operation and records an initial phase from the original sampled entry instant.
    pub fn bind_and_record_phase(
        self,
        operation: BoundedOperation,
        phase: RequestMetricPhase,
    ) -> Arc<RequestMetricContext> {
        let started_at = self.started_at;
        let context = self.bind(operation);
        context.record_phase_nanos(phase, duration_nanos(started_at.elapsed()).max(1));
        context
    }
}

pub struct RequestMetricContext {
    operation: BoundedOperation,
    started_at: Instant,
    phase_nanos: [AtomicU64; REQUEST_METRIC_PHASE_COUNT],
    attempt_count: AtomicU32,
    lifecycle: Mutex<RequestMetricLifecycle>,
    state: Arc<RequestMetricsState>,
}

struct RequestMetricLifecycle {
    // This is the sole context-owned lock. No lifecycle method awaits or calls back into a phase
    // owner while holding it; phase owners may therefore call context methods under their own
    // locks without creating a reverse context-to-owner lock order.
    finished: bool,
    next_phase_id: u64,
    active_phases: Vec<ActivePhase>,
}

struct ActivePhase {
    id: u64,
    phase: RequestMetricPhase,
    started_at: Instant,
}

impl RequestMetricContext {
    fn new(
        operation: BoundedOperation,
        started_at: Instant,
        state: Arc<RequestMetricsState>,
    ) -> Self {
        Self {
            operation,
            started_at,
            phase_nanos: array::from_fn(|_| AtomicU64::new(0)),
            attempt_count: AtomicU32::new(0),
            lifecycle: Mutex::new(RequestMetricLifecycle {
                finished: false,
                next_phase_id: 0,
                active_phases: Vec::new(),
            }),
            state,
        }
    }

    pub fn start_phase(self: &Arc<Self>, phase: RequestMetricPhase) -> PhaseTimer {
        let started_at = Instant::now();
        let phase_id = {
            let mut lifecycle = self.lock_lifecycle();
            if lifecycle.finished {
                None
            } else {
                let id = lifecycle.next_phase_id;
                lifecycle.next_phase_id = lifecycle.next_phase_id.wrapping_add(1);
                lifecycle.active_phases.push(ActivePhase {
                    id,
                    phase,
                    started_at,
                });
                Some(id)
            }
        };
        PhaseTimer {
            inner: Arc::new(PhaseTimerInner {
                phase_id,
                context: Arc::clone(self),
            }),
        }
    }

    pub fn record_phase_duration(&self, phase: RequestMetricPhase, duration: Duration) {
        self.record_phase_nanos(phase, duration_nanos(duration));
    }

    pub fn increment_attempt_count(&self) {
        self.add_attempts(1);
    }

    pub fn add_attempts(&self, count: u32) {
        let lifecycle = self.lock_lifecycle();
        if !lifecycle.finished {
            saturating_add_u32(&self.attempt_count, count);
        }
    }

    /// Completes this context once and enqueues a compact sample without blocking.
    pub fn finish(&self, result: RequestMetricResult) -> bool {
        let mut lifecycle = self.lock_lifecycle();
        if lifecycle.finished {
            return false;
        }
        lifecycle.finished = true;

        let finished_at = Instant::now();
        for active_phase in lifecycle.active_phases.drain(..) {
            self.record_phase_nanos_uncoordinated(
                active_phase.phase,
                duration_nanos(finished_at.duration_since(active_phase.started_at)).max(1),
            );
        }
        self.record_phase_nanos_uncoordinated(
            RequestMetricPhase::Total,
            duration_nanos(finished_at.duration_since(self.started_at)).max(1),
        );
        let sample = RequestMetricSample {
            operation: self.operation,
            result,
            attempt_count: self.attempt_count.load(Ordering::Relaxed),
            phase_nanos: array::from_fn(|index| self.phase_nanos[index].load(Ordering::Relaxed)),
        };

        match self.state.sender.try_send(sample) {
            Ok(()) => {
                self.state.buffered_samples.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                saturating_add_u64(&self.state.dropped_samples, 1);
            }
        }
        true
    }

    fn record_phase_nanos(&self, phase: RequestMetricPhase, nanos: u64) {
        let lifecycle = self.lock_lifecycle();
        if lifecycle.finished {
            return;
        }
        self.record_phase_nanos_uncoordinated(phase, nanos);
    }

    fn record_phase_nanos_uncoordinated(&self, phase: RequestMetricPhase, nanos: u64) {
        saturating_add_u64(&self.phase_nanos[phase.index()], nanos);
    }

    fn finish_phase(&self, phase_id: u64) -> bool {
        let finished_at = Instant::now();
        let mut lifecycle = self.lock_lifecycle();
        if lifecycle.finished {
            return false;
        }
        let Some(index) = lifecycle
            .active_phases
            .iter()
            .position(|active_phase| active_phase.id == phase_id)
        else {
            return false;
        };
        let active_phase = lifecycle.active_phases.swap_remove(index);
        self.record_phase_nanos_uncoordinated(
            active_phase.phase,
            duration_nanos(finished_at.duration_since(active_phase.started_at)).max(1),
        );
        true
    }

    fn lock_lifecycle(&self) -> std::sync::MutexGuard<'_, RequestMetricLifecycle> {
        self.lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Clone)]
pub struct PhaseTimer {
    inner: Arc<PhaseTimerInner>,
}

struct PhaseTimerInner {
    phase_id: Option<u64>,
    context: Arc<RequestMetricContext>,
}

impl PhaseTimer {
    /// Records this timer once across all clones.
    pub fn finish(&self) -> bool {
        self.inner.finish()
    }
}

impl PhaseTimerInner {
    fn finish(&self) -> bool {
        self.phase_id
            .is_some_and(|phase_id| self.context.finish_phase(phase_id))
    }
}

impl Drop for PhaseTimerInner {
    fn drop(&mut self) {
        self.finish();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestMetricSample {
    operation: BoundedOperation,
    result: RequestMetricResult,
    attempt_count: u32,
    phase_nanos: [u64; REQUEST_METRIC_PHASE_COUNT],
}

impl RequestMetricSample {
    pub fn operation(&self) -> BoundedOperation {
        self.operation
    }

    pub fn result(&self) -> RequestMetricResult {
        self.result
    }

    pub fn attempt_count(&self) -> u32 {
        self.attempt_count
    }

    pub fn phase_durations(&self) -> &[u64; REQUEST_METRIC_PHASE_COUNT] {
        &self.phase_nanos
    }

    pub fn phase_duration(&self, phase: RequestMetricPhase) -> u64 {
        self.phase_nanos[phase.index()]
    }

    pub fn populated_phase_durations(
        &self,
    ) -> impl Iterator<Item = (RequestMetricPhase, u64)> + '_ {
        RequestMetricPhase::ALL
            .into_iter()
            .zip(self.phase_nanos.iter().copied())
            .filter(|(_, nanos)| *nanos != 0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestMetricDrain {
    samples: Vec<RequestMetricSample>,
    dropped_samples: u64,
    remaining_samples: u64,
    has_more: bool,
}

impl RequestMetricDrain {
    pub fn samples(&self) -> &[RequestMetricSample] {
        &self.samples
    }

    pub fn dropped_samples(&self) -> u64 {
        self.dropped_samples
    }

    pub fn remaining_samples(&self) -> u64 {
        self.remaining_samples
    }

    pub fn has_more(&self) -> bool {
        self.has_more
    }
}

/// Configures the process-global state or updates only its sampling percentage.
pub fn configure_request_metrics(
    sample_percentage: u32,
    capacity: usize,
    allowed_custom_commands: &[&[u8]],
) -> Result<(), RequestMetricsConfigurationError> {
    let allowed_custom_commands =
        validate_configuration(sample_percentage, capacity, allowed_custom_commands)?;

    if let Some(state) = REQUEST_METRICS_STATE.get() {
        return update_matching_global(
            state,
            sample_percentage,
            capacity,
            &allowed_custom_commands,
        );
    }

    let state = Arc::new(RequestMetricsState::from_validated(
        sample_percentage,
        capacity,
        allowed_custom_commands,
    ));
    match REQUEST_METRICS_STATE.set(state) {
        Ok(()) => Ok(()),
        Err(unused_state) => update_matching_global(
            REQUEST_METRICS_STATE
                .get()
                .expect("another thread initialized the request metrics state"),
            sample_percentage,
            capacity,
            &unused_state.allowed_custom_commands,
        ),
    }
}

pub fn set_request_metrics_sample_percentage(
    sample_percentage: u32,
) -> Result<(), RequestMetricsConfigurationError> {
    validate_sample_percentage(sample_percentage)?;
    let state = REQUEST_METRICS_STATE
        .get()
        .ok_or(RequestMetricsConfigurationError::NotConfigured)?;
    state.update_sample_percentage(sample_percentage);
    Ok(())
}

pub fn request_metrics_state() -> Option<&'static Arc<RequestMetricsState>> {
    REQUEST_METRICS_STATE.get()
}

fn update_matching_global(
    state: &RequestMetricsState,
    sample_percentage: u32,
    capacity: usize,
    allowed_custom_commands: &[Box<[u8]>],
) -> Result<(), RequestMetricsConfigurationError> {
    if !state.has_same_fixed_configuration(capacity, allowed_custom_commands) {
        return Err(RequestMetricsConfigurationError::ConfigurationMismatch);
    }
    state.update_sample_percentage(sample_percentage);
    Ok(())
}

fn validate_configuration(
    sample_percentage: u32,
    capacity: usize,
    allowed_custom_commands: &[&[u8]],
) -> Result<Arc<[Box<[u8]>]>, RequestMetricsConfigurationError> {
    validate_sample_percentage(sample_percentage)?;
    if !(1..=MAX_BUFFER_CAPACITY).contains(&capacity) {
        return Err(RequestMetricsConfigurationError::InvalidCapacity { capacity });
    }
    if allowed_custom_commands.len() > MAX_ALLOWED_CUSTOM_COMMANDS {
        return Err(
            RequestMetricsConfigurationError::TooManyAllowedCustomCommands {
                count: allowed_custom_commands.len(),
            },
        );
    }

    let mut validated = Vec::with_capacity(allowed_custom_commands.len());
    for (index, command) in allowed_custom_commands.iter().enumerate() {
        if command.is_empty()
            || command.len() > MAX_CUSTOM_COMMAND_LENGTH
            || !matches!(command.first(), Some(byte) if byte.is_ascii_uppercase())
            || !command.iter().all(|byte| {
                byte.is_ascii_uppercase() || byte.is_ascii_digit() || b"_.-".contains(byte)
            })
        {
            return Err(RequestMetricsConfigurationError::InvalidAllowedCustomCommand { index });
        }
        validated.push(command.to_vec().into_boxed_slice());
    }
    validated.sort_unstable();
    validated.dedup();
    Ok(Arc::from(validated))
}

fn validate_sample_percentage(
    sample_percentage: u32,
) -> Result<(), RequestMetricsConfigurationError> {
    if sample_percentage > MAX_SAMPLE_PERCENTAGE {
        return Err(RequestMetricsConfigurationError::InvalidSamplePercentage {
            percentage: sample_percentage,
        });
    }
    Ok(())
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn saturating_add_u64(value: &AtomicU64, increment: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(increment))
    });
}

fn saturating_add_u32(value: &AtomicU32, increment: u32) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(increment))
    });
}

fn decrement_buffered(value: &AtomicUsize) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(buffered_count_after_decrement(current))
    });
}

const fn buffered_count_after_decrement(current: usize) -> usize {
    current.wrapping_sub(1)
}

fn buffered_count_snapshot(value: &AtomicUsize) -> usize {
    // A receiver can consume a sample in the narrow window after try_send
    // publishes it but before the producer increments this counter. Wrapping
    // subtraction represents that transient counter debt without waiting; the
    // producer's required post-send increment resolves it.
    (value.load(Ordering::Relaxed) as isize).max(0) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_but_uncounted_sample_transitions_to_non_waiting_debt() {
        let buffered = AtomicUsize::new(buffered_count_after_decrement(0));
        assert_eq!(buffered.load(Ordering::Relaxed), usize::MAX);
        assert_eq!(buffered_count_snapshot(&buffered), 0);
        buffered.fetch_add(1, Ordering::Relaxed);
        assert_eq!(buffered_count_snapshot(&buffered), 0);
    }
}

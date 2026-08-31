use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use telemetrylib::request_metrics::{
    BoundedOperation, CUSTOM_COMMAND, REQUEST_METRIC_PHASE_COUNT, RequestMetricPhase,
    RequestMetricResult, RequestMetricsConfigurationError, RequestMetricsState, Sampler,
};

struct SequenceSampler {
    values: VecDeque<u32>,
    calls: usize,
}

impl SequenceSampler {
    fn new(values: impl IntoIterator<Item = u32>) -> Self {
        Self {
            values: values.into_iter().collect(),
            calls: 0,
        }
    }
}

impl Sampler for SequenceSampler {
    fn sample_percentile(&mut self) -> u32 {
        self.calls += 1;
        self.values.pop_front().expect("unexpected sampler call")
    }
}

fn state(
    percentage: u32,
    capacity: usize,
    allowed_custom_commands: &[&[u8]],
) -> Arc<RequestMetricsState> {
    RequestMetricsState::new(percentage, capacity, allowed_custom_commands)
        .expect("valid test configuration")
}

fn finish_one(state: &Arc<RequestMetricsState>, result: RequestMetricResult) {
    let context = state
        .start_with_sampler(
            BoundedOperation::known("GET"),
            &mut SequenceSampler::new([]),
        )
        .expect("100% sampling must create a context");
    assert!(context.finish(result));
}

#[test]
fn zero_percent_returns_none_without_consulting_the_sampler() {
    let state = state(0, 1, &[]);
    let mut sampler = SequenceSampler::new([]);

    assert!(
        state
            .start_with_sampler(BoundedOperation::known("GET"), &mut sampler)
            .is_none()
    );
    assert!(state.start(BoundedOperation::known("GET")).is_none());
    assert_eq!(sampler.calls, 0);
}

#[test]
fn one_hundred_percent_returns_a_context_without_consulting_the_sampler() {
    let state = state(100, 1, &[]);
    let mut sampler = SequenceSampler::new([]);

    assert!(
        state
            .start_with_sampler(BoundedOperation::known("GET"), &mut sampler)
            .is_some()
    );
    assert!(state.start(BoundedOperation::known("GET")).is_some());
    assert_eq!(sampler.calls, 0);
}

#[test]
fn ten_percent_sampling_uses_injected_percentiles_deterministically() {
    let state = state(10, 4, &[]);
    let mut sampler = SequenceSampler::new([0, 9, 10, 99]);

    let decisions = (0..4)
        .map(|_| {
            state
                .start_with_sampler(BoundedOperation::known("GET"), &mut sampler)
                .is_some()
        })
        .collect::<Vec<_>>();

    assert_eq!(decisions, [true, true, false, false]);
    assert_eq!(sampler.calls, 4);
}

#[test]
fn phase_values_use_fixed_order_and_accumulate_with_saturation() {
    let state = state(100, 1, &[]);
    let context = state
        .start_with_sampler(
            BoundedOperation::known("GET"),
            &mut SequenceSampler::new([]),
        )
        .unwrap();
    let phases = [
        RequestMetricPhase::JniIngress,
        RequestMetricPhase::ClientQueue,
        RequestMetricPhase::CommandPrepare,
        RequestMetricPhase::ConnectionWait,
        RequestMetricPhase::PipelineQueue,
        RequestMetricPhase::SocketWrite,
        RequestMetricPhase::ResponseWait,
        RequestMetricPhase::RetryBackoff,
        RequestMetricPhase::CoreDecode,
        RequestMetricPhase::CallbackQueue,
        RequestMetricPhase::CallbackComplete,
    ];

    for (index, phase) in phases.into_iter().enumerate() {
        context.record_phase_duration(phase, Duration::from_nanos(index as u64 + 1));
    }
    context.record_phase_duration(RequestMetricPhase::ClientQueue, Duration::from_nanos(5));
    context.record_phase_duration(RequestMetricPhase::CoreDecode, Duration::MAX);
    context.record_phase_duration(RequestMetricPhase::CoreDecode, Duration::from_nanos(1));
    context.add_attempts(u32::MAX);
    context.increment_attempt_count();
    assert!(context.finish(RequestMetricResult::Success));

    let drain = state.drain(1).unwrap();
    let sample = &drain.samples()[0];
    assert_eq!(sample.phase_durations().len(), REQUEST_METRIC_PHASE_COUNT);
    assert_eq!(state.operation_bytes(sample.operation()), b"GET");
    assert_eq!(sample.phase_duration(RequestMetricPhase::JniIngress), 1);
    assert_eq!(sample.phase_duration(RequestMetricPhase::ClientQueue), 7);
    assert_eq!(
        sample.phase_duration(RequestMetricPhase::CoreDecode),
        u64::MAX
    );
    assert!(sample.phase_duration(RequestMetricPhase::Total) > 0);
    assert_eq!(sample.attempt_count(), u32::MAX);

    let populated = sample.populated_phase_durations().collect::<Vec<_>>();
    assert_eq!(populated.len(), REQUEST_METRIC_PHASE_COUNT);
    for pair in populated.windows(2) {
        assert!((pair[0].0 as u8) < (pair[1].0 as u8));
    }
}

#[test]
fn cloned_phase_timer_records_exactly_once() {
    let state = state(100, 1, &[]);
    let context = state
        .start_with_sampler(
            BoundedOperation::known("GET"),
            &mut SequenceSampler::new([]),
        )
        .unwrap();
    let timer = context.start_phase(RequestMetricPhase::ClientQueue);
    let timer_clone = timer.clone();

    drop(timer);
    assert!(timer_clone.finish());
    assert!(!timer_clone.finish());
    assert!(context.finish(RequestMetricResult::Success));

    let drain = state.drain(1).unwrap();
    let sample = &drain.samples()[0];
    assert!(sample.phase_duration(RequestMetricPhase::ClientQueue) > 0);
}

#[test]
fn finish_is_exactly_once_and_preserves_each_terminal_result() {
    let state = state(100, 4, &[]);
    let expected = [
        RequestMetricResult::Success,
        RequestMetricResult::Failure,
        RequestMetricResult::Timeout,
        RequestMetricResult::Cancelled,
    ];

    for result in expected {
        let context = state
            .start_with_sampler(
                BoundedOperation::known("GET"),
                &mut SequenceSampler::new([]),
            )
            .unwrap();
        assert!(context.finish(result));
        assert!(!context.finish(RequestMetricResult::Failure));
    }

    let drain = state.drain(4).unwrap();
    assert_eq!(
        drain
            .samples()
            .iter()
            .map(|sample| sample.result())
            .collect::<Vec<_>>(),
        expected
    );
}

#[test]
fn full_capacity_drops_the_next_completed_sample() {
    let state = state(100, 1, &[]);

    finish_one(&state, RequestMetricResult::Success);
    finish_one(&state, RequestMetricResult::Failure);

    let drain = state.drain(1).unwrap();
    assert_eq!(drain.samples().len(), 1);
    assert_eq!(drain.samples()[0].result(), RequestMetricResult::Success);
    assert_eq!(drain.dropped_samples(), 1);
    assert_eq!(drain.remaining_samples(), 0);
    assert!(!drain.has_more());
}

#[test]
fn drain_respects_limit_and_resets_only_the_reported_drop_count() {
    let state = state(100, 3, &[]);
    for _ in 0..4 {
        finish_one(&state, RequestMetricResult::Success);
    }

    let first = state.drain(2).unwrap();
    assert_eq!(first.samples().len(), 2);
    assert_eq!(first.dropped_samples(), 1);
    assert_eq!(first.remaining_samples(), 1);
    assert!(first.has_more());

    finish_one(&state, RequestMetricResult::Timeout);
    finish_one(&state, RequestMetricResult::Cancelled);
    finish_one(&state, RequestMetricResult::Failure);

    let second = state.drain(1).unwrap();
    assert_eq!(second.samples().len(), 1);
    assert_eq!(second.dropped_samples(), 1);
    assert_eq!(second.remaining_samples(), 2);
    assert!(second.has_more());

    let third = state.drain(10).unwrap();
    assert_eq!(third.samples().len(), 2);
    assert_eq!(third.dropped_samples(), 0);
    assert_eq!(third.remaining_samples(), 0);
    assert!(!third.has_more());
}

#[test]
fn custom_command_configuration_is_validated_as_uppercase_ascii() {
    assert!(matches!(
        RequestMetricsState::new(101, 1, &[]),
        Err(RequestMetricsConfigurationError::InvalidSamplePercentage { .. })
    ));
    assert!(matches!(
        RequestMetricsState::new(10, 0, &[]),
        Err(RequestMetricsConfigurationError::InvalidCapacity { .. })
    ));
    assert!(matches!(
        RequestMetricsState::new(10, 1_000_001, &[]),
        Err(RequestMetricsConfigurationError::InvalidCapacity { .. })
    ));

    for invalid in [
        b"".as_slice(),
        b"graph.query".as_slice(),
        b"GRAPH QUERY".as_slice(),
        b"GRAPH/QUERY".as_slice(),
        &[b'A'; 65],
        &[0xff],
    ] {
        assert!(matches!(
            RequestMetricsState::new(10, 1, &[invalid]),
            Err(RequestMetricsConfigurationError::InvalidAllowedCustomCommand { .. })
        ));
    }

    let too_many = vec![b"GRAPH.QUERY".as_slice(); 65];
    assert!(matches!(
        RequestMetricsState::new(10, 1, &too_many),
        Err(RequestMetricsConfigurationError::TooManyAllowedCustomCommands { .. })
    ));
}

#[test]
fn custom_commands_use_allow_listed_identity_or_static_fallback_without_utf8() {
    let state = state(100, 1, &[b"GRAPH.QUERY"]);

    let allowed = state.custom_operation(b"GRAPH.QUERY");
    let unknown = state.custom_operation(b"FT.SEARCH");
    let invalid_utf8 = state.custom_operation(&[0xff, 0xfe]);

    assert_eq!(state.operation_bytes(allowed), b"GRAPH.QUERY");
    assert_eq!(state.operation_bytes(unknown), CUSTOM_COMMAND.as_bytes());
    assert_eq!(
        state.operation_bytes(invalid_utf8),
        CUSTOM_COMMAND.as_bytes()
    );
}

#[test]
fn custom_operation_cannot_be_relabelled_by_another_state() {
    let first = state(100, 1, &[b"GRAPH.QUERY"]);
    let second = state(100, 1, &[b"FT.SEARCH"]);
    let first_operation = first.custom_operation(b"GRAPH.QUERY");

    let context = second
        .start_with_sampler(first_operation, &mut SequenceSampler::new([]))
        .unwrap();
    assert!(context.finish(RequestMetricResult::Success));
    let drain = second.drain(1).unwrap();

    assert_eq!(
        second.operation_bytes(drain.samples()[0].operation()),
        CUSTOM_COMMAND.as_bytes()
    );
    assert_eq!(first.operation_bytes(first_operation), b"GRAPH.QUERY");
}

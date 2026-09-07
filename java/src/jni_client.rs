//! JNI client management infrastructure extracted from JNI-java implementation
//! This module provides direct JNI calls to glide-core

use anyhow::Result;
use dashmap::DashMap;
use glide_core::client::Client as GlideClient;
use glide_core::client::ConnectionRequest;
use glide_core::errors::{error_message, error_type};
use glide_core::native_request_metrics::{
    PhaseTimer, RequestMetricContext, RequestMetricPhase, RequestMetricResult,
};
use jni::JNIEnv;
use jni::JavaVM;
use jni::objects::{GlobalRef, JClass, JObject, JStaticMethodID, JValue};
use jni::signature;
use jni::sys::{JNI_VERSION_1_8, jint, jlong, jstring};
use logger_core::log_structured;
use parking_lot::Mutex;
use redis::{RedisError as ServerError, Value as ServerValue};
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SendError, Sender, channel};
use std::thread;
use tokio::runtime::Runtime;

const MAX_EXCEPTION_DESCRIPTIONS: usize = 5;
static EXCEPTION_DESCRIPTIONS_EMITTED: AtomicUsize = AtomicUsize::new(0);

// ExceptionDescribe clears the pending exception. Only call this immediately before an existing
// exception_clear recovery path so diagnostics do not change callback behavior.
fn describe_pending_java_exception_before_clear(
    env: &JNIEnv,
    callback_id: Option<jlong>,
    stage: &'static str,
) {
    let pending = match env.exception_check() {
        Ok(pending) => pending,
        Err(error) => {
            log_structured(
                logger_core::Level::Error,
                "glide_jni_java_exception_check_failed",
                logger_core::structured_fields!(
                    "callback_id" => callback_id,
                    "stage" => stage,
                    "error" => error.to_string(),
                ),
            );
            return;
        }
    };

    let describe_ordinal =
        pending.then(|| EXCEPTION_DESCRIPTIONS_EMITTED.fetch_add(1, Ordering::Relaxed));
    let describe_emitted =
        describe_ordinal.is_some_and(|ordinal| ordinal < MAX_EXCEPTION_DESCRIPTIONS);
    let describe_error = describe_emitted
        .then(|| {
            env.exception_describe()
                .err()
                .map(|error| error.to_string())
        })
        .flatten();

    log_structured(
        if pending {
            logger_core::Level::Error
        } else {
            logger_core::Level::Warn
        },
        "glide_jni_pending_java_exception",
        logger_core::structured_fields!(
            "callback_id" => callback_id,
            "stage" => stage,
            "pending" => pending,
            "exception_describe_emitted" => describe_emitted,
            "exception_describe_ordinal" => describe_ordinal,
            "exception_describe_error" => describe_error,
        ),
    );
}

#[unsafe(no_mangle)]
pub extern "system" fn JNI_OnLoad(vm: JavaVM, _reserved: *mut c_void) -> jint {
    // Cache JavaVM env for later use
    let _ = JVM.set(Arc::new(vm));

    // Pre-cache MethodCache and JavaValueConversionCache with correct classloader context
    // GlideCoreClientCache and RegistryMethodCache will be cached automatically later
    if let Some(jvm) = JVM.get()
        && let Ok(mut env) = jvm.get_env()
    {
        let _ = get_method_cache(&mut env);
        let _ = crate::get_java_value_conversion_cache(&mut env);
    }

    JNI_VERSION_1_8
}

#[unsafe(no_mangle)]
pub extern "system" fn JNI_OnUnload(_vm: *const JavaVM, _reserved: *const c_void) {
    // Clean up global references by setting cached Options to None
    // This triggers Drop on GlobalRef objects, which calls delete_global_ref
    // Note: All cache functions use unsafe transmute to return static references
    // from OnceLock data that lives for the entire program duration

    if let Some(cache_mutex) = METHOD_CACHE.get() {
        *cache_mutex.lock() = None;
    }

    if let Some(cache_mutex) = GLIDE_CORE_CLIENT_CACHE.get() {
        *cache_mutex.lock() = None;
    }

    // Clean up caches in lib.rs
    crate::cleanup_global_caches();
}

/// Invalidate cached JNI method IDs so the next call re-initializes them.
/// Called when callback completion fails — stale method IDs (e.g. from classloader
/// changes) would cause every subsequent callback to fail permanently. Clearing the
/// cache lets the fallback `find_class` path re-discover the correct method IDs.
///
/// Safe to call at any time: the Mutex<Option<...>> pattern means the next caller
/// will re-populate the cache from the current JNIEnv.
fn invalidate_jni_caches() {
    if let Some(cache_mutex) = METHOD_CACHE.get() {
        *cache_mutex.lock() = None;
    }
    if let Some(cache_mutex) = GLIDE_CORE_CLIENT_CACHE.get() {
        *cache_mutex.lock() = None;
    }
}

// Type aliases for complex types
type PushMessageTuple = (Vec<u8>, Vec<u8>, Option<Vec<u8>>);
type CallbackResult = Result<ServerValue, ServerError>;

// Runtime and JVM statics
pub static JVM: std::sync::OnceLock<Arc<JavaVM>> = std::sync::OnceLock::new();
static RUNTIME: std::sync::OnceLock<Runtime> = std::sync::OnceLock::new();

// Defaults for runtime and callback workers
// NOTE: minimum 2 worker threads required for MultiplexedConnection (scope feature).
// The connection's internal reader task must run concurrently with command sends.
const DEFAULT_RUNTIME_WORKER_THREADS: usize = 1;
const DEFAULT_CALLBACK_WORKER_THREADS: usize = 2;

// =========================
// Native buffer registry
// =========================
static NATIVE_BUFFER_REGISTRY: std::sync::OnceLock<dashmap::DashMap<u64, Vec<u8>>> =
    std::sync::OnceLock::new();
static NEXT_NATIVE_BUFFER_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
static CALLBACK_COORDINATION: std::sync::OnceLock<CallbackCoordinationRegistry> =
    std::sync::OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CallbackCoordinationState {
    TimedOut,
    CompletedAwaitingMark,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CallbackCoordinationFinish {
    None,
    CheckForTimeout,
    AwaitTimeoutMark,
}

#[derive(Default)]
struct CallbackCoordinationRegistry {
    states: Mutex<HashMap<jlong, CallbackCoordinationState>>,
}

impl CallbackCoordinationRegistry {
    fn mark_timed_out(
        &self,
        callback_id: jlong,
        has_in_flight_command: impl FnOnce() -> bool,
    ) -> bool {
        let mut states = self.states.lock();
        match states.remove(&callback_id) {
            Some(CallbackCoordinationState::CompletedAwaitingMark) => true,
            Some(CallbackCoordinationState::TimedOut) => {
                states.insert(callback_id, CallbackCoordinationState::TimedOut);
                true
            }
            None if has_in_flight_command() => {
                states.insert(callback_id, CallbackCoordinationState::TimedOut);
                true
            }
            None => false,
        }
    }

    fn take_timed_out(&self, callback_id: jlong) -> bool {
        let mut states = self.states.lock();
        if states.get(&callback_id) == Some(&CallbackCoordinationState::TimedOut) {
            states.remove(&callback_id);
            true
        } else {
            false
        }
    }

    fn finish(
        &self,
        callback_id: jlong,
        coordination: CallbackCoordinationFinish,
        finish_in_flight_command: impl FnOnce(),
    ) -> bool {
        let mut states = self.states.lock();
        let timed_out = match coordination {
            CallbackCoordinationFinish::None => false,
            CallbackCoordinationFinish::CheckForTimeout => {
                if states.get(&callback_id) == Some(&CallbackCoordinationState::TimedOut) {
                    states.remove(&callback_id);
                    true
                } else {
                    false
                }
            }
            CallbackCoordinationFinish::AwaitTimeoutMark => {
                if states.get(&callback_id) == Some(&CallbackCoordinationState::TimedOut) {
                    states.remove(&callback_id);
                    true
                } else {
                    states.insert(
                        callback_id,
                        CallbackCoordinationState::CompletedAwaitingMark,
                    );
                    false
                }
            }
        };
        // Release caller-visible ownership under the same lock. A later timeout mark therefore
        // cannot insert a stale marker after normal completion removes the exact command.
        finish_in_flight_command();
        timed_out
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.states.lock().len()
    }
}

fn get_native_buffer_registry() -> &'static dashmap::DashMap<u64, Vec<u8>> {
    NATIVE_BUFFER_REGISTRY.get_or_init(dashmap::DashMap::new)
}

pub fn register_native_buffer(bytes: Vec<u8>) -> (u64, *mut u8, usize) {
    let id = NEXT_NATIVE_BUFFER_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let registry = get_native_buffer_registry();
    registry.insert(id, bytes);
    // Obtain stable pointer/len from stored Vec
    let guard = registry.get(&id).expect("buffer just inserted");
    let ptr = guard.as_ptr() as *mut u8;
    let len = guard.len();
    (id, ptr, len)
}

pub fn free_native_buffer(id: u64) -> bool {
    let registry = get_native_buffer_registry();
    registry.remove(&id).is_some()
}

fn callback_coordination() -> &'static CallbackCoordinationRegistry {
    CALLBACK_COORDINATION.get_or_init(CallbackCoordinationRegistry::default)
}

pub fn mark_callback_timed_out(callback_id: jlong) -> bool {
    callback_coordination()
        .mark_timed_out(callback_id, || crate::has_in_flight_command(callback_id))
}

/// Initialize or return the shared Tokio runtime.
pub(crate) fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        let worker_threads = if let Ok(threads_str) = std::env::var("GLIDE_TOKIO_WORKER_THREADS") {
            threads_str
                .parse::<usize>()
                .unwrap_or(DEFAULT_RUNTIME_WORKER_THREADS)
        } else {
            DEFAULT_RUNTIME_WORKER_THREADS
        };

        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .max_blocking_threads(worker_threads * 2)
            .enable_all()
            .thread_name("glide-worker")
            .thread_stack_size(2 * 1024 * 1024)
            .thread_keep_alive(std::time::Duration::from_secs(60))
            .build()
            .expect("Failed to create Tokio runtime")
    })
}

/// Handle table for native clients.
type JniHandleTable = Arc<DashMap<u64, GlideClient>>;
type PendingMap = Arc<DashMap<u64, ConnectionRequest>>;

static JNI_HANDLE_TABLE: std::sync::OnceLock<JniHandleTable> = std::sync::OnceLock::new();
static PENDING_CONFIGS: std::sync::OnceLock<PendingMap> = std::sync::OnceLock::new();

pub(crate) fn get_handle_table() -> &'static JniHandleTable {
    JNI_HANDLE_TABLE.get_or_init(|| Arc::new(DashMap::new()))
}

pub(crate) fn get_pending_map() -> &'static PendingMap {
    PENDING_CONFIGS.get_or_init(|| Arc::new(DashMap::new()))
}

/// Generate unique safe handle for JNI resource management
static NEXT_HANDLE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub fn generate_safe_handle() -> u64 {
    NEXT_HANDLE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Create actual glide-core Valkey client with specified configuration
pub async fn create_glide_client(
    connection_request: ConnectionRequest,
    push_tx: Option<tokio::sync::mpsc::UnboundedSender<redis::PushInfo>>,
) -> Result<GlideClient> {
    let client = GlideClient::new(connection_request, push_tx)
        .await
        .map_err(|e| {
            log::error!("Failed to create glide-core client: {e}");
            anyhow::anyhow!("Failed to create glide-core client: {e}")
        })?;
    Ok(client)
}

pub async fn ensure_client_for_handle(handle_id: u64) -> Result<GlideClient> {
    let table = get_handle_table();
    if let Some(entry) = table.get(&handle_id) {
        return Ok(entry.value().clone());
    }

    // Check for pending config and create lazily
    let pending = {
        let pm = get_pending_map();
        pm.remove(&handle_id).map(|(_, cfg)| cfg)
    };

    if let Some(mut cfg) = pending {
        cfg.lazy_connect = false;

        // Always setup push channel for push message support
        // This enables dynamic subscriptions to work,
        // even when no initial subscriptions are configured
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<redis::PushInfo>();

        let client = create_glide_client(cfg, Some(tx)).await?;
        table.insert(handle_id, client.clone());

        // Register in the glide-core scope registry for scope command execution
        glide_core::scope::register_client(handle_id, client.clone());

        // Always spawn push notification handler
        let jvm_arc = JVM.get().cloned();
        let handle_for_java = handle_id as jlong;
        get_runtime().spawn(async move {
            while let Some(push) = rx.recv().await {
                if let Some(jvm) = jvm_arc.as_ref()
                    && let Ok(mut env) = jvm.attach_current_thread_as_daemon()
                {
                    // Handle push notification callback to Java
                    handle_push_notification(&mut env, handle_for_java, push);
                }
            }
        });

        return Ok(table.get(&handle_id).unwrap().value().clone());
    }

    Err(anyhow::anyhow!("Client not found in handle_table"))
}

pub(crate) fn handle_push_notification(env: &mut JNIEnv, handle_id: jlong, push: redis::PushInfo) {
    use redis::{PushKind, Value};

    let as_bytes = |v: &Value| -> Option<Vec<u8>> {
        match v {
            Value::BulkString(b) => Some(b.to_vec()),
            _ => None,
        }
    };

    let mapped: Option<PushMessageTuple> = match push.kind {
        PushKind::Message | PushKind::SMessage => {
            if push.data.len() >= 2 {
                let channel = as_bytes(&push.data[0]).unwrap_or_default();
                let message = as_bytes(&push.data[1]).unwrap_or_default();
                Some((message, channel, None))
            } else {
                None
            }
        }
        PushKind::PMessage => {
            if push.data.len() >= 3 {
                let pattern = as_bytes(&push.data[0]).unwrap_or_default();
                let channel = as_bytes(&push.data[1]).unwrap_or_default();
                let message = as_bytes(&push.data[2]).unwrap_or_default();
                Some((message, channel, Some(pattern)))
            } else {
                None
            }
        }
        _ => None,
    };

    if let Some((m, c, p)) = mapped {
        let _ = env.push_local_frame(16);
        let jm = env.byte_array_from_slice(&m).ok();
        let jc = env.byte_array_from_slice(&c).ok();
        let jp = p.as_ref().and_then(|pp| env.byte_array_from_slice(pp).ok());

        if let (Some(jm), Some(jc)) = (jm, jc) {
            let jm_obj: JObject = jm.into();
            let jc_obj: JObject = jc.into();
            let jp_obj: JObject = jp.map(Into::into).unwrap_or(JObject::null());

            if let Ok(cache) = get_glide_core_client_cache_safe(env) {
                unsafe {
                    let _ = env.call_static_method_unchecked(
                        &cache.class,
                        cache.on_native_push,
                        signature::ReturnType::Primitive(signature::Primitive::Void),
                        &[
                            JValue::Long(handle_id).as_jni(),
                            JValue::Object(&jm_obj).as_jni(),
                            JValue::Object(&jc_obj).as_jni(),
                            JValue::Object(&jp_obj).as_jni(),
                        ],
                    );
                }
            }
        }

        let _ = unsafe { env.pop_local_frame(&JObject::null()) };
    }
}

/// Cache of required Java method IDs.
#[derive(Clone)]
pub(crate) struct MethodCache {
    async_handle_table_class: GlobalRef,
    complete_callback_method: JStaticMethodID,
    complete_callback_for_native_method: JStaticMethodID,
    complete_error_with_code_method: JStaticMethodID,
    complete_error_with_code_for_native_method: JStaticMethodID,
    fail_all_method: JStaticMethodID,
}

static METHOD_CACHE: std::sync::OnceLock<Mutex<Option<MethodCache>>> = std::sync::OnceLock::new();

/// Get or initialize the method cache.
pub(crate) fn get_method_cache(env: &mut JNIEnv) -> Result<MethodCache> {
    let cache_mutex = METHOD_CACHE.get_or_init(|| Mutex::new(None));

    {
        let cache_guard = cache_mutex.lock();
        if let Some(cache) = cache_guard.as_ref() {
            return Ok(cache.clone());
        }
    }

    let class = env
        .find_class("glide/internal/AsyncRegistry")
        .map_err(|e| anyhow::anyhow!("Failed to find AsyncRegistry class: {e}"))?;

    let global_class = env
        .new_global_ref(&class)
        .map_err(|e| anyhow::anyhow!("Failed to create global class reference: {e}"))?;

    let complete_callback_method = env
        .get_static_method_id(&class, "completeCallback", "(JLjava/lang/Object;)Z")
        .map_err(|e| anyhow::anyhow!("Failed to get completeCallback method ID: {e}"))?;

    let complete_callback_for_native_method = env
        .get_static_method_id(
            &class,
            "completeCallbackForNative",
            "(JLjava/lang/Object;)I",
        )
        .map_err(|e| anyhow::anyhow!("Failed to get completeCallbackForNative method ID: {e}"))?;

    let complete_error_with_code_method = env
        .get_static_method_id(
            &class,
            "completeCallbackWithErrorCode",
            "(JILjava/lang/String;)Z",
        )
        .map_err(|e| {
            anyhow::anyhow!("Failed to get completeCallbackWithErrorCode method ID: {e}")
        })?;

    let complete_error_with_code_for_native_method = env
        .get_static_method_id(
            &class,
            "completeCallbackWithErrorCodeForNative",
            "(JILjava/lang/String;)I",
        )
        .map_err(|e| {
            anyhow::anyhow!("Failed to get completeCallbackWithErrorCodeForNative method ID: {e}")
        })?;

    let fail_all_method = env
        .get_static_method_id(&class, "failAllWithError", "(Ljava/lang/String;)V")
        .map_err(|e| anyhow::anyhow!("Failed to get failAllWithError method ID: {e}"))?;

    let method_cache = MethodCache {
        async_handle_table_class: global_class,
        complete_callback_method,
        complete_callback_for_native_method,
        complete_error_with_code_method,
        complete_error_with_code_for_native_method,
        fail_all_method,
    };

    // Store in cache
    {
        let mut cache_guard = cache_mutex.lock();
        *cache_guard = Some(method_cache.clone());
    }

    Ok(method_cache)
}

struct JavaCallbackPayload {
    result: CallbackResult,
    binary_mode: bool,
}

/// A callback payload and the sampled request lifecycle it owns, if any.
struct CallbackJob<P> {
    callback_id: jlong,
    payload: P,
    request_metrics: Option<Arc<RequestMetricContext>>,
    detailed_completion: bool,
    callback_queue: Option<PhaseTimer>,
}

impl<P> CallbackJob<P> {
    fn new(
        callback_id: jlong,
        payload: P,
        request_metrics: Option<Arc<RequestMetricContext>>,
        detailed_completion: bool,
    ) -> Self {
        Self {
            callback_id,
            payload,
            request_metrics,
            detailed_completion,
            callback_queue: None,
        }
    }
}

type JavaCallbackJob = CallbackJob<JavaCallbackPayload>;

/// Global unbounded callback queue sender
static CALLBACK_SENDER: std::sync::OnceLock<Sender<JavaCallbackJob>> = std::sync::OnceLock::new();

fn get_callback_worker_threads() -> usize {
    if let Ok(val) = std::env::var("GLIDE_CALLBACK_WORKER_THREADS") {
        val.parse::<usize>()
            .unwrap_or(DEFAULT_CALLBACK_WORKER_THREADS)
            .max(1)
    } else {
        DEFAULT_CALLBACK_WORKER_THREADS
    }
}

fn init_callback_workers() -> &'static Sender<JavaCallbackJob> {
    CALLBACK_SENDER.get_or_init(|| {
        let (tx, rx) = channel::<JavaCallbackJob>();
        let rx = Arc::new(std::sync::Mutex::new(rx));
        let worker_threads = get_callback_worker_threads();

        for i in 0..worker_threads {
            let rx_clone = Arc::clone(&rx);
            thread::Builder::new()
                .name(format!("glide-jni-callback-{i}"))
                .spawn(move || {
                    // Pre-attach to JVM once at thread start. attach_current_thread_as_daemon
                    // keeps the thread attached for its entire lifetime (no detach on drop).
                    // This eliminates per-callback attach overhead and the attach failure window.
                    let Some(jvm) = JVM.get() else {
                        log::error!("Callback worker {i}: JVM not cached, cannot start");
                        return;
                    };
                    let Ok(mut env) = jvm.attach_current_thread_as_daemon() else {
                        log::error!("Callback worker {i}: failed to attach to JVM at startup");
                        return;
                    };

                    loop {
                        let received = {
                            let guard = rx_clone.lock().unwrap();
                            receive_callback_job(&guard)
                        };
                        let Some((job, callback_complete)) = received else {
                            break;
                        };

                        // Process callback with pre-attached env
                        process_callback_job_with_env(&mut env, job, callback_complete);
                    }
                })
                .expect("Failed to spawn callback worker thread");
        }

        tx
    })
}

fn enqueue_callback_job<P>(
    sender: &Sender<CallbackJob<P>>,
    mut job: CallbackJob<P>,
) -> Result<(), SendError<CallbackJob<P>>> {
    job.callback_queue = job
        .request_metrics
        .as_ref()
        .map(|context| context.start_phase(RequestMetricPhase::CallbackQueue));
    sender.send(job)
}

fn receive_callback_job<P>(
    receiver: &Receiver<CallbackJob<P>>,
) -> Option<(CallbackJob<P>, Option<PhaseTimer>)> {
    let mut job = receiver.recv().ok()?;
    // This must be the first action after receiving the exact job.
    let callback_complete = begin_callback_completion(&mut job);
    Some((job, callback_complete))
}

fn begin_callback_completion<P>(job: &mut CallbackJob<P>) -> Option<PhaseTimer> {
    if let Some(callback_queue) = job.callback_queue.take() {
        callback_queue.finish();
    }
    job.request_metrics
        .as_ref()
        .map(|context| context.start_phase(RequestMetricPhase::CallbackComplete))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JavaCompletionOutcome {
    Completed,
    Timeout,
    Cancelled,
    Failure,
    TimeoutMarkMissed,
    NativeTimeout,
}

impl TryFrom<jint> for JavaCompletionOutcome {
    type Error = anyhow::Error;

    fn try_from(value: jint) -> Result<Self> {
        match value {
            0 => Ok(Self::Completed),
            1 => Ok(Self::Timeout),
            2 => Ok(Self::Cancelled),
            3 => Ok(Self::Failure),
            4 => Ok(Self::TimeoutMarkMissed),
            5 => Ok(Self::NativeTimeout),
            _ => Err(anyhow::anyhow!(
                "Unknown Java callback completion outcome: {value}"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CallbackFailure {
    Conversion,
    JniDelivery,
    Channel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CallbackCompletion {
    Java(JavaCompletionOutcome),
    TimedOutByMarker,
    Failed(CallbackFailure),
}

fn callback_coordination_finish(completion: CallbackCompletion) -> CallbackCoordinationFinish {
    match completion {
        CallbackCompletion::Java(JavaCompletionOutcome::Timeout) => {
            CallbackCoordinationFinish::AwaitTimeoutMark
        }
        CallbackCompletion::Java(JavaCompletionOutcome::Failure)
        | CallbackCompletion::Failed(_) => CallbackCoordinationFinish::CheckForTimeout,
        CallbackCompletion::Java(_) | CallbackCompletion::TimedOutByMarker => {
            CallbackCoordinationFinish::None
        }
    }
}

fn classify_callback_completion(
    command_succeeded: bool,
    completion: CallbackCompletion,
) -> RequestMetricResult {
    match completion {
        CallbackCompletion::Java(JavaCompletionOutcome::Completed) if command_succeeded => {
            RequestMetricResult::Success
        }
        CallbackCompletion::Java(JavaCompletionOutcome::Completed)
        | CallbackCompletion::Java(JavaCompletionOutcome::Failure)
        | CallbackCompletion::Failed(_) => RequestMetricResult::Failure,
        CallbackCompletion::Java(
            JavaCompletionOutcome::Timeout
            | JavaCompletionOutcome::TimeoutMarkMissed
            | JavaCompletionOutcome::NativeTimeout,
        )
        | CallbackCompletion::TimedOutByMarker => RequestMetricResult::Timeout,
        CallbackCompletion::Java(JavaCompletionOutcome::Cancelled) => {
            RequestMetricResult::Cancelled
        }
    }
}

fn finish_callback(
    callback_id: jlong,
    request_metrics: Option<&Arc<RequestMetricContext>>,
    callback_queue: Option<PhaseTimer>,
    callback_complete: Option<PhaseTimer>,
    command_succeeded: bool,
    completion: CallbackCompletion,
) -> RequestMetricResult {
    finish_callback_with_registry(
        callback_coordination(),
        callback_id,
        request_metrics,
        callback_queue,
        callback_complete,
        command_succeeded,
        completion,
    )
}

fn finish_callback_with_registry(
    registry: &CallbackCoordinationRegistry,
    callback_id: jlong,
    request_metrics: Option<&Arc<RequestMetricContext>>,
    callback_queue: Option<PhaseTimer>,
    callback_complete: Option<PhaseTimer>,
    command_succeeded: bool,
    completion: CallbackCompletion,
) -> RequestMetricResult {
    if let Some(callback_queue) = callback_queue {
        callback_queue.finish();
    }
    if let Some(callback_complete) = callback_complete {
        callback_complete.finish();
    }
    let marked_timed_out = registry.finish(
        callback_id,
        callback_coordination_finish(completion),
        || crate::finish_in_flight_command(callback_id),
    );
    let result = if marked_timed_out {
        RequestMetricResult::Timeout
    } else {
        classify_callback_completion(command_succeeded, completion)
    };
    if let Some(request_metrics) = request_metrics {
        request_metrics.finish(result);
    }
    result
}

/// Process a callback with an already-attached JNIEnv.
/// Used by pre-attached callback worker threads.
fn process_callback_job_with_env(
    env: &mut JNIEnv,
    job: JavaCallbackJob,
    callback_complete: Option<PhaseTimer>,
) {
    let CallbackJob {
        callback_id,
        payload,
        request_metrics,
        detailed_completion,
        callback_queue,
    } = job;
    let JavaCallbackPayload {
        result,
        binary_mode,
    } = payload;
    debug_assert!(callback_queue.is_none());

    if callback_coordination().take_timed_out(callback_id) {
        log_structured(
            logger_core::Level::Warn,
            "glide_jni_callback_dropped_after_timeout",
            logger_core::structured_fields!(
                "callback_id" => callback_id,
                "binary_mode" => binary_mode,
                "stage" => "before_response_conversion",
            ),
        );
        finish_callback(
            callback_id,
            request_metrics.as_ref(),
            callback_queue,
            callback_complete,
            false,
            CallbackCompletion::TimedOutByMarker,
        );
        logger_core::log_debug_rate_limited!(
            "jni_callback",
            5,
            format!(
                "Rust task completed for callback_id={} after Java timeout — result discarded.",
                callback_id
            )
        );
        return;
    }

    match result {
        Ok(server_value) => {
            let response_type = crate::redis_value_type(&server_value);
            let response_size_bytes = estimate_value_size(&server_value);
            let use_direct_buffer = should_use_direct_buffer(&server_value);
            let conversion_path = response_conversion_path(&server_value);
            let callback_worker = thread::current().name().unwrap_or("unnamed").to_string();

            log_structured(
                logger_core::Level::Debug,
                "glide_jni_response_conversion_started",
                logger_core::structured_fields!(
                    "callback_id" => callback_id,
                    "binary_mode" => binary_mode,
                    "callback_worker" => callback_worker.as_str(),
                    "response_type" => response_type,
                    "response_size_bytes" => response_size_bytes,
                    "conversion_path" => conversion_path,
                    "native_buffer_count" => get_native_buffer_registry().len(),
                ),
            );

            if let Err(error) = env.push_local_frame(16) {
                log_structured(
                    logger_core::Level::Error,
                    "glide_jni_response_conversion_stage_failed",
                    logger_core::structured_fields!(
                        "callback_id" => callback_id,
                        "stage" => "push_local_frame",
                        "response_type" => response_type,
                        "response_size_bytes" => response_size_bytes,
                        "conversion_path" => conversion_path,
                        "java_exception_pending" => env.exception_check().unwrap_or(false),
                        "error" => error.to_string(),
                    ),
                );
            }

            let java_result = if use_direct_buffer {
                create_direct_byte_buffer(env, server_value, !binary_mode)
            } else {
                crate::resp_value_to_java(env, server_value, !binary_mode)
            };

            if callback_coordination().take_timed_out(callback_id) {
                log_structured(
                    logger_core::Level::Warn,
                    "glide_jni_callback_dropped_after_timeout",
                    logger_core::structured_fields!(
                        "callback_id" => callback_id,
                        "binary_mode" => binary_mode,
                        "stage" => "after_response_conversion",
                    ),
                );
                let _ = unsafe { env.pop_local_frame(&JObject::null()) };
                finish_callback(
                    callback_id,
                    request_metrics.as_ref(),
                    callback_queue,
                    callback_complete,
                    false,
                    CallbackCompletion::TimedOutByMarker,
                );
                return;
            }

            let (command_succeeded, completion) = match java_result {
                Ok(java_result) => match complete_java_callback(
                    env,
                    callback_id,
                    &java_result,
                    detailed_completion,
                ) {
                    Ok(outcome) => {
                        if outcome == JavaCompletionOutcome::Completed {
                            log_structured(
                                logger_core::Level::Debug,
                                "glide_jni_callback_completed",
                                logger_core::structured_fields!(
                                    "callback_id" => callback_id,
                                    "binary_mode" => binary_mode,
                                    "result" => "success",
                                ),
                            );
                        }
                        (true, CallbackCompletion::Java(outcome))
                    }
                    Err(e) => {
                        log_structured(
                            logger_core::Level::Error,
                            "glide_jni_callback_completion_failed",
                            logger_core::structured_fields!(
                                "callback_id" => callback_id,
                                "binary_mode" => binary_mode,
                                "result" => "success",
                                "response_type" => response_type,
                                "response_size_bytes" => response_size_bytes,
                                "conversion_path" => conversion_path,
                                "error" => e.to_string(),
                            ),
                        );
                        describe_pending_java_exception_before_clear(
                            env,
                            Some(callback_id),
                            "complete_success_callback",
                        );
                        log::error!("JNI completion failed for callback {callback_id}: {e}");
                        let _ = env.exception_clear();
                        invalidate_jni_caches();
                        fail_all_pending_futures(
                            env,
                            "JNI callback completion failed — cached method IDs may be stale",
                        );
                        (
                            false,
                            CallbackCompletion::Failed(CallbackFailure::JniDelivery),
                        )
                    }
                },
                Err(e) => {
                    let error_code = 0;
                    let error_msg = format!("Response conversion failed: {e}");
                    log_structured(
                        logger_core::Level::Error,
                        "glide_jni_callback_response_conversion_failed",
                        logger_core::structured_fields!(
                            "callback_id" => callback_id,
                            "binary_mode" => binary_mode,
                            "error_code" => error_code,
                            "callback_worker" => callback_worker.as_str(),
                            "response_type" => response_type,
                            "response_size_bytes" => response_size_bytes,
                            "conversion_path" => conversion_path,
                            "native_buffer_count" => get_native_buffer_registry().len(),
                            "java_exception_pending" => env.exception_check().unwrap_or(false),
                            "error_message" => error_msg.as_str(),
                        ),
                    );
                    match complete_java_callback_with_error_code_for_native(
                        env,
                        callback_id,
                        error_code,
                        &error_msg,
                        detailed_completion,
                    ) {
                        Ok(outcome) => {
                            if outcome == JavaCompletionOutcome::Completed {
                                log_structured(
                                    logger_core::Level::Debug,
                                    "glide_jni_callback_completed",
                                    logger_core::structured_fields!(
                                        "callback_id" => callback_id,
                                        "binary_mode" => binary_mode,
                                        "result" => "conversion_error",
                                    ),
                                );
                                (
                                    false,
                                    CallbackCompletion::Failed(CallbackFailure::Conversion),
                                )
                            } else {
                                (false, CallbackCompletion::Java(outcome))
                            }
                        }
                        Err(e2) => {
                            log_structured(
                                logger_core::Level::Error,
                                "glide_jni_callback_completion_failed",
                                logger_core::structured_fields!(
                                    "callback_id" => callback_id,
                                    "binary_mode" => binary_mode,
                                    "result" => "conversion_error",
                                    "response_type" => response_type,
                                    "response_size_bytes" => response_size_bytes,
                                    "conversion_path" => conversion_path,
                                    "error" => e2.to_string(),
                                ),
                            );
                            log::error!(
                                "JNI error completion failed for callback {callback_id}: {e2}"
                            );
                            describe_pending_java_exception_before_clear(
                                env,
                                Some(callback_id),
                                "complete_conversion_error_callback",
                            );
                            let _ = env.exception_clear();
                            invalidate_jni_caches();
                            fail_all_pending_futures(
                                env,
                                "JNI error callback completion failed — cached method IDs may be stale",
                            );
                            (
                                false,
                                CallbackCompletion::Failed(CallbackFailure::JniDelivery),
                            )
                        }
                    }
                }
            };
            let _ = unsafe { env.pop_local_frame(&JObject::null()) };
            finish_callback(
                callback_id,
                request_metrics.as_ref(),
                callback_queue,
                callback_complete,
                command_succeeded,
                completion,
            );
        }
        Err(server_err) => {
            if callback_coordination().take_timed_out(callback_id) {
                log_structured(
                    logger_core::Level::Warn,
                    "glide_jni_callback_dropped_after_timeout",
                    logger_core::structured_fields!(
                        "callback_id" => callback_id,
                        "binary_mode" => binary_mode,
                        "stage" => "before_error_completion",
                        "server_error_kind" => format!("{:?}", server_err.kind()),
                        "server_error_type" => format!("{:?}", error_type(&server_err)),
                        "server_error_message" => error_message(&server_err),
                    ),
                );
                finish_callback(
                    callback_id,
                    request_metrics.as_ref(),
                    callback_queue,
                    callback_complete,
                    false,
                    CallbackCompletion::TimedOutByMarker,
                );
                return;
            }

            let error_code = error_type(&server_err) as i32;
            let error_msg = error_message(&server_err);
            log_structured(
                logger_core::Level::Warn,
                "glide_jni_callback_completing_error",
                logger_core::structured_fields!(
                    "callback_id" => callback_id,
                    "binary_mode" => binary_mode,
                    "error_code" => error_code,
                    "error_kind" => format!("{:?}", server_err.kind()),
                    "error_type" => format!("{:?}", error_type(&server_err)),
                    "error_message" => error_msg.as_str(),
                ),
            );
            let completion = match complete_java_callback_with_error_code_for_native(
                env,
                callback_id,
                error_code,
                &error_msg,
                detailed_completion,
            ) {
                Ok(outcome) => {
                    if outcome == JavaCompletionOutcome::Completed {
                        log_structured(
                            logger_core::Level::Debug,
                            "glide_jni_callback_completed",
                            logger_core::structured_fields!(
                                "callback_id" => callback_id,
                                "binary_mode" => binary_mode,
                                "result" => "server_error",
                            ),
                        );
                    }
                    CallbackCompletion::Java(outcome)
                }
                Err(e) => {
                    log_structured(
                        logger_core::Level::Error,
                        "glide_jni_callback_completion_failed",
                        logger_core::structured_fields!(
                            "callback_id" => callback_id,
                            "binary_mode" => binary_mode,
                            "result" => "server_error",
                            "error" => e.to_string(),
                        ),
                    );
                    describe_pending_java_exception_before_clear(
                        env,
                        Some(callback_id),
                        "complete_server_error_callback",
                    );
                    log::error!("JNI error completion failed for callback {callback_id}: {e}");
                    let _ = env.exception_clear();
                    invalidate_jni_caches();
                    fail_all_pending_futures(
                        env,
                        "JNI error callback completion failed — cached method IDs may be stale",
                    );
                    CallbackCompletion::Failed(CallbackFailure::JniDelivery)
                }
            };
            finish_callback(
                callback_id,
                request_metrics.as_ref(),
                callback_queue,
                callback_complete,
                false,
                completion,
            );
        }
    }
}

/// Enqueue callback job to dedicated workers.
/// If the channel is dead (all workers terminated), sweeps all pending futures with error.
pub fn complete_callback(
    jvm: Arc<JavaVM>,
    callback_id: jlong,
    result: CallbackResult,
    binary_mode: bool,
) {
    complete_callback_with_metrics(jvm, callback_id, result, binary_mode, None, false);
}

/// Enqueue a callback carrying its sampled direct-request lifecycle.
pub fn complete_callback_with_metrics(
    jvm: Arc<JavaVM>,
    callback_id: jlong,
    result: CallbackResult,
    binary_mode: bool,
    request_metrics: Option<Arc<RequestMetricContext>>,
    detailed_completion: bool,
) {
    match &result {
        Ok(server_value) => log_structured(
            logger_core::Level::Debug,
            "glide_jni_callback_enqueued",
            logger_core::structured_fields!(
                "callback_id" => callback_id,
                "binary_mode" => binary_mode,
                "result" => "success",
                "response_type" => crate::redis_value_type(server_value),
            ),
        ),
        Err(server_err) => log_structured(
            logger_core::Level::Warn,
            "glide_jni_callback_enqueued",
            logger_core::structured_fields!(
                "callback_id" => callback_id,
                "binary_mode" => binary_mode,
                "result" => "server_error",
                "server_error_kind" => format!("{:?}", server_err.kind()),
                "server_error_type" => format!("{:?}", error_type(server_err)),
                "server_error_message" => error_message(server_err),
            ),
        ),
    }

    let sender = init_callback_workers();
    let job = CallbackJob::new(
        callback_id,
        JavaCallbackPayload {
            result,
            binary_mode,
        },
        request_metrics,
        detailed_completion,
    );
    if let Err(e) = enqueue_callback_job(sender, job) {
        let error = e.to_string();
        log_structured(
            logger_core::Level::Error,
            "glide_jni_callback_enqueue_failed",
            logger_core::structured_fields!(
                "callback_id" => callback_id,
                "binary_mode" => binary_mode,
                "error" => error.as_str(),
            ),
        );
        let failed_job = e.0;
        finish_callback(
            failed_job.callback_id,
            failed_job.request_metrics.as_ref(),
            failed_job.callback_queue,
            None,
            false,
            CallbackCompletion::Failed(CallbackFailure::Channel),
        );
        log::error!("Callback channel dead, sweeping all pending futures: {error}");
        // Workers are dead — sweep the entire AsyncRegistry table
        if let Ok(mut env) = jvm.attach_current_thread_as_daemon() {
            fail_all_pending_futures(
                &mut env,
                "Native callback workers terminated — all pending requests failed",
            );
        } else {
            log::error!(
                "FATAL: Cannot attach to JVM to sweep futures — all pending requests will hang"
            );
        }
    }
}

/// Fail all pending futures in AsyncRegistry by calling failAllWithError from Java.
/// Used when fatal infrastructure failures are detected (channel dead, native panic).
pub fn fail_all_pending_futures(env: &mut JNIEnv, error_msg: &str) {
    log_structured(
        logger_core::Level::Warn,
        "glide_jni_fail_all_pending_futures_started",
        logger_core::structured_fields!(
            "reason" => error_msg,
        ),
    );
    let cache = match get_method_cache(env) {
        Ok(c) => c,
        Err(e) => {
            log_structured(
                logger_core::Level::Error,
                "glide_jni_fail_all_pending_futures_failed",
                logger_core::structured_fields!(
                    "stage" => "get_method_cache",
                    "reason" => error_msg,
                    "error" => e.to_string(),
                ),
            );
            log::error!("Cannot sweep futures — failed to get method cache: {e}");
            return;
        }
    };
    let _ = env.push_local_frame(4);
    let msg = match env.new_string(error_msg) {
        Ok(s) => s,
        Err(e) => {
            log_structured(
                logger_core::Level::Error,
                "glide_jni_fail_all_pending_futures_failed",
                logger_core::structured_fields!(
                    "stage" => "new_error_string",
                    "reason" => error_msg,
                    "error" => e.to_string(),
                ),
            );
            log::error!("Cannot sweep futures — failed to create error string: {e}");
            let _ = unsafe { env.pop_local_frame(&JObject::null()) };
            return;
        }
    };
    if let Err(e) = unsafe {
        env.call_static_method_unchecked(
            &cache.async_handle_table_class,
            cache.fail_all_method,
            signature::ReturnType::Primitive(signature::Primitive::Void),
            &[JValue::Object(&msg).as_jni()],
        )
    } {
        log_structured(
            logger_core::Level::Error,
            "glide_jni_fail_all_pending_futures_failed",
            logger_core::structured_fields!(
                "stage" => "call_fail_all_with_error",
                "reason" => error_msg,
                "error" => e.to_string(),
            ),
        );
        describe_pending_java_exception_before_clear(env, None, "fail_all_call_java");
        log::error!("Failed to sweep pending futures via failAllWithError: {e}");
        let _ = env.exception_clear();
    } else {
        log_structured(
            logger_core::Level::Warn,
            "glide_jni_fail_all_pending_futures_completed",
            logger_core::structured_fields!(
                "reason" => error_msg,
            ),
        );
    }
    let _ = unsafe { env.pop_local_frame(&JObject::null()) };
}

/// Complete Java CompletableFuture with success result using cached method IDs.
fn complete_java_callback(
    env: &mut JNIEnv,
    callback_id: jlong,
    result: &JObject,
    detailed_completion: bool,
) -> Result<JavaCompletionOutcome> {
    let method_cache = get_method_cache(env)?;
    let method = if detailed_completion {
        method_cache.complete_callback_for_native_method
    } else {
        method_cache.complete_callback_method
    };
    let return_type = if detailed_completion {
        jni::signature::ReturnType::Primitive(jni::signature::Primitive::Int)
    } else {
        jni::signature::ReturnType::Primitive(jni::signature::Primitive::Boolean)
    };

    let completed = unsafe {
        env.call_static_method_unchecked(
            &method_cache.async_handle_table_class,
            method,
            return_type,
            &[
                JValue::Long(callback_id).as_jni(),
                JValue::Object(result).as_jni(),
            ],
        )
    }?;

    if detailed_completion {
        JavaCompletionOutcome::try_from(completed.i()?)
    } else if completed.z()? {
        Ok(JavaCompletionOutcome::Completed)
    } else {
        Ok(JavaCompletionOutcome::Failure)
    }
}

/// Complete Java CompletableFuture with error code and message using cached method IDs.
pub fn complete_java_callback_with_error_code(
    env: &mut JNIEnv,
    callback_id: jlong,
    error_code: i32,
    error: &str,
    detailed_completion: bool,
) -> Result<()> {
    complete_java_callback_with_error_code_for_native(
        env,
        callback_id,
        error_code,
        error,
        detailed_completion,
    )
    .map(|_| ())
}

fn complete_java_callback_with_error_code_for_native(
    env: &mut JNIEnv,
    callback_id: jlong,
    error_code: i32,
    error: &str,
    detailed_completion: bool,
) -> Result<JavaCompletionOutcome> {
    let method_cache = get_method_cache(env)?;
    env.push_local_frame(4)?;
    let completion = (|| -> Result<JavaCompletionOutcome> {
        let error_string = env.new_string(error)?;
        let method = if detailed_completion {
            method_cache.complete_error_with_code_for_native_method
        } else {
            method_cache.complete_error_with_code_method
        };
        let return_type = if detailed_completion {
            jni::signature::ReturnType::Primitive(jni::signature::Primitive::Int)
        } else {
            jni::signature::ReturnType::Primitive(jni::signature::Primitive::Boolean)
        };
        let completed = unsafe {
            env.call_static_method_unchecked(
                &method_cache.async_handle_table_class,
                method,
                return_type,
                &[
                    JValue::Long(callback_id).as_jni(),
                    JValue::Int(error_code).as_jni(),
                    JValue::Object(&error_string).as_jni(),
                ],
            )
        }?;
        if detailed_completion {
            JavaCompletionOutcome::try_from(completed.i()?)
        } else if completed.z()? {
            Ok(JavaCompletionOutcome::Completed)
        } else {
            Ok(JavaCompletionOutcome::Failure)
        }
    })();
    unsafe { env.pop_local_frame(&JObject::null()) }?;
    completion
}

fn response_conversion_path(value: &ServerValue) -> &'static str {
    if should_use_direct_buffer(value)
        && matches!(
            value,
            ServerValue::BulkString(_) | ServerValue::Array(_) | ServerValue::Map(_)
        )
    {
        "direct_byte_buffer"
    } else {
        "java_object"
    }
}

/// Check if response should use DirectByteBuffer based on size threshold (16KB)
fn should_use_direct_buffer(value: &ServerValue) -> bool {
    const THRESHOLD: usize = 16 * 1024; // 16KB threshold

    match value {
        redis::Value::BulkString(data) => data.len() > THRESHOLD,
        redis::Value::Array(arr) => {
            // Only offload arrays composed of simple scalar types. Nested arrays/maps lose fidelity
            if arr.iter().any(|elem| !is_simple_scalar(elem)) {
                return false;
            }

            // Calculate total estimated size of array elements
            let total_size: usize = arr.iter().map(estimate_value_size).sum();
            total_size > THRESHOLD
        }
        redis::Value::Map(map) => {
            // Direct buffers are only safe when both keys and values are bulk strings; complex
            // structures (arrays, integers, maps) need full decoding to preserve types.
            if map.iter().any(|(k, v)| {
                !matches!(k, redis::Value::BulkString(_))
                    || !matches!(v, redis::Value::BulkString(_))
            }) {
                return false;
            }

            // Calculate total size of map (keys + values)
            let total_size: usize = map
                .iter()
                .map(|(k, v)| estimate_value_size(k) + estimate_value_size(v))
                .sum();
            total_size > THRESHOLD
        }
        redis::Value::Set(set) => {
            // Sets must also contain only scalar elements to be safely serialized.
            if set.iter().any(|elem| !is_simple_scalar(elem)) {
                return false;
            }

            // Calculate total size of set elements
            let total_size: usize = set.iter().map(estimate_value_size).sum();
            total_size > THRESHOLD
        }
        _ => false, // Other types (Int, Double, Boolean, etc.) are typically small
    }
}

fn is_simple_scalar(value: &ServerValue) -> bool {
    matches!(
        value,
        redis::Value::BulkString(_)
            | redis::Value::SimpleString(_)
            | redis::Value::Int(_)
            | redis::Value::Boolean(_)
            | redis::Value::Double(_)
            | redis::Value::Nil
            | redis::Value::Okay
            | redis::Value::BigNumber(_)
    )
}

/// Estimate the memory size of a ServerValue for threshold calculations
fn estimate_value_size(value: &ServerValue) -> usize {
    match value {
        redis::Value::Nil => 0,
        redis::Value::SimpleString(s) => s.len(),
        redis::Value::BulkString(data) => data.len(),
        redis::Value::Int(_) => 8,    // 64-bit int
        redis::Value::Double(_) => 8, // 64-bit double
        redis::Value::Boolean(_) => 1,
        redis::Value::Array(arr) => {
            arr.iter().map(estimate_value_size).sum::<usize>() + (arr.len() * 8) // overhead
        }
        redis::Value::Map(map) => {
            map.iter()
                .map(|(k, v)| estimate_value_size(k) + estimate_value_size(v))
                .sum::<usize>()
                + (map.len() * 16) // overhead for key-value pairs
        }
        redis::Value::Set(set) => {
            set.iter().map(estimate_value_size).sum::<usize>() + (set.len() * 8) // overhead
        }
        redis::Value::VerbatimString { text, .. } => text.len(),
        redis::Value::BigNumber(num) => num.to_string().len(), // Estimate size as string representation
        redis::Value::Push { data, .. } => data.iter().map(estimate_value_size).sum::<usize>(),
        redis::Value::ServerError(_) => 128, // Estimate for error messages
        redis::Value::Okay => 2,             // "OK"
        redis::Value::Attribute { data, .. } => estimate_value_size(data.as_ref()),
    }
}

/// Create DirectByteBuffer for large responses (>16KB) with zero-copy optimization
fn create_direct_byte_buffer<'local>(
    env: &mut JNIEnv<'local>,
    value: ServerValue,
    encoding_utf8: bool,
) -> Result<JObject<'local>, crate::errors::FFIError> {
    match value {
        redis::Value::BulkString(data) => {
            let (id, ptr, len) = register_native_buffer(data.into());
            let bb = unsafe { env.new_direct_byte_buffer(ptr.cast(), len)? };
            // Register Java-side cleaner to free native buffer when GC'd
            let obj: JObject = bb.into();
            let out = env.new_local_ref(&obj)?;
            register_buffer_cleaner(env, &out, id)?;
            Ok(out)
        }
        redis::Value::Array(arr) => {
            let serialized = serialize_array_to_bytes(arr, encoding_utf8)?;
            let (id, ptr, len) = register_native_buffer(serialized);
            let bb = unsafe { env.new_direct_byte_buffer(ptr.cast(), len)? };
            let obj: JObject = bb.into();
            let out = env.new_local_ref(&obj)?;
            register_buffer_cleaner(env, &out, id)?;
            Ok(out)
        }
        redis::Value::Map(map) => {
            let serialized = serialize_map_vec_to_bytes(map, encoding_utf8)?;
            let (id, ptr, len) = register_native_buffer(serialized);
            let bb = unsafe { env.new_direct_byte_buffer(ptr.cast(), len)? };
            let obj: JObject = bb.into();
            let out = env.new_local_ref(&obj)?;
            register_buffer_cleaner(env, &out, id)?;
            Ok(out)
        }
        _ => {
            // Fall back to regular conversion for other large types
            crate::resp_value_to_java(env, value, encoding_utf8)
        }
    }
}

fn register_buffer_cleaner<'local>(
    env: &mut JNIEnv<'local>,
    buffer: &JObject<'local>,
    id: u64,
) -> Result<(), crate::errors::FFIError> {
    let cache = get_glide_core_client_cache_safe(env).map_err(|_e| {
        // Map to a representative JNI error variant
        jni::errors::Error::JNIEnvMethodNotFound("GlideCoreClient cache")
    })?;
    unsafe {
        env.call_static_method_unchecked(
            &cache.class,
            cache.register_native_buffer_cleaner,
            signature::ReturnType::Primitive(signature::Primitive::Void),
            &[
                JValue::Object(buffer).as_jni(),
                JValue::Long(id as jlong).as_jni(),
            ],
        )?
    };

    Ok(())
}

/// Serialize array to bytes for DirectByteBuffer (simplified binary format)
fn serialize_array_to_bytes(
    arr: Vec<ServerValue>,
    _encoding_utf8: bool,
) -> Result<Vec<u8>, crate::errors::FFIError> {
    const NULL_VALUE: i32 = -1;
    const FALSE_BOOL: u8 = 0;
    const TRUE_BOOL: u8 = 1;

    let mut bytes = Vec::new();

    // Write array marker and length
    bytes.push(b'*'); // RESP array prefix
    bytes.extend_from_slice(&(arr.len() as u32).to_be_bytes());

    for value in arr {
        match value {
            redis::Value::Nil => {
                bytes.push(b'$'); // Bulk string marker
                bytes.extend_from_slice(&NULL_VALUE.to_be_bytes()); // -1 indicates null in binary format
            }
            redis::Value::BulkString(data) => {
                bytes.push(b'$'); // Bulk string marker
                bytes.extend_from_slice(&(data.len() as u32).to_be_bytes());
                bytes.extend_from_slice(&data);
            }
            redis::Value::SimpleString(s) => {
                // Normalize "ok" to "OK" while avoiding unnecessary allocations
                if s == "OK" {
                    let data = s.into_bytes();
                    bytes.push(b'+'); // Simple string marker
                    bytes.extend_from_slice(&(data.len() as u32).to_be_bytes());
                    bytes.extend_from_slice(&data);
                } else if s.eq_ignore_ascii_case("ok") {
                    bytes.push(b'+');
                    bytes.extend_from_slice(&2u32.to_be_bytes());
                    bytes.extend_from_slice(b"OK");
                } else {
                    let data = s.into_bytes();
                    bytes.push(b'+');
                    bytes.extend_from_slice(&(data.len() as u32).to_be_bytes());
                    bytes.extend_from_slice(&data);
                }
            }
            redis::Value::Okay => {
                let data = b"OK";
                bytes.push(b'+');
                bytes.extend_from_slice(&(data.len() as u32).to_be_bytes());
                bytes.extend_from_slice(data);
            }
            redis::Value::Int(n) => {
                bytes.push(b':'); // Integer marker
                bytes.extend_from_slice(&n.to_be_bytes());
            }
            redis::Value::Double(n) => {
                bytes.push(b','); // Double marker
                bytes.extend_from_slice(&n.to_be_bytes());
            }
            redis::Value::Boolean(b) => {
                bytes.push(b'?'); // Boolean marker
                bytes.push(if b { TRUE_BOOL } else { FALSE_BOOL });
            }
            redis::Value::BigNumber(n) => {
                let data = n.to_string().into_bytes();
                bytes.push(b'('); // BigNumber marker
                bytes.extend_from_slice(&(data.len() as u32).to_be_bytes());
                bytes.extend_from_slice(&data);
            }
            _ => {
                // For complex nested types, store as serialized string representation
                let repr = format!("{:?}", value);
                let data = repr.into_bytes();
                bytes.push(b'#'); // Complex type marker
                bytes.extend_from_slice(&(data.len() as u32).to_be_bytes());
                bytes.extend_from_slice(&data);
            }
        }
    }

    Ok(bytes)
}

/// Serialize map Vec<(K,V)> to bytes for DirectByteBuffer (simplified binary format)
fn serialize_map_vec_to_bytes(
    map: Vec<(ServerValue, ServerValue)>,
    _encoding_utf8: bool,
) -> Result<Vec<u8>, crate::errors::FFIError> {
    let mut bytes = Vec::new();

    // Write map marker and length
    bytes.push(b'%'); // Map prefix
    bytes.extend_from_slice(&(map.len() as u32).to_be_bytes());

    for (key, value) in map {
        // Serialize key
        if let redis::Value::BulkString(key_data) = key {
            bytes.extend_from_slice(&(key_data.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&key_data);
        } else {
            let key_repr = format!("{:?}", key).into_bytes();
            bytes.extend_from_slice(&(key_repr.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&key_repr);
        }

        // Serialize value
        if let redis::Value::BulkString(value_data) = value {
            bytes.extend_from_slice(&(value_data.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&value_data);
        } else {
            let value_repr = format!("{:?}", value).into_bytes();
            bytes.extend_from_slice(&(value_repr.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&value_repr);
        }
    }

    Ok(bytes)
}

/// Extract optional string parameter from JNI.
pub fn get_optional_string_param_raw(env: &mut JNIEnv, param: jstring) -> Option<String> {
    if param.is_null() {
        return None;
    }
    unsafe {
        let js = jni::objects::JString::from_raw(param);
        env.get_string(&js)
            .ok()
            .map(|s| s.to_str().unwrap_or("").to_string())
    }
}

/// JNI init hook to cache JVM and GlideCoreClient class/methods with correct classloader context.
///
/// This is called from `GlideCoreClient`'s static initializer in Java. We cache the class and
/// method IDs here rather than in `JNI_OnLoad` because `GlideCoreClient` may not be findable
/// via `env.find_class()` during `JNI_OnLoad` in environments with non-standard classloaders
/// (AWS Lambda, Spring Boot with nested JARs). The `class` parameter passed by JNI is already
/// loaded by the application classloader, bypassing `find_class` issues entirely.
///
/// Other caches (`MethodCache`, `JavaValueConversionCache`) are safe to initialize in
/// `JNI_OnLoad` because they only reference standard Java classes (`java/lang/Long`,
/// `java/util/HashMap`, etc.) which are always available from the bootstrap classloader.
#[unsafe(no_mangle)]
pub extern "system" fn Java_glide_internal_GlideCoreClient_onNativeInit(
    mut env: JNIEnv,
    class: JClass,
) {
    // Cache JVM
    if let Ok(jvm) = env.get_java_vm() {
        let _ = JVM.set(Arc::new(jvm));
    }
    let jvm_cached = JVM.get().is_some();

    // Cache GlideCoreClient class and method IDs with correct classloader context.
    // The 'class' parameter is GlideCoreClient, already loaded by the application classloader.
    let mut core_client_cache_initialized = false;
    if let Ok(global) = env.new_global_ref(&class)
        && let (Ok(on_native_push), Ok(register_cleaner)) = (
            env.get_static_method_id(&class, "onNativePush", "(J[B[B[B)V"),
            env.get_static_method_id(
                &class,
                "registerNativeBufferCleaner",
                "(Ljava/nio/ByteBuffer;J)V",
            ),
        )
    {
        let cache = GlideCoreClientCache {
            class: global,
            on_native_push,
            register_native_buffer_cleaner: register_cleaner,
        };
        let cache_mutex = GLIDE_CORE_CLIENT_CACHE.get_or_init(|| Mutex::new(None));
        *cache_mutex.lock() = Some(cache);
        core_client_cache_initialized = true;
    }

    log_structured(
        if core_client_cache_initialized {
            logger_core::Level::Debug
        } else {
            logger_core::Level::Error
        },
        if core_client_cache_initialized {
            "glide_jni_core_client_cache_initialized"
        } else {
            "glide_jni_core_client_cache_initialization_failed"
        },
        logger_core::structured_fields!(
            "source" => "application_classloader",
            "jvm_cached" => jvm_cached,
            "core_client_cache_initialized" => core_client_cache_initialized,
            "java_exception_pending" => env.exception_check().unwrap_or(false),
        ),
    );
}

/// Native free for DirectByteBuffer-backed native memory (called by Java Cleaner)
#[unsafe(no_mangle)]
pub extern "system" fn Java_glide_internal_GlideCoreClient_freeNativeBuffer(
    _env: JNIEnv,
    _class: JClass,
    id: jlong,
) {
    let id = id as u64;
    let _ = free_native_buffer(id);
}

#[derive(Clone)]
struct GlideCoreClientCache {
    class: GlobalRef,
    on_native_push: JStaticMethodID,
    register_native_buffer_cleaner: JStaticMethodID,
}

static GLIDE_CORE_CLIENT_CACHE: std::sync::OnceLock<Mutex<Option<GlideCoreClientCache>>> =
    std::sync::OnceLock::new();

/// Get GlideCoreClient cache, with fallback dynamic initialization.
///
/// Preferred path: return the cache populated by `onNativeInit` (correct classloader context).
/// Fallback: if `onNativeInit` wasn't called or failed, attempt `find_class` with the provided
/// `env`. This may fail in non-standard classloader environments but keeps the client resilient
/// in standard JVM setups.
fn get_glide_core_client_cache_safe(env: &mut JNIEnv) -> Result<GlideCoreClientCache> {
    let cache_mutex = GLIDE_CORE_CLIENT_CACHE.get_or_init(|| Mutex::new(None));
    {
        let guard = cache_mutex.lock();
        if let Some(ref cache) = *guard {
            return Ok(cache.clone());
        }
    }

    // Fallback: try to initialize dynamically using the provided env
    log_structured(
        logger_core::Level::Warn,
        "glide_jni_core_client_cache_fallback_started",
        logger_core::structured_fields!(
            "source" => "callback_thread_find_class",
        ),
    );
    let class = env.find_class("glide/internal/GlideCoreClient")?;
    let global = env.new_global_ref(&class)?;
    let on_native_push = env.get_static_method_id(&class, "onNativePush", "(J[B[B[B)V")?;
    let register_cleaner = env.get_static_method_id(
        &class,
        "registerNativeBufferCleaner",
        "(Ljava/nio/ByteBuffer;J)V",
    )?;

    let cache = GlideCoreClientCache {
        class: global,
        on_native_push,
        register_native_buffer_cleaner: register_cleaner,
    };

    let mut guard = cache_mutex.lock();
    if guard.is_none() {
        *guard = Some(cache);
    }

    log_structured(
        logger_core::Level::Warn,
        "glide_jni_core_client_cache_initialized",
        logger_core::structured_fields!(
            "source" => "callback_thread_find_class",
        ),
    );

    Ok(guard.as_ref().cloned().unwrap())
}

/// Complete a callback with an error synchronously from the JNI calling thread.
/// This bypasses the async callback channel entirely — used for immediate rejection
/// (e.g., inflight limit exceeded) where the Java thread must not park.
///
/// Requires the calling thread to already have a JNIEnv (which JNI entry points always do).
pub fn complete_error_sync(
    env: &mut JNIEnv,
    callback_id: jni::sys::jlong,
    message: &str,
    error_code: i32,
    detailed_completion: bool,
) {
    let Ok(method_cache) = get_method_cache(env) else {
        log::error!(
            "complete_error_sync: method cache not initialized for callback_id={}",
            callback_id
        );
        return;
    };

    // Create the error message string — a small Java heap allocation,
    // not affected by native memory pressure.
    let Ok(error_msg) = env.new_string(message) else {
        log::error!(
            "complete_error_sync: failed to create error string for callback_id={}",
            callback_id
        );
        return;
    };

    let method = if detailed_completion {
        method_cache.complete_error_with_code_for_native_method
    } else {
        method_cache.complete_error_with_code_method
    };
    let return_type = if detailed_completion {
        jni::signature::ReturnType::Primitive(jni::signature::Primitive::Int)
    } else {
        jni::signature::ReturnType::Primitive(jni::signature::Primitive::Boolean)
    };

    let result = unsafe {
        env.call_static_method_unchecked(
            &method_cache.async_handle_table_class,
            method,
            return_type,
            &[
                jni::sys::jvalue { j: callback_id },
                jni::sys::jvalue { i: error_code },
                jni::sys::jvalue {
                    l: error_msg.as_raw(),
                },
            ],
        )
    };

    if let Err(e) = result {
        log::error!(
            "complete_error_sync: JNI call failed for callback_id={}: {:?}",
            callback_id,
            e
        );
        let _ = env.exception_clear();
    }
}

#[cfg(test)]
mod tests {
    use super::{response_conversion_path, serialize_array_to_bytes};
    use redis::{Value, parse_redis_value};

    #[test]
    fn bulk_string_diagnostics_identify_direct_buffer_threshold() {
        let threshold_sized = Value::BulkString(vec![0; 16 * 1024].into());
        assert_eq!(response_conversion_path(&threshold_sized), "java_object");

        let above_threshold = Value::BulkString(vec![0; 16 * 1024 + 1].into());
        assert_eq!(
            response_conversion_path(&above_threshold),
            "direct_byte_buffer"
        );
    }

    #[test]
    fn serialize_array_to_bytes_encodes_bool_double_bignumber_and_nil() {
        let big_number_value = parse_redis_value(b"(123456789012345678901234567890\r\n").unwrap();
        let Value::BigNumber(big_number) = big_number_value else {
            panic!("expected big number from parser");
        };

        let payload = vec![
            Value::Boolean(true),
            Value::Double(42.25),
            Value::BigNumber(big_number),
            Value::Nil,
        ];

        let bytes = match serialize_array_to_bytes(payload, false) {
            Ok(bytes) => bytes,
            Err(err) => panic!("serialization failed: {err}"),
        };

        // Array header: '*' + 4-byte element count.
        assert_eq!(bytes[0], b'*');
        assert_eq!(u32::from_be_bytes(bytes[1..5].try_into().unwrap()), 4);

        // Element 1: boolean true ('?'+1).
        assert_eq!(bytes[5], b'?');
        assert_eq!(bytes[6], 1);

        // Element 2: double (',' + 8 bytes).
        assert_eq!(bytes[7], b',');
        let decoded_double = f64::from_be_bytes(bytes[8..16].try_into().unwrap());
        assert_eq!(decoded_double, 42.25);

        // Element 3: big number ('(' + len + utf8 digits).
        assert_eq!(bytes[16], b'(');
        let big_number_len = u32::from_be_bytes(bytes[17..21].try_into().unwrap()) as usize;
        let big_number_text = std::str::from_utf8(&bytes[21..21 + big_number_len]).unwrap();
        assert_eq!(big_number_text, "123456789012345678901234567890");

        // Element 4: null bulk string ('$' + -1).
        let null_offset = 21 + big_number_len;
        assert_eq!(bytes[null_offset], b'$');
        assert_eq!(
            i32::from_be_bytes(bytes[null_offset + 1..null_offset + 5].try_into().unwrap()),
            -1
        );
    }
}

#[cfg(test)]
mod callback_metrics_tests {
    use super::{
        CallbackCompletion, CallbackCoordinationFinish, CallbackCoordinationRegistry,
        CallbackFailure, CallbackJob, JavaCompletionOutcome, enqueue_callback_job,
        finish_callback_with_registry, receive_callback_job,
    };
    use glide_core::native_request_metrics::{
        BoundedOperation, RequestMetricContext, RequestMetricPhase, RequestMetricResult,
        RequestMetricsState,
    };
    use parking_lot::Mutex;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    fn sampled_context(
        state: &Arc<RequestMetricsState>,
        operation: &'static str,
    ) -> Arc<RequestMetricContext> {
        state
            .start(BoundedOperation::known(operation))
            .expect("100% sampling must create a context")
    }

    #[tokio::test(start_paused = true)]
    async fn production_jobs_measure_queue_and_completion_across_a_controlled_barrier() {
        let state = RequestMetricsState::new(100, 4, &[]).unwrap();
        let first_context = sampled_context(&state, "FIRST");
        let second_context = sampled_context(&state, "SECOND");
        let registry = CallbackCoordinationRegistry::default();
        let (sender, receiver) = mpsc::channel::<CallbackJob<()>>();
        tokio::time::advance(Duration::from_millis(7)).await;

        enqueue_callback_job(
            &sender,
            CallbackJob::new(-9_000_001, (), Some(first_context), true),
        )
        .unwrap();
        enqueue_callback_job(
            &sender,
            CallbackJob::new(-9_000_002, (), Some(second_context), true),
        )
        .unwrap();

        let (first, first_complete) = receive_callback_job(&receiver).unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let advancing_barrier = Arc::clone(&barrier);
        let controlled_interval = Duration::from_millis(25);
        let advance = tokio::spawn(async move {
            advancing_barrier.wait().await;
            tokio::time::advance(controlled_interval).await;
        });
        barrier.wait().await;
        advance.await.unwrap();

        finish_callback_with_registry(
            &registry,
            first.callback_id,
            first.request_metrics.as_ref(),
            first.callback_queue,
            first_complete,
            true,
            CallbackCompletion::Java(JavaCompletionOutcome::Completed),
        );
        let (second, second_complete) = receive_callback_job(&receiver).unwrap();
        finish_callback_with_registry(
            &registry,
            second.callback_id,
            second.request_metrics.as_ref(),
            second.callback_queue,
            second_complete,
            true,
            CallbackCompletion::Java(JavaCompletionOutcome::Completed),
        );

        let drain = state.drain(4).unwrap();
        let first_sample = drain
            .samples()
            .iter()
            .find(|sample| state.operation_bytes(sample.operation()) == b"FIRST")
            .unwrap();
        let second_sample = drain
            .samples()
            .iter()
            .find(|sample| state.operation_bytes(sample.operation()) == b"SECOND")
            .unwrap();
        assert_eq!(
            first_sample.phase_duration(RequestMetricPhase::CallbackComplete),
            controlled_interval.as_nanos() as u64
        );
        assert_eq!(
            second_sample.phase_duration(RequestMetricPhase::CallbackQueue),
            controlled_interval.as_nanos() as u64
        );
        assert!(second_sample.phase_duration(RequestMetricPhase::CallbackComplete) > 0);
        assert_eq!(registry.len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn sender_failure_uses_the_production_job_and_terminal_finalizer() {
        let state = RequestMetricsState::new(100, 2, &[]).unwrap();
        let context = sampled_context(&state, "SEND.FAILURE");
        let registry = CallbackCoordinationRegistry::default();
        let (sender, receiver) = mpsc::channel::<CallbackJob<()>>();
        drop(receiver);

        let failed_job = enqueue_callback_job(
            &sender,
            CallbackJob::new(-9_000_003, (), Some(context), true),
        )
        .unwrap_err()
        .0;
        tokio::time::advance(Duration::from_millis(3)).await;
        let result = finish_callback_with_registry(
            &registry,
            failed_job.callback_id,
            failed_job.request_metrics.as_ref(),
            failed_job.callback_queue,
            None,
            false,
            CallbackCompletion::Failed(CallbackFailure::Channel),
        );

        assert_eq!(result, RequestMetricResult::Failure);
        let drain = state.drain(2).unwrap();
        assert_eq!(drain.samples().len(), 1);
        assert_eq!(
            drain.samples()[0].phase_duration(RequestMetricPhase::CallbackQueue),
            Duration::from_millis(3).as_nanos() as u64
        );
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn callback_finalizer_closes_active_phases_cleans_registry_and_emits_once() {
        let state = RequestMetricsState::new(100, 2, &[]).unwrap();
        let context = sampled_context(&state, "GET");
        let queue = context.start_phase(RequestMetricPhase::CallbackQueue);
        let completion = Some(context.start_phase(RequestMetricPhase::CallbackComplete));
        let registry = CallbackCoordinationRegistry::default();

        finish_callback_with_registry(
            &registry,
            -9_000_004,
            Some(&context),
            Some(queue),
            completion,
            false,
            CallbackCompletion::Java(JavaCompletionOutcome::Completed),
        );
        finish_callback_with_registry(
            &registry,
            -9_000_004,
            Some(&context),
            None,
            None,
            true,
            CallbackCompletion::Java(JavaCompletionOutcome::Completed),
        );

        let drain = state.drain(2).unwrap();
        assert_eq!(drain.samples().len(), 1);
        assert_eq!(drain.samples()[0].result(), RequestMetricResult::Failure);
        assert!(drain.samples()[0].phase_duration(RequestMetricPhase::CallbackQueue) > 0);
        assert!(drain.samples()[0].phase_duration(RequestMetricPhase::CallbackComplete) > 0);
        assert!(drain.samples()[0].phase_duration(RequestMetricPhase::Total) > 0);
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn java_and_delivery_outcomes_follow_the_terminal_contract() {
        let registry = CallbackCoordinationRegistry::default();
        let cases = [
            (
                true,
                CallbackCompletion::Java(JavaCompletionOutcome::Completed),
                RequestMetricResult::Success,
            ),
            (
                false,
                CallbackCompletion::Java(JavaCompletionOutcome::Completed),
                RequestMetricResult::Failure,
            ),
            (
                true,
                CallbackCompletion::Java(JavaCompletionOutcome::TimeoutMarkMissed),
                RequestMetricResult::Timeout,
            ),
            (
                false,
                CallbackCompletion::Java(JavaCompletionOutcome::NativeTimeout),
                RequestMetricResult::Timeout,
            ),
            (
                true,
                CallbackCompletion::Java(JavaCompletionOutcome::Cancelled),
                RequestMetricResult::Cancelled,
            ),
            (
                true,
                CallbackCompletion::Java(JavaCompletionOutcome::Failure),
                RequestMetricResult::Failure,
            ),
            (
                true,
                CallbackCompletion::Failed(CallbackFailure::Conversion),
                RequestMetricResult::Failure,
            ),
            (
                true,
                CallbackCompletion::Failed(CallbackFailure::JniDelivery),
                RequestMetricResult::Failure,
            ),
            (
                true,
                CallbackCompletion::TimedOutByMarker,
                RequestMetricResult::Timeout,
            ),
        ];

        for (offset, (redis_succeeded, completion, expected)) in cases.into_iter().enumerate() {
            let callback_id = -9_001_000 - offset as i64;
            assert_eq!(
                finish_callback_with_registry(
                    &registry,
                    callback_id,
                    None,
                    None,
                    None,
                    redis_succeeded,
                    completion,
                ),
                expected
            );
        }
        assert_eq!(registry.len(), 0);
        assert!(JavaCompletionOutcome::try_from(99).is_err());
    }

    #[test]
    fn timeout_only_coordination_covers_all_mark_and_completion_orders() {
        let registry = CallbackCoordinationRegistry::default();
        let in_flight = Mutex::new(std::collections::HashSet::new());

        in_flight.lock().insert(-9_000_005);
        assert!(registry.mark_timed_out(-9_000_005, || in_flight.lock().contains(&-9_000_005)));
        assert!(registry.take_timed_out(-9_000_005));
        assert!(
            !registry.finish(-9_000_005, CallbackCoordinationFinish::None, || {
                in_flight.lock().remove(&-9_000_005);
            })
        );

        in_flight.lock().insert(-9_000_006);
        assert!(registry.mark_timed_out(-9_000_006, || in_flight.lock().contains(&-9_000_006)));
        assert!(registry.finish(
            -9_000_006,
            CallbackCoordinationFinish::CheckForTimeout,
            || {
                in_flight.lock().remove(&-9_000_006);
            }
        ));

        in_flight.lock().insert(-9_000_007);
        assert!(!registry.finish(
            -9_000_007,
            CallbackCoordinationFinish::AwaitTimeoutMark,
            || {
                in_flight.lock().remove(&-9_000_007);
            }
        ));
        assert_eq!(registry.len(), 1);
        assert!(registry.mark_timed_out(-9_000_007, || false));

        assert!(!registry.mark_timed_out(-9_000_010, || false));
        assert!(!registry.finish(-9_000_010, CallbackCoordinationFinish::None, || {}));

        in_flight.lock().insert(-9_000_011);
        assert!(
            !registry.finish(-9_000_011, CallbackCoordinationFinish::None, || {
                in_flight.lock().remove(&-9_000_011);
            })
        );
        assert!(!registry.mark_timed_out(-9_000_011, || in_flight.lock().contains(&-9_000_011)));
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn owned_timeout_overrides_java_state_cleanup_and_does_not_poison_reused_id() {
        let registry = CallbackCoordinationRegistry::default();
        let callback_id = -9_000_012;

        assert!(registry.mark_timed_out(callback_id, || true));
        assert_eq!(
            finish_callback_with_registry(
                &registry,
                callback_id,
                None,
                None,
                None,
                true,
                CallbackCompletion::Java(JavaCompletionOutcome::Failure),
            ),
            RequestMetricResult::Timeout
        );
        assert_eq!(registry.len(), 0);

        assert_eq!(
            finish_callback_with_registry(
                &registry,
                callback_id,
                None,
                None,
                None,
                true,
                CallbackCompletion::Java(JavaCompletionOutcome::Completed),
            ),
            RequestMetricResult::Success
        );
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn unsampled_production_job_constructs_no_timers_and_cleans_coordination() {
        let registry = CallbackCoordinationRegistry::default();
        let (sender, receiver) = mpsc::channel::<CallbackJob<()>>();
        enqueue_callback_job(&sender, CallbackJob::new(-9_000_008, (), None, false)).unwrap();

        let (job, callback_complete) = receive_callback_job(&receiver).unwrap();

        assert!(job.request_metrics.is_none());
        assert!(job.callback_queue.is_none());
        assert!(callback_complete.is_none());
        finish_callback_with_registry(
            &registry,
            job.callback_id,
            None,
            None,
            None,
            true,
            CallbackCompletion::Java(JavaCompletionOutcome::Completed),
        );
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn callback_job_retains_selection_without_relying_on_native_context() {
        let (sender, receiver) = mpsc::channel::<CallbackJob<()>>();
        enqueue_callback_job(&sender, CallbackJob::new(-9_000_009, (), None, false)).unwrap();
        enqueue_callback_job(&sender, CallbackJob::new(-9_000_010, (), None, true)).unwrap();

        let (legacy, _) = receive_callback_job(&receiver).unwrap();
        let (detailed, _) = receive_callback_job(&receiver).unwrap();

        assert!(!legacy.detailed_completion);
        assert!(detailed.detailed_completion);
        assert!(legacy.request_metrics.is_none());
        assert!(detailed.request_metrics.is_none());
    }
}

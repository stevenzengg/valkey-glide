/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import glide.api.logging.Logger;
import glide.api.models.exceptions.CircuitBreakerException;
import glide.api.models.exceptions.ClosingException;
import glide.api.models.exceptions.ExecAbortException;
import glide.api.models.exceptions.RequestException;
import glide.api.models.exceptions.TimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongPredicate;

/**
 * Async registry for correlating native callbacks with Java {@link CompletableFuture}s.
 *
 * <p>Responsibilities:
 *
 * <ul>
 *   <li>Maintain a thread-safe mapping from correlation id to the original future
 *   <li>Enforce per-client max inflight requests in Java (0 = defer to core default)
 *   <li>Schedule optional Java-side timeouts with cancellable tasks
 *   <li>Perform atomic cleanup on completion to avoid races and leaks
 * </ul>
 *
 * <p>Timeouts can be enforced at the Java layer (for immediate user feedback) or deferred to the
 * Rust core (when timeoutMillis = 0). Backpressure defaults and concurrency tuning are handled by
 * the Rust core.
 */
public final class AsyncRegistry {

    /** Rate-limit interval for timeout/disconnect log messages (in nanoseconds) */
    private static final long LOG_RATE_LIMIT_NS = 5_000_000_000L; // 5 seconds

    /** Last log timestamp for timeout errors */
    private static final AtomicLong lastTimeoutLogNs = new AtomicLong(0);

    /** Last log timestamp for disconnect errors */
    private static final AtomicLong lastDisconnectLogNs = new AtomicLong(0);

    /** Suppressed timeout log count since last emitted log */
    private static final AtomicLong suppressedTimeoutLogs = new AtomicLong(0);

    /** Suppressed disconnect log count since last emitted log */
    private static final AtomicLong suppressedDisconnectLogs = new AtomicLong(0);

    /** Thread-safe storage for active futures. Using ConcurrentHashMap for lock-free operations. */
    private static final ConcurrentHashMap<Long, CompletableFuture<Object>> activeFutures =
            new ConcurrentHashMap<>(estimateInitialCapacity());

    /**
     * Terminal state retained until the native callback consumes the exact completion outcome.
     * Cleanup can run synchronously inside {@code CompletableFuture.complete*}, so the active-future
     * table cannot also own this native delivery handshake.
     */
    private static final ConcurrentHashMap<Long, CompletionState> completionStates =
            new ConcurrentHashMap<>(estimateInitialCapacity());

    private enum CompletionOutcome {
        COMPLETED(0),
        TIMEOUT(1),
        CANCELLED(2),
        FAILURE(3),
        TIMEOUT_MARK_MISSED(4),
        NATIVE_TIMEOUT(5);

        private final int nativeCode;

        CompletionOutcome(int nativeCode) {
            this.nativeCode = nativeCode;
        }

        private boolean acceptedNativeDelivery() {
            return this == COMPLETED || this == NATIVE_TIMEOUT;
        }
    }

    private static final class CompletionState {
        private final CompletableFuture<Object> future;
        private final CompletableFuture<?> terminalFuture;
        private CompletionOutcome internallyCompletedAs;

        private CompletionState(CompletableFuture<Object> future, CompletableFuture<?> terminalFuture) {
            this.future = future;
            this.terminalFuture = terminalFuture;
        }

        private CompletionOutcome acceptedCompletionOutcome(CompletionOutcome deliveredAs) {
            if (terminalFuture != future && terminalFuture.isCancelled()) {
                return CompletionOutcome.CANCELLED;
            }
            if (deliveredAs != CompletionOutcome.NATIVE_TIMEOUT
                    && terminalFuture != future
                    && terminalFuture.isCompletedExceptionally()) {
                return CompletionOutcome.FAILURE;
            }
            return deliveredAs;
        }

        private CompletionOutcome rejectedCompletionOutcome() {
            if (internallyCompletedAs != null) {
                return internallyCompletedAs;
            }
            return terminalFuture.isCancelled() || future.isCancelled()
                    ? CompletionOutcome.CANCELLED
                    : CompletionOutcome.FAILURE;
        }
    }

    /** Scheduled timeout tasks mapped by correlation ID for cancellation on completion. */
    private static final ConcurrentHashMap<Long, ScheduledFuture<?>> timeoutTasks =
            new ConcurrentHashMap<>();

    /**
     * Per-client inflight request counters. Maps client handle to the number of active requests for
     * that client.
     */
    private static final ConcurrentHashMap<Long, AtomicInteger> clientInflightCounts =
            new ConcurrentHashMap<>();

    /** Thread-safe ID generator for correlation IDs. */
    private static final AtomicLong nextId = new AtomicLong(1);

    /** Registration timestamps for measuring elapsed time on errors. */
    private static final ConcurrentHashMap<Long, Long> registrationTimestamps =
            new ConcurrentHashMap<>();

    /**
     * Shutdown flag to prevent race conditions between register() and shutdown()/failAllWithError().
     * Once set to true, register() will return pre-failed futures instead of adding to the registry.
     */
    private static final AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
     * Single-threaded scheduler for timeout tasks. Uses a daemon thread so it won't prevent JVM
     * shutdown. Tasks are cancellable via {@link ScheduledFuture#cancel(boolean)}.
     */
    private static final ScheduledExecutorService timeoutScheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "GlideTimeoutScheduler");
                        t.setDaemon(true);
                        return t;
                    });

    private static final Thread shutdownHook =
            new Thread(AsyncRegistry::handleJvmShutdown, "AsyncRegistry-Shutdown");

    static {
        if (!"false".equalsIgnoreCase(System.getProperty("glide.autoShutdownHook", "true"))) {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }

    /**
     * Handler invoked by the automatic JVM shutdown hook.
     *
     * <p>This is intentionally non-destructive. When the JVM is exiting, all shutdown hooks (this one
     * and any registered by the user) run concurrently, and the client must remain usable so that a
     * user's own shutdown hook can still issue commands (e.g. to persist state before exit). Setting
     * the {@link #isShutdown} gate or cancelling in-flight futures here would abort those legitimate
     * requests and previously surfaced as {@code ClosingException: Client is shutting down} (see <a
     * href="https://github.com/valkey-io/valkey-glide/issues/4809">#4809</a>).
     *
     * <p>Eager cleanup is unnecessary at JVM exit: all internal GLIDE threads (callback workers,
     * tokio runtime, timeout scheduler, cleaner) are daemon threads, and native resources are
     * reclaimed by the OS once the process terminates. Deterministic teardown remains available
     * through the explicit {@link #shutdown()} method and {@link
     * glide.internal.GlideCoreClient#close()}.
     */
    static void handleJvmShutdown() {
        // Intentionally a no-op: keep the client usable for concurrent user shutdown hooks.
    }

    private static void logLifecycle(Logger.Level level, long correlationId, String event) {
        Long startedAtNanos = registrationTimestamps.get(correlationId);
        Logger.log(
                level,
                "glide_java_async_registry",
                () ->
                        "{"
                                + "\"glide_structured\":true,"
                                + "\"glide_event\":\"glide_java_async_registry_"
                                + event
                                + "\","
                                + "\"correlation_id\":"
                                + correlationId
                                + ","
                                + "\"active_future_count\":"
                                + activeFutures.size()
                                + ","
                                + "\"pending_timeout_count\":"
                                + timeoutTasks.size()
                                + ","
                                + "\"duration_ms\":"
                                + durationMillis(startedAtNanos)
                                + "}");
    }

    private static void logLifecycle(
            Logger.Level level, long correlationId, String event, String extraJsonFields) {
        Long startedAtNanos = registrationTimestamps.get(correlationId);
        Logger.log(
                level,
                "glide_java_async_registry",
                () ->
                        "{"
                                + "\"glide_structured\":true,"
                                + "\"glide_event\":\"glide_java_async_registry_"
                                + event
                                + "\","
                                + "\"correlation_id\":"
                                + correlationId
                                + ","
                                + "\"active_future_count\":"
                                + activeFutures.size()
                                + ","
                                + "\"pending_timeout_count\":"
                                + timeoutTasks.size()
                                + ","
                                + "\"duration_ms\":"
                                + durationMillis(startedAtNanos)
                                + (extraJsonFields == null || extraJsonFields.trim().isEmpty()
                                        ? ""
                                        : "," + extraJsonFields)
                                + "}");
    }

    private static long durationMillis(Long startedAtNanos) {
        if (startedAtNanos == null) {
            return -1L;
        }
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
    }

    private static String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder escaped = new StringBuilder(value.length() + 2);
        escaped.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    escaped.append("\\\"");
                    break;
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) c));
                    } else {
                        escaped.append(c);
                    }
            }
        }
        escaped.append('"');
        return escaped.toString();
    }

    /** Estimate initial capacity for the active futures map using inflight limit with margin. */
    private static int estimateInitialCapacity() {
        String env = System.getenv("GLIDE_MAX_INFLIGHT_REQUESTS");
        if (env != null) {
            try {
                int v = Integer.parseInt(env.trim());
                if (v > 0) return Math.max(16, v * 2);
            } catch (NumberFormatException ignored) {
            }
        }

        String prop = System.getProperty("glide.maxInflightRequests");
        if (prop != null) {
            try {
                int v = Integer.parseInt(prop.trim());
                if (v > 0) return Math.max(16, v * 2);
            } catch (NumberFormatException ignored) {
            }
        }

        return 2000; // Default with margin over core's 1000
    }

    /**
     * Register future with client-specific inflight limit, client handle for per-client tracking, and
     * optional Java-side timeout.
     *
     * <p>If the registry is shutting down, the future will be completed exceptionally with a
     * ClosingException and a special correlation ID (0) will be returned to indicate the registration
     * failed.
     *
     * @param future the future to register
     * @param maxInflightRequests per-client limit (0 = no Java-side limit, defer to core)
     * @param clientHandle native client handle for tracking
     * @param timeoutMillis Java-side timeout in milliseconds (0 = use Rust default timeout)
     * @return correlation ID for native callback, or 0 if shutdown is in progress
     */
    public static <T> long register(
            CompletableFuture<T> future, int maxInflightRequests, long clientHandle, long timeoutMillis) {
        return register(future, future, maxInflightRequests, clientHandle, timeoutMillis);
    }

    /**
     * Register a native completion future and the terminal command future returned to the caller. The
     * terminal future lets callback metrics include synchronous response handling and preserve
     * caller-visible cancellation/failure outcomes.
     */
    public static <T, R> long register(
            CompletableFuture<T> future,
            CompletableFuture<R> terminalFuture,
            int maxInflightRequests,
            long clientHandle,
            long timeoutMillis) {
        if (future == null) {
            throw new IllegalArgumentException("Future cannot be null");
        }
        if (terminalFuture == null) {
            throw new IllegalArgumentException("Terminal future cannot be null");
        }

        // Check shutdown flag before registering to prevent race conditions
        // This ensures no futures are added after shutdown() starts clearing
        if (isShutdown.get()) {
            future.completeExceptionally(
                    new ClosingException("Client is shutting down, cannot register new requests"));
            return 0L; // Special ID indicating registration failed
        }

        // Client-specific inflight limit check
        // 0 means "use native/core defaults" - no limit enforcement in Java layer
        if (maxInflightRequests > 0) {
            enforceInflightLimit(clientHandle, maxInflightRequests);
        }

        long correlationId = nextId.getAndIncrement();

        // Store the original future
        @SuppressWarnings("unchecked")
        CompletableFuture<Object> originalFuture = (CompletableFuture<Object>) future;

        // Store original future for completion by native code
        activeFutures.put(correlationId, originalFuture);
        completionStates.put(correlationId, new CompletionState(originalFuture, terminalFuture));
        registrationTimestamps.put(correlationId, System.nanoTime());
        logLifecycle(
                Logger.Level.DEBUG,
                correlationId,
                "registered",
                "\"client_handle\":"
                        + clientHandle
                        + ",\"max_inflight_requests\":"
                        + maxInflightRequests
                        + ",\"timeout_ms\":"
                        + timeoutMillis);

        // Double-check shutdown flag after insertion to handle race with shutdown()
        // If shutdown started between our first check and the put(), clean up and fail
        if (isShutdown.get()) {
            activeFutures.remove(correlationId);
            completionStates.remove(correlationId);
            registrationTimestamps.remove(correlationId);
            if (maxInflightRequests > 0) {
                decrementInflightCount(clientHandle);
            }
            future.completeExceptionally(
                    new ClosingException("Client is shutting down, cannot register new requests"));
            return 0L;
        }

        // Schedule Java-side timeout if configured (0 = defer to Rust core timeout)
        if (timeoutMillis > 0) {
            scheduleTimeout(correlationId, timeoutMillis);
        }

        // Set up cleanup on the original future
        // This ensures proper resource cleanup when completed
        setupCleanup(correlationId, originalFuture, maxInflightRequests, clientHandle);
        if (terminalFuture != future) {
            terminalFuture.whenComplete(
                    (result, error) -> {
                        if (terminalFuture.isCancelled()) {
                            originalFuture.cancel(false);
                        }
                    });
        }

        return correlationId;
    }

    /** Enforce per-client inflight limit, throwing RequestException if exceeded. */
    private static void enforceInflightLimit(long clientHandle, int maxInflightRequests) {
        clientInflightCounts.compute(
                clientHandle,
                (key, counter) -> {
                    AtomicInteger value = counter != null ? counter : new AtomicInteger(0);
                    if (value.incrementAndGet() > maxInflightRequests) {
                        value.decrementAndGet();
                        throw new RequestException("Client reached maximum inflight requests");
                    }
                    return value;
                });
    }

    /**
     * Schedule a cancellable timeout task. If the request doesn't complete within timeoutMillis, the
     * future is completed exceptionally with TimeoutException and the native layer is notified.
     */
    private static void scheduleTimeout(long correlationId, long timeoutMillis) {
        ScheduledFuture<?> task =
                timeoutScheduler.schedule(
                        () -> {
                            timeoutTasks.remove(correlationId);
                            completeTimeout(correlationId, timeoutMillis, GlideNativeBridge::markTimedOut);
                        },
                        timeoutMillis,
                        TimeUnit.MILLISECONDS);
        timeoutTasks.put(correlationId, task);
    }

    /**
     * Publish a Java timeout outcome before notifying native code. The injected notifier is the
     * production native timeout boundary and keeps the race independently testable.
     */
    static boolean completeTimeout(
            long correlationId, long timeoutMillis, LongPredicate timeoutNotifier) {
        CompletionState state = completionStates.get(correlationId);
        if (state == null) {
            logLifecycle(
                    Logger.Level.DEBUG,
                    correlationId,
                    "timeout_skipped_missing_state",
                    "\"timeout_ms\":" + timeoutMillis);
            return false;
        }

        boolean completed;
        synchronized (state) {
            completed = state.future.completeExceptionally(new TimeoutException("Request timed out"));
            if (completed) {
                state.internallyCompletedAs = CompletionOutcome.TIMEOUT;
                logLifecycle(
                        Logger.Level.WARN, correlationId, "timed_out", "\"timeout_ms\":" + timeoutMillis);
                boolean nativeOwnsTimeout = timeoutNotifier.test(correlationId);
                if (nativeOwnsTimeout) {
                    completionStates.remove(correlationId, state);
                } else {
                    // Publish the missed mark before a concurrent native completion can query the
                    // state. Otherwise it could return TIMEOUT after the only mark already failed.
                    state.internallyCompletedAs = CompletionOutcome.TIMEOUT_MARK_MISSED;
                }
            }
        }
        if (!completed) {
            logLifecycle(
                    Logger.Level.DEBUG,
                    correlationId,
                    "timeout_skipped_already_completed",
                    "\"timeout_ms\":" + timeoutMillis);
            return false;
        }
        return true;
    }

    /**
     * Set up cleanup handler for when the future completes (success, error, or timeout). Performs
     * atomic cleanup to avoid races and leaks.
     */
    private static void setupCleanup(
            long correlationId,
            CompletableFuture<Object> future,
            int maxInflightRequests,
            long clientHandle) {
        future.whenComplete(
                (result, error) -> {
                    logLifecycle(
                            error == null ? Logger.Level.DEBUG : Logger.Level.WARN,
                            correlationId,
                            "cleanup",
                            "\"completed_with_error\":"
                                    + (error != null)
                                    + ",\"error_type\":"
                                    + jsonString(error == null ? null : error.getClass().getName())
                                    + ",\"error_message\":"
                                    + jsonString(error == null ? null : error.getMessage()));

                    // Atomic cleanup - no race conditions
                    activeFutures.remove(correlationId);
                    registrationTimestamps.remove(correlationId);

                    // Cancel the timeout task if it hasn't fired yet
                    // Using cancel(false) to avoid interrupting the scheduler thread
                    ScheduledFuture<?> timeoutTask = timeoutTasks.remove(correlationId);
                    if (timeoutTask != null) {
                        timeoutTask.cancel(false);
                    }

                    // Decrement per-client counter if applicable
                    if (maxInflightRequests > 0) {
                        decrementInflightCount(clientHandle);
                    }
                });
    }

    /** Decrement inflight count for client, removing the entry when it reaches zero. */
    private static void decrementInflightCount(long clientHandle) {
        clientInflightCounts.computeIfPresent(
                clientHandle,
                (key, counter) -> {
                    int remaining = counter.decrementAndGet();
                    // Clean up the entry when no more inflight requests
                    // to avoid leaking counters for inactive clients
                    return remaining <= 0 ? null : counter;
                });
    }

    /**
     * Complete callback with proper race condition handling. Returns false if already completed or
     * timed out.
     *
     * @param correlationId the correlation ID from register()
     * @param result the result to complete with
     * @return true if completed, false if already done
     */
    public static boolean completeCallback(long correlationId, Object result) {
        return completeCallbackForNative(correlationId, result)
                == CompletionOutcome.COMPLETED.nativeCode;
    }

    /**
     * Complete a callback and return its exact terminal outcome to native code. The per-entry monitor
     * spans {@link CompletableFuture#complete(Object)} and synchronous dependent work, then reads any
     * competing timeout, cancellation, or shutdown outcome.
     */
    public static int completeCallbackForNative(long correlationId, Object result) {
        CompletionState state = completionStates.get(correlationId);
        if (state == null) {
            logLifecycle(Logger.Level.WARN, correlationId, "complete_success_missing_future");
            return CompletionOutcome.FAILURE.nativeCode;
        }

        logLifecycle(
                Logger.Level.DEBUG,
                correlationId,
                "complete_success_attempt",
                "\"result_type\":" + jsonString(result == null ? null : result.getClass().getName()));
        CompletionOutcome outcome;
        synchronized (state) {
            outcome =
                    state.future.complete(result)
                            ? state.acceptedCompletionOutcome(CompletionOutcome.COMPLETED)
                            : state.rejectedCompletionOutcome();
        }
        completionStates.remove(correlationId, state);
        logLifecycle(
                outcome == CompletionOutcome.COMPLETED ? Logger.Level.DEBUG : Logger.Level.WARN,
                correlationId,
                outcome == CompletionOutcome.COMPLETED
                        ? "complete_success"
                        : "complete_success_already_completed",
                "\"result_type\":"
                        + jsonString(result == null ? null : result.getClass().getName())
                        + ",\"completion_outcome\":"
                        + jsonString(outcome.name()));
        return outcome.nativeCode;
    }

    /**
     * Complete with error using a structured error code from native layer. Codes map to glide-core
     * RequestErrorType: 0=Unspecified, 1=ExecAbort, 2=Timeout, 3=Disconnect.
     *
     * @param correlationId the correlation ID from register()
     * @param errorTypeCode error type code from native layer
     * @param errorMessage error message from native layer
     * @return true if completed, false if already done
     */
    public static boolean completeCallbackWithErrorCode(
            long correlationId, int errorTypeCode, String errorMessage) {
        int outcome =
                completeCallbackWithErrorCodeForNative(correlationId, errorTypeCode, errorMessage);
        return outcome == CompletionOutcome.COMPLETED.nativeCode
                || outcome == CompletionOutcome.NATIVE_TIMEOUT.nativeCode;
    }

    /** Complete an exceptional callback and return its exact terminal outcome to native code. */
    public static int completeCallbackWithErrorCodeForNative(
            long correlationId, int errorTypeCode, String errorMessage) {
        CompletionState state = completionStates.get(correlationId);
        if (state == null) {
            logLifecycle(
                    Logger.Level.WARN,
                    correlationId,
                    "complete_error_missing_future",
                    "\"error_type_code\":"
                            + errorTypeCode
                            + ",\"error_message\":"
                            + jsonString(errorMessage));
            return CompletionOutcome.FAILURE.nativeCode;
        }

        String msg =
                (errorMessage == null || errorMessage.trim().isEmpty())
                        ? "Unknown error from native code"
                        : errorMessage;

        // Log elapsed time for timeout and disconnect errors (rate-limited)
        if (errorTypeCode == 2 || errorTypeCode == 3) {
            Long registeredAt = registrationTimestamps.get(correlationId);
            if (registeredAt != null) {
                long elapsedMs = (System.nanoTime() - registeredAt) / 1_000_000;
                boolean isTimeout = errorTypeCode == 2;
                AtomicLong lastLogRef = isTimeout ? lastTimeoutLogNs : lastDisconnectLogNs;
                AtomicLong suppressedRef = isTimeout ? suppressedTimeoutLogs : suppressedDisconnectLogs;
                String errorTypeName = isTimeout ? "Timeout" : "Disconnect";

                long now = System.nanoTime();
                long lastLog = lastLogRef.get();
                if (now - lastLog >= LOG_RATE_LIMIT_NS && lastLogRef.compareAndSet(lastLog, now)) {
                    long suppressed = suppressedRef.getAndSet(0);
                    String suffix = suppressed > 0 ? " (suppressed " + suppressed + " similar)" : "";
                    Logger.log(
                            Logger.Level.WARN,
                            "AsyncRegistry",
                            errorTypeName + " after " + elapsedMs + "ms: " + msg + suffix);
                } else {
                    suppressedRef.incrementAndGet();
                }
            }
        }

        RuntimeException ex;
        switch (errorTypeCode) {
            case 2:
                ex = new TimeoutException(msg);
                break;
            case 3:
                ex = new ClosingException(msg);
                break;
            case 1:
                ex = new ExecAbortException(msg);
                break;
            case 4:
                ex = new CircuitBreakerException(msg);
                break;
            default:
                ex = new RequestException(msg);
                break;
        }

        logLifecycle(
                Logger.Level.WARN,
                correlationId,
                "complete_error_attempt",
                "\"error_type_code\":"
                        + errorTypeCode
                        + ",\"exception_type\":"
                        + jsonString(ex.getClass().getName())
                        + ",\"error_message\":"
                        + jsonString(msg));
        CompletionOutcome outcome;
        synchronized (state) {
            CompletionOutcome deliveredAs =
                    errorTypeCode == 2 ? CompletionOutcome.NATIVE_TIMEOUT : CompletionOutcome.COMPLETED;
            outcome =
                    state.future.completeExceptionally(ex)
                            ? state.acceptedCompletionOutcome(deliveredAs)
                            : state.rejectedCompletionOutcome();
        }
        completionStates.remove(correlationId, state);
        logLifecycle(
                Logger.Level.WARN,
                correlationId,
                outcome.acceptedNativeDelivery() ? "complete_error" : "complete_error_already_completed",
                "\"error_type_code\":"
                        + errorTypeCode
                        + ",\"exception_type\":"
                        + jsonString(ex.getClass().getName())
                        + ",\"error_message\":"
                        + jsonString(msg)
                        + ",\"completion_outcome\":"
                        + jsonString(outcome.name()));
        return outcome.nativeCode;
    }

    /** Get current pending operation count. */
    public static int getPendingCount() {
        return activeFutures.size();
    }

    /**
     * Explicit, destructive shutdown cleanup - cancel all pending operations and stop the timeout
     * scheduler. Sets the {@link #isShutdown} gate so no new requests are accepted afterward.
     *
     * <p>This is <em>not</em> wired to the automatic JVM shutdown hook (that path is intentionally
     * non-destructive; see {@link #handleJvmShutdown()}). Call this only when you want deterministic
     * teardown of the registry.
     */
    public static void shutdown() {
        // Set shutdown flag first to prevent new registrations
        // This must happen before any clearing to avoid race conditions
        isShutdown.set(true);

        // Cancel timeout tasks without interrupting (they're just scheduled, not running)
        timeoutTasks.values().forEach(task -> task.cancel(false));
        timeoutTasks.clear();

        cancelPendingForShutdown();
        registrationTimestamps.clear();
        clientInflightCounts.clear();

        // Shutdown the timeout scheduler
        timeoutScheduler.shutdownNow();
    }

    /** Publish shutdown as failure before cancelling pending futures. */
    static void cancelPendingForShutdown() {
        completionStates.forEach(
                (correlationId, state) -> {
                    synchronized (state) {
                        state.internallyCompletedAs = CompletionOutcome.FAILURE;
                        state.future.cancel(true);
                    }
                });
        activeFutures.clear();
        completionStates.clear();
    }

    /**
     * Fail all pending futures with a {@link ClosingException}. Called from the native layer when a
     * fatal infrastructure failure is detected (e.g., callback worker threads terminated or native
     * panic). This ensures no future is left dangling.
     *
     * @param errorMessage description of the failure cause
     */
    public static void failAllWithError(String errorMessage) {
        // Set shutdown flag first to prevent new registrations
        // This must happen before any clearing to avoid race conditions
        isShutdown.set(true);

        String msg =
                (errorMessage == null || errorMessage.isEmpty())
                        ? "Native callback infrastructure failed"
                        : errorMessage;
        completionStates.forEach(
                (correlationId, state) -> {
                    synchronized (state) {
                        state.internallyCompletedAs = CompletionOutcome.FAILURE;
                        state.future.completeExceptionally(new ClosingException(msg));
                    }
                });
        activeFutures.clear();
        completionStates.clear();
        registrationTimestamps.clear();

        timeoutTasks.values().forEach(task -> task.cancel(false));
        timeoutTasks.clear();
        clientInflightCounts.clear();
    }

    /** Clean up per-client tracking when a client is closed. */
    public static void cleanupClient(long clientHandle) {
        clientInflightCounts.remove(clientHandle);
    }

    /** Reset all internal state. Intended for test isolation and client shutdown cleanup. */
    public static void reset() {
        // Reset shutdown flag first to allow new registrations
        isShutdown.set(false);

        // Cancel timeout tasks without interrupting
        timeoutTasks.values().forEach(task -> task.cancel(false));
        timeoutTasks.clear();
        activeFutures.clear();
        completionStates.clear();
        registrationTimestamps.clear();
        clientInflightCounts.clear();
        nextId.set(1);
    }

    /**
     * Returns the count of pending timeout tasks. Intended for testing to verify timeout tasks are
     * cancelled properly and don't accumulate.
     *
     * @return number of active timeout tasks
     */
    public static int getPendingTimeoutCount() {
        return timeoutTasks.size();
    }

    /**
     * Returns the count of active futures. Intended for testing to verify futures are cleaned up
     * properly.
     *
     * @return number of active futures
     */
    public static int getActiveFutureCount() {
        return activeFutures.size();
    }

    /**
     * Returns whether the registry is in shutdown state. Intended for testing and diagnostics.
     *
     * @return true if shutdown() or failAllWithError() has been called
     */
    public static boolean isShutdown() {
        return isShutdown.get();
    }

    /**
     * Remove the automatic shutdown hook, allowing users to manage shutdown manually. Call this if
     * you want to control shutdown behavior yourself.
     */
    public static void removeShutdownHook() {
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
            // Hook was never registered or already removed
        }
    }
}

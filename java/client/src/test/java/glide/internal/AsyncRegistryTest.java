/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import glide.api.models.exceptions.ClosingException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(5)
public class AsyncRegistryTest {
    private static final int COMPLETED = 0;
    private static final int TIMEOUT = 1;
    private static final int CANCELLED = 2;
    private static final int FAILURE = 3;
    private static final int TIMEOUT_MARK_MISSED = 4;
    private static final int NATIVE_TIMEOUT = 5;

    @BeforeEach
    void setUp() {
        AsyncRegistry.reset();
    }

    @Test
    void failAllWithError_completesAllPendingFutures() {
        CompletableFuture<Object> f1 = new CompletableFuture<>();
        CompletableFuture<Object> f2 = new CompletableFuture<>();
        CompletableFuture<Object> f3 = new CompletableFuture<>();

        // timeoutMillis=0 avoids native call to markTimedOut
        AsyncRegistry.register(f1, 0, 1L, 0);
        AsyncRegistry.register(f2, 0, 1L, 0);
        AsyncRegistry.register(f3, 0, 1L, 0);

        assertEquals(3, AsyncRegistry.getActiveFutureCount());

        AsyncRegistry.failAllWithError("test error");

        assertTrue(f1.isCompletedExceptionally());
        assertTrue(f2.isCompletedExceptionally());
        assertTrue(f3.isCompletedExceptionally());
        assertEquals(0, AsyncRegistry.getActiveFutureCount());
        assertEquals(0, AsyncRegistry.getPendingTimeoutCount());

        assertClosingException(f1, "test error");
        assertClosingException(f2, "test error");
        assertClosingException(f3, "test error");
    }

    @Test
    void failAllWithError_withNullMessage_usesDefault() {
        CompletableFuture<Object> f = new CompletableFuture<>();
        AsyncRegistry.register(f, 0, 1L, 0);

        AsyncRegistry.failAllWithError(null);

        assertTrue(f.isCompletedExceptionally());
        assertClosingException(f, "Native callback infrastructure failed");
    }

    @Test
    void failAllWithError_withEmptyMessage_usesDefault() {
        CompletableFuture<Object> f = new CompletableFuture<>();
        AsyncRegistry.register(f, 0, 1L, 0);

        AsyncRegistry.failAllWithError("");

        assertTrue(f.isCompletedExceptionally());
        assertClosingException(f, "Native callback infrastructure failed");
    }

    @Test
    void failAllWithError_withEmptyTable_isNoOp() {
        assertEquals(0, AsyncRegistry.getActiveFutureCount());
        assertDoesNotThrow(() -> AsyncRegistry.failAllWithError("msg"));
        assertEquals(0, AsyncRegistry.getActiveFutureCount());
    }

    @Test
    void failAllWithError_raceWithNormalCompletion() {
        CompletableFuture<Object> f = new CompletableFuture<>();
        long id = AsyncRegistry.register(f, 0, 1L, 0);

        // Complete normally first
        AsyncRegistry.completeCallback(id, "normal result");

        // Then sweep — should not override the normal result
        AsyncRegistry.failAllWithError("late error");

        assertTrue(f.isDone());
        // First completion wins — should have normal result, not exception
        assertEquals("normal result", f.getNow(null));
    }

    @Test
    void failAllWithError_clearsInflightCounters() {
        CompletableFuture<Object> f1 = new CompletableFuture<>();
        // Register with inflight limit of 1
        AsyncRegistry.register(f1, 1, 42L, 0);

        // Sweep clears counters
        AsyncRegistry.failAllWithError("msg");

        // Should be able to register again on the same client (counter was reset)
        CompletableFuture<Object> f2 = new CompletableFuture<>();
        assertDoesNotThrow(() -> AsyncRegistry.register(f2, 1, 42L, 0));
    }

    // ==================== Shutdown Race Condition Tests ====================

    @Test
    void register_afterShutdown_returnsZeroAndFailsFuture() {
        // First, trigger shutdown
        AsyncRegistry.failAllWithError("shutdown");
        assertTrue(AsyncRegistry.isShutdown());

        // Now try to register a new future
        CompletableFuture<Object> f = new CompletableFuture<>();
        long id = AsyncRegistry.register(f, 0, 1L, 0);

        // Should return 0 (special ID indicating registration failed)
        assertEquals(0L, id);

        // Future should be completed exceptionally
        assertTrue(f.isCompletedExceptionally());
        assertClosingException(f, "Client is shutting down, cannot register new requests");

        // Should not be added to active futures
        assertEquals(0, AsyncRegistry.getActiveFutureCount());
    }

    @Test
    void failAllWithError_setsShutdownFlag() {
        assertFalse(AsyncRegistry.isShutdown());

        AsyncRegistry.failAllWithError("test");

        assertTrue(AsyncRegistry.isShutdown());
    }

    @Test
    void reset_clearsShutdownFlag() {
        // Trigger shutdown
        AsyncRegistry.failAllWithError("test");
        assertTrue(AsyncRegistry.isShutdown());

        // Reset should clear the flag
        AsyncRegistry.reset();

        assertFalse(AsyncRegistry.isShutdown());

        // Should be able to register again
        CompletableFuture<Object> f = new CompletableFuture<>();
        long id = AsyncRegistry.register(f, 0, 1L, 0);

        assertTrue(id > 0);
        assertEquals(1, AsyncRegistry.getActiveFutureCount());
    }

    @Test
    void register_afterShutdown_doesNotIncrementInflightCounter() {
        // Trigger shutdown
        AsyncRegistry.failAllWithError("shutdown");

        // Try to register with inflight limit
        CompletableFuture<Object> f = new CompletableFuture<>();
        long id = AsyncRegistry.register(f, 10, 42L, 0);

        assertEquals(0L, id);

        // Reset and verify we can register the full limit (counter wasn't incremented)
        AsyncRegistry.reset();

        for (int i = 0; i < 10; i++) {
            CompletableFuture<Object> fi = new CompletableFuture<>();
            long regId = AsyncRegistry.register(fi, 10, 42L, 0);
            assertTrue(regId > 0, "Registration " + i + " should succeed");
        }
    }

    @Test
    void isShutdown_initiallyFalse() {
        assertFalse(AsyncRegistry.isShutdown());
    }

    // ==================== JVM Shutdown Hook Behavior (issue #4809) ====================

    @Test
    void handleJvmShutdown_doesNotSetShutdownFlag() {
        assertFalse(AsyncRegistry.isShutdown());

        AsyncRegistry.handleJvmShutdown();

        // The automatic JVM-exit hook must be non-destructive so concurrent user shutdown
        // hooks can keep using the client.
        assertFalse(AsyncRegistry.isShutdown());
    }

    @Test
    void handleJvmShutdown_allowsSubsequentRegistration() {
        // Simulate the JVM exit hook firing.
        AsyncRegistry.handleJvmShutdown();

        // A command issued from a user's own shutdown hook (running concurrently) must still
        // register successfully rather than being rejected with a ClosingException. This is the
        // regression guard for issue #4809.
        CompletableFuture<Object> f = new CompletableFuture<>();
        long id = AsyncRegistry.register(f, 0, 1L, 0);

        assertTrue(id > 0, "register() must succeed after the JVM shutdown hook runs");
        assertFalse(f.isDone(), "future must not be pre-failed");
        assertEquals(1, AsyncRegistry.getActiveFutureCount());
    }

    @Test
    void handleJvmShutdown_doesNotCancelPendingFutures() {
        CompletableFuture<Object> f = new CompletableFuture<>();
        // Register with a Java-side timeout so we also cover the scheduled timeout-task path.
        AsyncRegistry.register(f, 0, 1L, 60_000);

        assertEquals(1, AsyncRegistry.getActiveFutureCount());
        assertEquals(1, AsyncRegistry.getPendingTimeoutCount());

        AsyncRegistry.handleJvmShutdown();

        // In-flight requests must not be aborted by the JVM-exit hook; they are left to complete
        // (or be reclaimed at process exit). Both futures and their scheduled timeout tasks must
        // survive.
        assertFalse(f.isDone());
        assertEquals(1, AsyncRegistry.getActiveFutureCount());
        assertEquals(1, AsyncRegistry.getPendingTimeoutCount());

        // Clean up: cancel the abandoned future so its 60s timeout task is cancelled and doesn't
        // outlive this test and invoke GlideNativeBridge.markTimedOut in the test JVM.
        f.cancel(true);
    }

    @Test
    void nativeSuccessReturnsOnlyAfterSynchronousCompletionActionsFinish() throws Exception {
        CompletableFuture<Object> future = new CompletableFuture<>();
        long correlationId = register(future);
        CountDownLatch actionStarted = new CountDownLatch(1);
        CountDownLatch releaseAction = new CountDownLatch(1);
        future.thenRun(
                () -> {
                    actionStarted.countDown();
                    await(releaseAction);
                });
        AtomicInteger outcome = new AtomicInteger(-1);

        Thread completion =
                new Thread(
                        () -> outcome.set(AsyncRegistry.completeCallbackForNative(correlationId, "response")));
        completion.start();

        assertTrue(actionStarted.await(1, TimeUnit.SECONDS));
        assertTrue(completion.isAlive());
        releaseAction.countDown();
        completion.join();

        assertEquals(COMPLETED, outcome.get());
        assertEquals("response", future.join());
    }

    @Test
    void nativeErrorDeliveryIsCompletedButExternalExceptionalCompletionIsFailure() {
        CompletableFuture<Object> deliveredError = new CompletableFuture<>();
        long deliveredErrorId = register(deliveredError);
        assertEquals(
                COMPLETED,
                AsyncRegistry.completeCallbackWithErrorCodeForNative(deliveredErrorId, 0, "server error"));
        assertTrue(deliveredError.isCompletedExceptionally());

        CompletableFuture<Object> externallyFailed = new CompletableFuture<>();
        long externallyFailedId = register(externallyFailed);
        externallyFailed.completeExceptionally(new IllegalStateException("external failure"));
        assertEquals(
                FAILURE, AsyncRegistry.completeCallbackForNative(externallyFailedId, "late response"));
    }

    @Test
    void terminalCommandFutureControlsCancellationAndHandlerFailureOutcomes() {
        CompletableFuture<Object> cancellationRoot = new CompletableFuture<>();
        CompletableFuture<String> cancelledCommand = cancellationRoot.thenApply(Object::toString);
        long cancelledId = AsyncRegistry.register(cancellationRoot, cancelledCommand, 0, 17L, 0);

        cancelledCommand.cancel(false);

        assertTrue(cancellationRoot.isCancelled());
        assertEquals(CANCELLED, AsyncRegistry.completeCallbackForNative(cancelledId, "late response"));

        CompletableFuture<Object> handlerFailureRoot = new CompletableFuture<>();
        CompletableFuture<String> failedCommand =
                handlerFailureRoot.thenApply(
                        ignored -> {
                            throw new IllegalStateException("handler failed");
                        });
        long failedId = AsyncRegistry.register(handlerFailureRoot, failedCommand, 0, 17L, 0);

        assertEquals(FAILURE, AsyncRegistry.completeCallbackForNative(failedId, "response"));
        assertTrue(failedCommand.isCompletedExceptionally());
    }

    @Test
    void nativeTimeoutDeliveryHasADistinctTerminalOutcome() {
        CompletableFuture<Object> root = new CompletableFuture<>();
        CompletableFuture<String> command = root.thenApply(Object::toString);
        long correlationId = AsyncRegistry.register(root, command, 0, 17L, 0);

        assertEquals(
                NATIVE_TIMEOUT,
                AsyncRegistry.completeCallbackWithErrorCodeForNative(
                        correlationId, 2, "core request timed out"));
        assertTrue(command.isCompletedExceptionally());
    }

    @Test
    void explicitCancellationAndMissingEntriesHaveDistinctOutcomes() {
        CompletableFuture<Object> cancelled = new CompletableFuture<>();
        long cancelledId = register(cancelled);
        cancelled.cancel(false);

        assertEquals(CANCELLED, AsyncRegistry.completeCallbackForNative(cancelledId, "late response"));
        assertEquals(FAILURE, AsyncRegistry.completeCallbackForNative(Long.MAX_VALUE, "missing"));
    }

    @Test
    void timeoutOutcomeIsPublishedBeforeNativeTimeoutNotification() throws Exception {
        CompletableFuture<Object> future = new CompletableFuture<>();
        long correlationId = register(future);
        CountDownLatch notifierEntered = new CountDownLatch(1);
        CountDownLatch releaseNotifier = new CountDownLatch(1);
        AtomicLong notifiedId = new AtomicLong(-1);
        AtomicInteger outcome = new AtomicInteger(-1);

        Thread timeout =
                new Thread(
                        () ->
                                AsyncRegistry.completeTimeout(
                                        correlationId,
                                        25,
                                        id -> {
                                            notifiedId.set(id);
                                            notifierEntered.countDown();
                                            await(releaseNotifier);
                                            return true;
                                        }));
        timeout.start();

        assertTrue(notifierEntered.await(1, TimeUnit.SECONDS));
        assertTrue(future.isCompletedExceptionally());
        Thread completion =
                new Thread(
                        () ->
                                outcome.set(
                                        AsyncRegistry.completeCallbackForNative(correlationId, "late response")));
        completion.start();
        awaitThreadBlocked(completion);
        releaseNotifier.countDown();
        timeout.join();
        completion.join();

        assertEquals(correlationId, notifiedId.get());
        assertEquals(TIMEOUT, outcome.get());
    }

    @Test
    void timeoutThatLosesToCompletionDoesNotNotifyOrReclassify() {
        CompletableFuture<Object> future = new CompletableFuture<>();
        long correlationId = register(future);
        assertEquals(COMPLETED, AsyncRegistry.completeCallbackForNative(correlationId, "response"));
        AtomicInteger notifications = new AtomicInteger();

        assertFalse(
                AsyncRegistry.completeTimeout(
                        correlationId,
                        25,
                        ignored -> {
                            notifications.incrementAndGet();
                            return true;
                        }));
        assertEquals(0, notifications.get());
    }

    @Test
    void timeoutBeforeNativeRegistrationIsRetainedUntilTheCallbackQueriesIt() {
        CompletableFuture<Object> future = new CompletableFuture<>();
        long correlationId = register(future);

        assertTrue(AsyncRegistry.completeTimeout(correlationId, 25, ignored -> false));

        assertEquals(
                TIMEOUT_MARK_MISSED,
                AsyncRegistry.completeCallbackForNative(correlationId, "late response"));
        assertEquals(
                FAILURE, AsyncRegistry.completeCallbackForNative(correlationId, "duplicate response"));
    }

    @Test
    void missedTimeoutMarkIsPublishedBeforeAConcurrentNativeQuery() throws Exception {
        CompletableFuture<Object> future = new CompletableFuture<>();
        long correlationId = register(future);
        AtomicInteger outcome = new AtomicInteger(-1);
        Thread[] completion = new Thread[1];

        assertTrue(
                AsyncRegistry.completeTimeout(
                        correlationId,
                        25,
                        ignored -> {
                            completion[0] =
                                    new Thread(
                                            () ->
                                                    outcome.set(
                                                            AsyncRegistry.completeCallbackForNative(
                                                                    correlationId, "late response")));
                            completion[0].start();
                            awaitThreadBlocked(completion[0]);
                            return false;
                        }));
        completion[0].join();

        assertEquals(TIMEOUT_MARK_MISSED, outcome.get());
    }

    @Test
    void resetClearsATimeoutTombstoneWhenNoNativeEntryEverArrives() {
        CompletableFuture<Object> future = new CompletableFuture<>();
        long correlationId = register(future);
        assertTrue(AsyncRegistry.completeTimeout(correlationId, 25, ignored -> false));

        AsyncRegistry.reset();

        assertEquals(
                FAILURE, AsyncRegistry.completeCallbackForNative(correlationId, "missing response"));
    }

    @Test
    void shutdownCancellationIsFailureRatherThanUserCancellation() {
        CompletableFuture<Object> future = new CompletableFuture<>();
        long correlationId = register(future);

        AsyncRegistry.cancelPendingForShutdown();

        assertTrue(future.isCancelled());
        assertEquals(FAILURE, AsyncRegistry.completeCallbackForNative(correlationId, "late response"));
    }

    private static long register(CompletableFuture<Object> future) {
        return AsyncRegistry.register(future, 0, 17, 0);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(1, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static void awaitThreadBlocked(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (thread.getState() != Thread.State.BLOCKED && thread.isAlive()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("completion did not block on the exact entry state");
            }
            Thread.yield();
        }
        assertEquals(Thread.State.BLOCKED, thread.getState());
    }

    private static void assertClosingException(CompletableFuture<?> future, String expectedMessage) {
        try {
            future.get();
        } catch (ExecutionException e) {
            assertInstanceOf(ClosingException.class, e.getCause());
            assertEquals(expectedMessage, e.getCause().getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Unexpected interruption", e);
        }
    }
}

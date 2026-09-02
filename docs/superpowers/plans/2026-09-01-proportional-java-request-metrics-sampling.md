# Proportional Java Request-Metrics Sampling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Java request-metrics overhead proportional to 0/1/5/100% selection while retaining exact terminal outcomes for selected commands, case-insensitive custom labels, and locked Rust dependency integrity.

**Architecture:** A Java binding-local sampler makes one decision before command registration. The decision controls terminal `CompletionState`, is forwarded through JNI, and creates a preselected native context without a second random draw. Unselected callbacks use the legacy `main` completion methods; selected callbacks use the detailed terminal-outcome protocol.

**Tech Stack:** Java 11, `CompletableFuture`, JNI, Rust, Cargo, JUnit 5, Mockito, JMH.

## Global Constraints

- Sampling percentages remain integer values from 0 through 100.
- One selection applies to all phases and retries of a Java direct command.
- Unselected commands create neither Java terminal metrics state nor native request lifecycle state.
- Selected timeout, cancellation, response-handler failure, and normal completion remain exactly-once.
- Batch, transaction, script, scan, and fan-out child coverage remain out of scope.
- Every commit must include DCO signoff and use conventional commit syntax.

---

### Task 1: Java sampling controller and configuration state

**Files:**
- Create: `java/client/src/main/java/glide/internal/RequestMetricsSampling.java`
- Create: `java/client/src/test/java/glide/internal/RequestMetricsSamplingTest.java`
- Modify: `java/client/src/main/java/glide/api/RequestMetrics.java`
- Modify: `java/client/src/test/java/glide/ffi/resolvers/RequestMetricsResolverTest.java`

**Interfaces:**
- Produces: `RequestMetricsSampling.shouldSample(): boolean`
- Produces: `RequestMetricsSampling.updateSamplePercentage(int): void`
- Produces for deterministic package tests: `RequestMetricsSampling.shouldSample(int): boolean`

- [ ] **Step 1: Write deterministic failing sampler tests**

```java
@Test
void percentageBoundariesUseOneDeterministicPercentile() {
    RequestMetricsSampling.updateSamplePercentage(0);
    assertFalse(RequestMetricsSampling.shouldSample(0));
    RequestMetricsSampling.updateSamplePercentage(1);
    assertTrue(RequestMetricsSampling.shouldSample(0));
    assertFalse(RequestMetricsSampling.shouldSample(1));
    RequestMetricsSampling.updateSamplePercentage(5);
    assertTrue(RequestMetricsSampling.shouldSample(4));
    assertFalse(RequestMetricsSampling.shouldSample(5));
    RequestMetricsSampling.updateSamplePercentage(100);
    assertTrue(RequestMetricsSampling.shouldSample(99));
}
```

- [ ] **Step 2: Run the sampler test and verify RED**

Run: `cd java && ./gradlew :client:test --tests glide.internal.RequestMetricsSamplingTest`

Expected: compilation fails because `RequestMetricsSampling` does not exist.

- [ ] **Step 3: Implement the minimal sampler**

```java
public final class RequestMetricsSampling {
    private static final int MAX_PERCENTAGE = 100;
    private static volatile int samplePercentage;

    private RequestMetricsSampling() {}

    public static boolean shouldSample() {
        int percentage = samplePercentage;
        return percentage == MAX_PERCENTAGE
                || (percentage > 0
                        && shouldSample(java.util.concurrent.ThreadLocalRandom.current().nextInt(100)));
    }

    static boolean shouldSample(int percentile) {
        return percentile < samplePercentage;
    }

    public static void updateSamplePercentage(int percentage) {
        samplePercentage = percentage;
    }

    static int getSamplePercentage() {
        return samplePercentage;
    }
}
```

- [ ] **Step 4: Serialize public configuration updates and mirror only successful native changes**

Make `RequestMetrics.configure` and `RequestMetrics.setSamplePercentage` `synchronized`. Call `throwForNonzeroStatus` first, then `RequestMetricsSampling.updateSamplePercentage(...)`.

- [ ] **Step 5: Extend the isolated native lifecycle test**

Assert the mirror remains 0 after the pre-configuration update failure, becomes 100 after successful configuration, becomes 0 after `setSamplePercentage(0)`, and returns to 100 after re-enabling.

- [ ] **Step 6: Run focused tests and verify GREEN**

Run: `cd java && ./gradlew :client:test --tests glide.internal.RequestMetricsSamplingTest --tests glide.ffi.resolvers.RequestMetricsResolverTest`

Expected: both classes pass.

- [ ] **Step 7: Commit**

```bash
git add java/client/src/main/java/glide/internal/RequestMetricsSampling.java \
  java/client/src/main/java/glide/api/RequestMetrics.java \
  java/client/src/test/java/glide/internal/RequestMetricsSamplingTest.java \
  java/client/src/test/java/glide/ffi/resolvers/RequestMetricsResolverTest.java
git commit -s -m "feat(java): select request metrics before registration"
```

### Task 2: Split selected and legacy Java completion paths

**Files:**
- Modify: `java/client/src/main/java/glide/internal/AsyncRegistry.java`
- Modify: `java/client/src/test/java/glide/internal/AsyncRegistryTest.java`

**Interfaces:**
- Consumes: one immutable `requestMetricsSampled` boolean.
- Produces: `AsyncRegistry.register(root, terminal, maxInflight, handle, timeout, requestMetricsSampled)`.
- Preserves: detailed `completeCallbackForNative` and `completeCallbackWithErrorCodeForNative`.
- Restores: legacy boolean `completeCallback` and `completeCallbackWithErrorCode` using `activeFutures` directly.

- [ ] **Step 1: Write failing registration-path tests**

```java
@Test
void unselectedRegistrationUsesOnlyLegacyFutureState() {
    CompletableFuture<Object> future = new CompletableFuture<>();
    long id = AsyncRegistry.register(future, future, 0, 17L, 0, false);
    assertEquals(0, AsyncRegistry.getCompletionStateCount());
    assertTrue(AsyncRegistry.completeCallback(id, "response"));
    assertEquals("response", future.join());
}

@Test
void selectedRegistrationRetainsDetailedTerminalState() {
    CompletableFuture<Object> root = new CompletableFuture<>();
    CompletableFuture<String> terminal = root.thenApply(Object::toString);
    long id = AsyncRegistry.register(root, terminal, 0, 17L, 0, true);
    assertEquals(1, AsyncRegistry.getCompletionStateCount());
    assertEquals(COMPLETED, AsyncRegistry.completeCallbackForNative(id, "response"));
    assertEquals(0, AsyncRegistry.getCompletionStateCount());
}
```

- [ ] **Step 2: Run the two tests and verify RED**

Run: `cd java && ./gradlew :client:test --tests 'glide.internal.AsyncRegistryTest.unselectedRegistrationUsesOnlyLegacyFutureState' --tests 'glide.internal.AsyncRegistryTest.selectedRegistrationRetainsDetailedTerminalState'`

Expected: compilation fails because the boolean registration overload does not exist.

- [ ] **Step 3: Add the registration flag and conditional state**

Default non-metrics overloads pass `false`. The terminal overload accepts the flag and inserts `CompletionState` only when true. Detailed registrations use the current timeout/cancellation helpers; legacy registrations use the `main` timeout helper and cleanup behavior.

- [ ] **Step 4: Restore legacy completion implementations**

`completeCallback` and `completeCallbackWithErrorCode` must fetch `activeFutures`, call `complete` or `completeExceptionally`, and return the boolean result without touching `completionStates`. Keep the integer methods unchanged for selected requests.

- [ ] **Step 5: Make shutdown and fatal sweeps cover both populations**

Process selected states first to publish detailed failure, then process remaining `activeFutures`; repeated completion is harmless. Clear both maps exactly once.

- [ ] **Step 6: Update existing detailed race tests**

Change test helpers for terminal failure, cancellation, and timeout races to pass `true`. Leave general registry tests on the default legacy overload.

- [ ] **Step 7: Run the full registry class and verify GREEN**

Run: `cd java && ./gradlew :client:test --tests glide.internal.AsyncRegistryTest`

Expected: all registry tests pass with no retained completion state.

- [ ] **Step 8: Commit**

```bash
git add java/client/src/main/java/glide/internal/AsyncRegistry.java \
  java/client/src/test/java/glide/internal/AsyncRegistryTest.java
git commit -s -m "refactor(java): gate detailed callback state by sampling"
```

### Task 3: Forward one decision through command dispatch and JNI callbacks

**Files:**
- Modify: `java/client/src/main/java/glide/managers/CommandManager.java`
- Modify: `java/client/src/main/java/glide/internal/GlideCoreClient.java`
- Modify: `java/client/src/main/java/glide/internal/GlideNativeBridge.java`
- Modify: `java/src/lib.rs`
- Modify: `java/src/jni_client.rs`
- Modify: `java/client/src/test/java/glide/api/RequestMetricsTest.java`
- Modify: `java/src/jni_client.rs` test module

**Interfaces:**
- Consumes: `RequestMetricsSampling.shouldSample()` exactly once per direct command.
- Produces JNI argument: `boolean requestMetricsSampled` appended to `executeCommandAsync`.
- Produces native callback-job field: `detailed_completion: bool`.
- Produces cached legacy and detailed Java callback method IDs.

- [ ] **Step 1: Write failing Java JNI-contract reflection test**

```java
Method execute = GlideNativeBridge.class.getDeclaredMethod(
        "executeCommandAsync", long.class, long.class, int.class, byte[][].class,
        boolean.class, int.class, String.class, boolean.class, long.class, boolean.class);
assertTrue(Modifier.isNative(execute.getModifiers()));
```

- [ ] **Step 2: Write failing Rust callback-path test**

Construct one `CallbackJob` with `detailed_completion=false` and one with `true`; assert the field survives enqueue/dequeue independently of `request_metrics` presence.

- [ ] **Step 3: Run Java and Rust focused tests and verify RED**

Run: `cd java && ./gradlew :client:test --tests glide.api.RequestMetricsTest`

Run: `cd java && cargo test callback_metrics_tests`

Expected: Java reflection cannot resolve the new signature; Rust cannot construct the new callback field.

- [ ] **Step 4: Branch command setup around the one selection**

In both normal and blocking `CommandManager` methods, select once. For false, use the legacy core future and attach the pipeline afterward. For true, pass the terminal pipeline before dispatch. Both calls forward the boolean.

- [ ] **Step 5: Extend core and JNI signatures**

Add explicit boolean overloads to `GlideCoreClient`; compatibility overloads select once and forward. Pass the flag to `AsyncRegistry.register` and append it to `GlideNativeBridge.executeCommandAsync` and the Rust JNI export.

- [ ] **Step 6: Cache and select legacy versus detailed callback methods**

Add cached IDs for `completeCallback(...): boolean` and `completeCallbackWithErrorCode(...): boolean`. Callback jobs retain the forwarded bit. Native workers invoke boolean methods when false and integer-outcome methods when true; convert a legacy `true` to `Completed` and `false` to `Failure` for shared finalization code.

- [ ] **Step 7: Match synchronous rejection to selection**

Extend `complete_error_sync` with `detailed_completion`. Direct inflight and argument-extraction rejections forward the request flag; unrelated callers pass false.

- [ ] **Step 8: Run Java and Rust focused tests and verify GREEN**

Run: `cd java && ./gradlew :client:test --tests glide.api.RequestMetricsTest --tests glide.internal.AsyncRegistryTest`

Run: `cd java && cargo test callback_metrics_tests`

Expected: all selected and legacy callback tests pass.

- [ ] **Step 9: Commit**

```bash
git add java/client/src/main/java/glide/managers/CommandManager.java \
  java/client/src/main/java/glide/internal/GlideCoreClient.java \
  java/client/src/main/java/glide/internal/GlideNativeBridge.java \
  java/client/src/test/java/glide/api/RequestMetricsTest.java \
  java/src/lib.rs java/src/jni_client.rs
git commit -s -m "feat(metrics): forward Java request sampling decision"
```

### Task 4: Add preselected native contexts and case-insensitive custom labels

**Files:**
- Modify: `glide-core/telemetry/src/request_metrics.rs`
- Modify: `glide-core/telemetry/tests/test_request_metrics.rs`
- Modify: `java/src/lib.rs`

**Interfaces:**
- Produces: `RequestMetricsState::start_pending_preselected()` with no random draw.
- Consumes: JNI `requestMetricsSampled` boolean.
- Preserves: `start_pending()` and `start_pending_with_sampler()` for native callers.

- [ ] **Step 1: Write failing Rust selection and command-case tests**

```rust
#[test]
fn preselected_context_ignores_percentage_without_drawing_again() {
    let state = RequestMetricsState::new(0, 4, &[]).unwrap();
    let context = state.start_pending_preselected().bind(BoundedOperation::known("PING"));
    assert!(context.finish(RequestMetricResult::Success));
    assert_eq!(state.drain(1).unwrap().samples().len(), 1);
}

#[test]
fn custom_operation_lookup_is_ascii_case_insensitive() {
    let state = RequestMetricsState::new(100, 4, &[b"GRAPH.GET_NODE"]).unwrap();
    for command in [b"GRAPH.GET_NODE".as_slice(), b"graph.get_node", b"Graph.Get_Node"] {
        let operation = state.custom_operation(command);
        assert_eq!(state.operation_bytes(operation), b"GRAPH.GET_NODE");
    }
}
```

- [ ] **Step 2: Run telemetry tests and verify RED**

Run: `cd glide-core/telemetry && cargo test --test test_request_metrics`

Expected: `start_pending_preselected` is missing and lowercase lookup resolves to `CUSTOM_COMMAND`.

- [ ] **Step 3: Implement preselected construction**

Factor pending-context construction into a private helper. `start_pending_preselected` calls it directly; sampled APIs retain their 0% and percentile checks before calling it.

- [ ] **Step 4: Implement allocation-free ASCII case-insensitive lookup**

Keep the configured allow-list uppercase and sorted. Compare each candidate iterator to `operation.iter().map(u8::to_ascii_uppercase)` in `binary_search_by`; do not allocate a normalized `Vec` or `String`.

- [ ] **Step 5: Use the preselected entry from JNI**

Change `start_request_metrics` to accept the JNI boolean, return no guard state when false, and call `start_pending_preselected` when true. Do not call `start_pending` on the Java direct-command path.

- [ ] **Step 6: Run telemetry, redis-rs, and Java Rust tests and verify GREEN**

Run: `cd glide-core/telemetry && cargo test --test test_request_metrics`

Run: `cd glide-core/redis-rs && cargo test --test test_request_metrics`

Run: `cd java && cargo test callback_metrics_tests`

Expected: all request-metrics tests pass.

- [ ] **Step 7: Commit**

```bash
git add glide-core/telemetry/src/request_metrics.rs \
  glide-core/telemetry/tests/test_request_metrics.rs java/src/lib.rs
git commit -s -m "fix(metrics): sample Java requests once"
```

### Task 5: Integration coverage, documentation, benchmark matrix, and lockfile

**Files:**
- Modify: `java/integTest/src/test/java/glide/RequestMetricsTests.java`
- Modify: `docs/request-metrics.md`
- Modify: `java/benchmarks/src/jmh/java/glide/benchmarks/RequestMetricsBenchmark.java`
- Modify: `glide-core/Cargo.lock`

**Interfaces:**
- Documents: Java-owned one-time selection forwarded to native execution.
- Benchmarks: never configured, configured 0%, sampled 1%, sampled 5%, sampled 100%.

- [ ] **Step 1: Add failing/behavioral integration assertions**

Send a lowercase custom `ping` while `PING` is allow-listed and assert its sample operation is `PING`. Temporarily set sampling to 0, execute a direct PING, assert no samples for a bounded interval, and restore 100 in `finally`.

- [ ] **Step 2: Update the JMH matrix**

Change `SampledCommandState.samplePercentage` from `{"1", "10", "100"}` to `{"1", "5", "100"}`. Retain never-configured and configured-zero benchmark methods and summary counts.

- [ ] **Step 3: Update user documentation**

Replace the native random-decision description with the Java binding's one-time pre-registration decision and state explicitly that unselected commands use the legacy Java callback path.

- [ ] **Step 4: Regenerate the root lockfile**

Run: `cd glide-core && cargo check`

Expected: `Cargo.lock` records the telemetry dependency closure, including `rand 0.9`, and compilation succeeds.

- [ ] **Step 5: Verify locked reproducibility**

Run: `cd glide-core && cargo check --locked`

Expected: success with no lockfile mutation.

- [ ] **Step 6: Run the integration class when the local Java toolchain permits**

Run: `cd java && ./gradlew :integTest:test --tests glide.RequestMetricsTests`

Expected: success. If local `protoc` remains incompatible with the pinned protobuf runtime, record the exact environmental error and rely on the repository pipeline for this Java gate; do not classify it as a product regression.

- [ ] **Step 7: Commit**

```bash
git add java/integTest/src/test/java/glide/RequestMetricsTests.java \
  docs/request-metrics.md \
  java/benchmarks/src/jmh/java/glide/benchmarks/RequestMetricsBenchmark.java \
  glide-core/Cargo.lock
git commit -s -m "test(metrics): verify proportional Java sampling"
```

### Task 6: Final verification and performance evidence

**Files:**
- No production files unless verification exposes a tested defect.

**Interfaces:**
- Produces: clean diff, focused test evidence, locked-build evidence, and benchmark comparison notes.

- [ ] **Step 1: Format and inspect**

Run: `cargo fmt --manifest-path glide-core/Cargo.toml --all -- --check`

Run: `cargo fmt --manifest-path java/Cargo.toml --all -- --check`

Run: `cd java && ./gradlew :spotlessCheck`

Run: `git diff --check origin/main...HEAD`

Expected: all commands succeed.

- [ ] **Step 2: Run complete focused Rust verification**

Run: `cd glide-core/telemetry && cargo test`

Run: `cd glide-core/redis-rs && cargo test --test test_request_metrics`

Run: `cd java && cargo test callback_metrics_tests request_metrics_tests`

Expected: all tests pass.

- [ ] **Step 3: Run Java unit verification**

Run: `cd java && ./gradlew :client:test --tests glide.api.RequestMetricsTest --tests glide.ffi.resolvers.RequestMetricsResolverTest --tests glide.internal.RequestMetricsSamplingTest --tests glide.internal.AsyncRegistryTest`

Expected: all tests pass, subject only to the documented local `protoc` compatibility blocker.

- [ ] **Step 4: Run JMH at 0/1/5/100 and capture summaries**

Use the request-metrics JMH include filter against one local Valkey endpoint with identical JDK and worker settings for every fork. Capture throughput, p50/p95/p99, operations, drained samples, and dropped samples.

- [ ] **Step 5: Compare against `origin/main`**

Run the equivalent PING JMH workload from an untouched `origin/main` worktree with the same server and settings. Treat differences within normal repeated-run noise as equivalent; call out any stable disabled/0% regression before completion.

- [ ] **Step 6: Review final repository state**

Run: `git status --short`

Run: `git log --format='%h %s%n%(trailers:key=Signed-off-by,valueonly)' origin/main..HEAD`

Expected: no uncommitted files and every new commit has a `Signed-off-by` trailer.

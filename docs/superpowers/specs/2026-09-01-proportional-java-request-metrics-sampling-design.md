# Proportional Java Request-Metrics Sampling Design

## Goal

Make Java request-phase telemetry cost proportional to its configured sampling percentage. With the intended production setting of 1–5%, the 95–99% of unselected direct commands must retain the pre-telemetry Java completion path, while a selected command must keep one metrics context through JNI ingress, native execution, response conversion, and synchronous Java response handling.

This change also closes the two remaining independent PR 24 review findings: custom-command allow-list matching must respect Valkey's case-insensitive command syntax, and the root `glide-core/Cargo.lock` must include the telemetry crate's `rand` dependency.

## Non-goals

- This design does not add request metrics to batches, transactions, scripts, scans, or fan-out child commands.
- It does not change the public sampling percentage from an integer in the inclusive range 0–100.
- It does not export metrics directly to an observability backend.
- It does not change sampling for OpenTelemetry traces.
- It does not remove the native sampler APIs used by Rust tests or potential future bindings.

## Approaches considered

### Chosen: make the Java binding's decision before registration

The Java binding chooses a direct command once, passes the resulting boolean to both `AsyncRegistry` and JNI, and Rust trusts that decision for this request. This lets Java avoid terminal-outcome state for unselected commands and guarantees that Java and Rust cannot independently disagree about selection.

### Rejected: gate Java state only when the percentage is positive

This restores the fast path at 0%, but at a 1% configuration every request still allocates `CompletionState` and mutates a second `ConcurrentHashMap`. It makes the dominant cost independent of the configured sampling rate and therefore does not meet the production use case.

### Rejected: ask native code for a decision before Java registration

A separate JNI preflight would retain native ownership of sampling but add another Java-to-native transition to every command, including metrics-disabled commands. Moving all future registration into JNI would avoid that extra call but would substantially redesign the existing callback ownership and immediate-rejection paths.

## Architecture

### Java sampling controller

A focused internal Java sampling controller owns the Java binding's current percentage. It starts at 0, uses `ThreadLocalRandom` only for percentages 1–99, always rejects at 0, and always selects at 100. A package-visible deterministic percentile helper allows boundary tests without probabilistic assertions.

`RequestMetrics.configure` and `RequestMetrics.setSamplePercentage` update this controller only after native configuration returns `STATUS_OK`. The two configuration methods serialize their native call and Java-state update so concurrent reconfiguration cannot leave Java and native configuration in different orders. A command racing with configuration uses either the previous or new Java percentage; its boolean remains authoritative for that command after selection.

The Java configuration mirror is binding-local. Native `RequestMetricsState` remains process-global and retains its percentage for native callers, drains, and existing tests. The Java direct-command path uses a new native entry that creates a pending context from an already-made selection instead of consulting the native random sampler again.

### One request decision

`CommandManager` makes exactly one request-metrics decision before registering a direct command:

- If unselected, it invokes the core command method without a terminal pipeline, then attaches response conversion and response handling exactly as `main` does.
- If selected, it supplies the terminal pipeline before native dispatch so callback completion can classify synchronous GLIDE response-conversion or handler failure.

Both paths pass the same boolean to `GlideCoreClient`, `AsyncRegistry`, and `GlideNativeBridge.executeCommandAsync`. Compatibility overloads that are not called by `CommandManager` may obtain one decision internally, but they must forward it rather than resample.

JNI receives `requestMetricsSampled` with the existing command arguments. `start_request_metrics(requestMetricsSampled)` returns immediately when false. When true, it obtains the configured process-global native state and creates a pending context without another random draw. That context continues through all existing phases and retries.

### Proportional Java completion tracking

`AsyncRegistry.register` accepts whether detailed terminal tracking is required:

- An unselected request is inserted only into the existing `activeFutures` map and uses the legacy completion, timeout, cancellation, failure-sweep, and shutdown behavior from `main`.
- A selected request additionally creates `CompletionState`, retains the terminal command future, and uses the existing exact timeout/cancellation/handler-failure race protocol.

The native callback job carries both the optional request metrics context and the forwarded selection bit. Callback workers call the legacy Java boolean completion methods when the bit is false and the detailed integer-outcome methods when it is true. Keeping that bit separate from context presence also lets an unexpectedly missing native state complete and clean up a Java-selected request correctly. The JNI method cache contains both method pairs. Synchronous command rejection receives the request selection explicitly so it calls the matching completion method before the native metrics guard records failure.

Selection is immutable for an in-flight request. Changing the global percentage affects only commands that have not yet selected. Disabling metrics never discards a selected request's `CompletionState`; normal terminal cleanup removes it.

### Case-insensitive custom-command lookup

Configured custom-command labels remain uppercase and bounded. For a sampled custom command, native lookup compares the command token to the sorted uppercase allow-list using ASCII case-insensitive ordering. It does not uppercase arguments, responses, or labels and does not allocate a normalized command string. Thus configured `GRAPH.GET_NODE` matches sent `GRAPH.GET_NODE`, `graph.get_node`, and mixed ASCII case, while an unlisted command remains `CUSTOM_COMMAND`.

### Lockfile integrity

Regenerate and commit `glide-core/Cargo.lock` after the telemetry crate's `rand = "0.9"` dependency is present. `cargo check --locked` from `glide-core` must succeed without modifying tracked files.

## Error and race behavior

- A failed native configuration call does not change Java's active percentage.
- A request selected before a later 0% update remains selected and reports its terminal sample once.
- A request unselected before a later positive update remains unselected and never acquires metrics state.
- A selected synchronous JNI rejection uses detailed completion and is finalized as failure by the native guard.
- A selected cancellation or timeout retains the current exactly-once classification and native bookkeeping release behavior.
- An unselected timeout, cancellation, callback, shutdown, or fatal sweep follows `main`; telemetry adds no second map entry or monitor.
- If Java reports a selected request while native state is unexpectedly absent, command execution must still complete normally. The inconsistency is treated as no native sample rather than a command failure.

## Tests

### Java unit tests

- Deterministically verify 0%, 1%, 5%, and 100% percentile boundaries.
- Verify successful configuration changes the Java percentage and failed configuration does not. Configuration tests must avoid random assertions.
- Verify unselected registration creates no `CompletionState` and legacy success/error completion still resolves the future.
- Verify selected registration creates one `CompletionState` and preserves handler failure, cancellation, timeout, shutdown, and cleanup outcomes.
- Verify changing the configured percentage does not change the selected flag already passed to an in-flight registration.
- Verify the JNI direct-command signature includes exactly one request-metrics selection parameter.

### Rust tests

- Verify an explicitly unselected request constructs no pending context even at a nonzero native percentage.
- Verify an explicitly selected request constructs a context without invoking the random sampler and emits one sample.
- Verify lowercase and mixed-case custom command tokens resolve to the configured uppercase label.
- Retain all current phase, retry, callback, timeout, cancellation, and drain tests.

### Integration tests

- Retain 100% end-to-end success, failure, callback-phase, and cancellation coverage.
- Add a 0% direct-command assertion that no sample is emitted and selected completion state does not accumulate.

## Performance verification

The JMH request-metrics command matrix covers never configured, configured 0%, 1%, 5%, and 100%. Each sampled run reports operations, drained samples, dropped samples, drain calls, and maximum batch size. The 1% and 5% runs must produce approximately proportional samples over a sufficiently large operation count; they must not be judged by an exact random count.

Run equivalent PING workloads against both `origin/main` and the PR branch with the same JDK, server, warmup, measurement, forks, and callback/runtime worker settings. Report throughput plus p50/p95/p99 sample time. Acceptance criteria are:

- Never configured and configured 0% show no material regression from `origin/main` beyond normal run-to-run noise.
- At 1% and 5%, latency and throughput remain close to the disabled branch result and materially closer to it than the 100% result.
- Completion-state tests prove allocation count is proportional by construction; benchmark results validate end-to-end impact rather than replacing that invariant.

## Documentation

Update `docs/request-metrics.md` to say that the Java binding decides once before registration and forwards the decision into native execution. Preserve the documented phase boundaries, bounded labels, process-global configuration, and exactly-once terminal semantics. Explicitly state that unselected Java direct commands retain the legacy completion path and create neither Java terminal metrics state nor native lifecycle state.

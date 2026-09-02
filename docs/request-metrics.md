# Java request-phase metrics

The Java client can sample the end-to-end lifecycle of direct commands and expose bounded batches of phase durations to an application-owned consumer. Collection is process-global and opt-in. It does not send metrics to a network service or start a consumer thread.

## Configure sampling

Call `RequestMetrics.configure` once before the command workload. The sampling percentage is an integer from 0 through 100. The initial buffer capacity and custom-command allow-list become immutable after the first successful configuration; later calls may only use the same fixed values. `RequestMetrics.setSamplePercentage` can change the percentage without replacing the buffer.

The native client makes one sampling decision when a direct command enters JNI, before parsing its operation. An unselected command does not create request lifecycle state, phase timers, or an operation label. A selected command keeps the same context across routing, retries, response decoding, and Java callback completion; retries do not make another sampling decision. At 0%, selection returns before consulting the random sampler or allocating request-metrics context state.

Sampled producers use a bounded native buffer and a nonblocking offer. When the buffer is full, command processing continues normally and the completed sample is counted as dropped. A later drain reports the accumulated drop count. Buffer capacity must be from 1 through 1,000,000 samples.

Custom-command labels are bounded as well. Known client commands use their fixed command name. An arbitrary custom command is reported as `CUSTOM_COMMAND` unless its name is present in the configured allow-list. The allow-list contains at most 64 names; each name is 1 through 64 characters, begins with an uppercase ASCII letter, and then uses only uppercase ASCII letters, digits, `_`, `.`, or `-`. Command arguments and responses are never used as labels.

```java
import glide.api.GlideClient;
import glide.api.RequestMetrics;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.metrics.RequestMetricBatch;
import glide.api.models.metrics.RequestMetricsConfiguration;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

RequestMetrics.configure(
        RequestMetricsConfiguration.builder()
                .samplePercentage(10)
                .bufferCapacity(16_384)
                .allowedCustomCommands(Collections.singleton("GRAPH.QUERY"))
                .build());

ScheduledExecutorService metricsConsumer =
        Executors.newSingleThreadScheduledExecutor();
metricsConsumer.scheduleWithFixedDelay(
        () -> {
            RequestMetricBatch batch = RequestMetrics.drain(2_000);
            publishToYourMetricsSystem(batch.getSamples());
            recordDroppedSamples(batch.getDroppedSamples());
        },
        0,
        100,
        TimeUnit.MILLISECONDS);

GlideClientConfiguration configuration =
        GlideClientConfiguration.builder()
                .address(NodeAddress.builder().host("127.0.0.1").port(6379).build())
                .build();

try (GlideClient client = GlideClient.createClient(configuration).get()) {
    String value = client.get("key").get();
} finally {
    metricsConsumer.shutdown();
}
```

`RequestMetrics.drain(maxSamples)` synchronously transfers and decodes at most `maxSamples` buffered samples; `maxSamples` must be from 1 through 10,000. It performs no network I/O, but its locking, native-to-Java serialization, allocation, and decoding cost is paid by the calling thread. Run drains on an application-owned background thread, not on a command-processing or latency-sensitive thread. Continue draining while `getHasMore()` is true when the consumer must catch up. The reported remaining count and `hasMore` value are concurrent snapshots.

## Phase boundaries

Only phases reached by a sampled request are present. Durations for repeated attempts or waits are accumulated under the same phase.

| Phase | Measured boundary |
| --- | --- |
| `JNI_INGRESS` | From the selected native direct-command entry timestamp through request-byte copying, parsing and validation, and operation binding. It ends before post-bind dispatch bookkeeping, `CLIENT_QUEUE`, and the runtime spawn. |
| `CLIENT_QUEUE` | From immediately before spawning the command future until the first action in that future. |
| `COMMAND_PREPARE` | Accumulated across two synchronous intervals: converting the parsed direct command and route into their core representations, and calling `client.send_command` to create the command future. The second interval ends when that call returns the future; awaiting the future and all network work are excluded. |
| `CONNECTION_WAIT` | Time inside the single-node cluster `get_connection` boundary: route/address selection and connection-future or reconnect readiness waits. For an `ASK` redirect, this boundary also includes the `ASKING` command round trip. It is omitted for standalone commands and multi-node fan-out, where there is no comparable honest boundary; general slot-refresh work is not labeled as connection wait. |
| `PIPELINE_QUEUE` | From immediately before the asynchronous `mpsc` send begins, including permit/backpressure wait, until entry to `PipelineSink::start_send`. It ends before output-closed, stored-error, and protocol-desynchronization rejection checks, so it can be present with zero attempts. If the handoff never reaches `start_send`, terminal finalization closes the active interval. |
| `SOCKET_WRITE` | From immediately before an actual underlying sink `start_send` call until the first covering successful flush or response, or until a synchronous start-send/flush error, cancellation/finalization, or sink drop closes it. Each attempt's timer ends exactly once, including when a response arrives before its flush. |
| `RESPONSE_WAIT` | From the same underlying `start_send` boundary until the complete logical response is assembled, including every aggregate or fenced response, or until terminal error, cancellation/finalization, or sink drop. It intentionally overlaps `SOCKET_WRITE`. |
| `RETRY_BACKOFF` | Time only inside existing explicit command retry sleeps, including ordinary retry, slot-refresh retry-delay, and busy-loading delay paths. Immediate redirects and routing, refresh, or reconnect work outside an explicit retry sleep are excluded. |
| `CORE_DECODE` | Conversion of the core response into the expected core return value, including configured response decompression. A raw command error passes through the same timed wrapper; Java object conversion is excluded. |
| `CALLBACK_QUEUE` | From enqueueing the callback job until a Java callback worker receives that exact job. |
| `CALLBACK_COMPLETE` | From callback-worker receipt, after `CALLBACK_QUEUE` ends, through native-to-Java conversion and Java future completion until synchronous Java completion actions return to native code. |
| `TOTAL` | From the selected native-entry timestamp through the terminal result/finalization timestamp, which also closes any active phases. Sample construction, serialization, and the nonblocking buffer offer happen afterward and are excluded. |

Because completing a Java future can wake its waiting thread before the native callback call returns, a sample may become visible to `drain` just after the corresponding future appears complete. Consumers should drain periodically instead of assuming that an immediately following drain must contain that command.

Multi-node fan-out currently retains only the outer request lifecycle phases. Fan-out child commands deliberately do not inherit the metrics context, so their `PIPELINE_QUEUE`, `SOCKET_WRITE`, `RESPONSE_WAIT`, and attempts are not attributed to the outer sample. Missing network phases on a fan-out request therefore mean "not measured," not zero network latency.

`attemptCount` is incremented immediately before each actual underlying sink `start_send` call, after queue-time rejection checks. It includes retries and a call whose synchronous `start_send` returns an error. Selection/readiness failure, channel-handoff failure, closed output, or stored error/protocol state rejected before that call does not count as an attempt. The terminal result is recorded once:

- `SUCCESS` means the command result and Java callback completion both succeeded.
- `FAILURE` includes command, decode, callback conversion, delivery, and other non-timeout failures.
- `TIMEOUT` means timeout processing won the terminal race.
- `CANCELLED` means Java cancellation won the terminal race.

Timeout and cancellation can overlap native response or callback work. Whichever terminal path wins finalizes the request once; late work cannot reclassify it or add phases. Active phase timers are closed at finalization so a terminal sample remains internally consistent.

Cancelling the Java command future releases its Java completion state and native in-flight metrics bookkeeping, then finalizes a sampled request as `CANCELLED`. This bookkeeping operation does not abort the spawned Rust future, retract bytes already written to the socket, or cancel work already running on the server. The underlying command may therefore finish in the background, but its late callback cannot emit a second sample or change the recorded result.

## Current scope

Sampling currently covers one direct Java command per request. Batch and transaction execution, scripts, scan lifecycles, and child network work in multi-node fan-out are outside this initial scope. Custom commands sent through the direct-command path are sampled, subject to the bounded labeling rules above. The schema and consumer API are designed so these other request shapes can be added later without making command producers perform network I/O.

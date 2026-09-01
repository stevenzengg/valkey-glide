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
| `JNI_INGRESS` | From entry into the direct-command JNI function, through byte copying, request parsing and validation, operation binding, and preparation for the asynchronous runtime handoff. |
| `CLIENT_QUEUE` | From immediately before spawning the command future until the first action in that future. |
| `COMMAND_PREPARE` | Conversion of the parsed single-command request into the core command and routing representation. |
| `CONNECTION_WAIT` | Waiting to acquire or refresh a connection when the selected command path requires it. |
| `PIPELINE_QUEUE` | From submission to an asynchronous connection pipeline until that pipeline accepts the message for an attempt. |
| `SOCKET_WRITE` | From acceptance of a network attempt until its bytes are flushed. |
| `RESPONSE_WAIT` | From the same accepted-attempt point until the response or terminal attempt outcome. It intentionally overlaps `SOCKET_WRITE`. |
| `RETRY_BACKOFF` | Time spent sleeping before another connection or command attempt. |
| `CORE_DECODE` | Conversion of the raw protocol result into the core value returned to the Java bridge. |
| `CALLBACK_QUEUE` | From enqueueing the callback job until a Java callback worker receives that exact job. |
| `CALLBACK_COMPLETE` | From callback-worker receipt, after `CALLBACK_QUEUE` ends, through native-to-Java conversion and Java future completion until synchronous Java completion actions return to native code. |
| `TOTAL` | From the one sampling decision at JNI entry until the terminal result is fixed and the compact sample is offered to the native buffer. |

Because completing a Java future can wake its waiting thread before the native callback call returns, a sample may become visible to `drain` just after the corresponding future appears complete. Consumers should drain periodically instead of assuming that an immediately following drain must contain that command.

`attemptCount` is incremented when a network send attempt is accepted. It includes retries and does not count work that fails before an attempt is accepted. The terminal result is recorded once:

- `SUCCESS` means the command result and Java callback completion both succeeded.
- `FAILURE` includes command, decode, callback conversion, delivery, and other non-timeout failures.
- `TIMEOUT` means timeout processing won the terminal race.
- `CANCELLED` means Java cancellation won the terminal race.

Timeout and cancellation can overlap native response or callback work. Whichever terminal path wins finalizes the request once; late work cannot reclassify it or add phases. Active phase timers are closed at finalization so a terminal sample remains internally consistent.

## Current scope

Sampling currently covers one direct Java command per request. Batch and transaction execution, scripts, and scan lifecycles are outside this initial scope. Custom commands sent through the direct-command path are sampled, subject to the bounded labeling rules above. The schema and consumer API are designed so these other request shapes can be added later without making command producers perform network I/O.

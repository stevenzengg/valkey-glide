/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.benchmarks;

import glide.api.GlideClient;
import glide.api.RequestMetrics;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.metrics.RequestMetricBatch;
import glide.api.models.metrics.RequestMetricsConfiguration;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/** JMH coverage for native request-phase sampling on one direct Java command. */
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(2)
@Threads(1)
public class RequestMetricsBenchmark {
    private static final int BUFFER_CAPACITY = 4_096;
    private static final int MAX_DRAIN_SAMPLES = 10_000;
    private static final int MEASURED_DRAIN_SAMPLES = 2_000;
    private static final int DRAIN_REFILL_COMMANDS = BUFFER_CAPACITY;
    private static final String EXPECTED_PING_RESPONSE = "PONG";

    /** Never references the request-metrics API, preserving an uninitialized native OnceLock. */
    @State(Scope.Benchmark)
    public static class NeverConfiguredState extends ClientState {}

    /** Configures the native state at zero percent in a fork separate from the baseline. */
    @State(Scope.Benchmark)
    public static class ConfiguredZeroPercentState extends ClientState {
        @Setup(Level.Trial)
        public void configureMetrics() {
            configureRequestMetrics(0);
            drainAll(null);
        }

        @TearDown(Level.Trial)
        public void reportMetrics() {
            BatchTotals totals = new BatchTotals();
            drainAll(totals);
            printSummary("configuredZeroPercentPing", 0, "none", operations, totals);
        }
    }

    /** Parameterized command workload for sampled configurations and consumer behavior. */
    @State(Scope.Benchmark)
    public static class SampledCommandState extends ClientState {
        @Param({"1", "10", "100"})
        public int samplePercentage;

        @Param({"scheduled-drain", "fill-without-drain", "drain-2000"})
        public String consumer;

        private final BatchTotals totals = new BatchTotals();
        private final AtomicReference<Throwable> consumerFailure = new AtomicReference<>();
        private ScheduledExecutorService consumerExecutor;

        @Setup(Level.Trial)
        public void configureMetrics() {
            configureRequestMetrics(samplePercentage);
            drainAll(totals);
            ConsumerMode mode = ConsumerMode.fromParameter(consumer);
            if (mode != ConsumerMode.FILL_WITHOUT_DRAIN) {
                int batchSize =
                        mode == ConsumerMode.DRAIN_2000 ? MEASURED_DRAIN_SAMPLES : MAX_DRAIN_SAMPLES;
                consumerExecutor =
                        java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                                runnable -> {
                                    Thread thread = new Thread(runnable, "request-metrics-benchmark-drain");
                                    thread.setDaemon(true);
                                    return thread;
                                });
                consumerExecutor.scheduleWithFixedDelay(
                        () -> drainFromConsumer(batchSize), 0, 1, TimeUnit.MILLISECONDS);
            }
        }

        @TearDown(Level.Trial)
        public void reportMetrics() throws InterruptedException {
            if (consumerExecutor != null) {
                consumerExecutor.shutdownNow();
                consumerExecutor.awaitTermination(5, TimeUnit.SECONDS);
            }
            drainAll(totals);
            Throwable failure = consumerFailure.get();
            if (failure != null) {
                throw new IllegalStateException("Request-metrics consumer failed", failure);
            }
            printSummary("sampledPing", samplePercentage, consumer, operations, totals);
        }

        private void drainFromConsumer(int maxSamples) {
            try {
                totals.add(RequestMetrics.drain(maxSamples));
            } catch (Throwable failure) {
                consumerFailure.compareAndSet(null, failure);
            }
        }
    }

    /** Refills outside the timed region so the measured public drain always returns 2,000 samples. */
    @State(Scope.Benchmark)
    public static class DrainTwoThousandState extends ClientState {
        private final BatchTotals totals = new BatchTotals();

        @Setup(Level.Trial)
        public void configureMetrics() {
            configureRequestMetrics(100);
            drainAll(totals);
        }

        @Setup(Level.Invocation)
        public void refill() {
            drainAll(totals);
            for (int index = 0; index < DRAIN_REFILL_COMMANDS; index++) {
                pingAndValidate(client);
            }
        }

        @TearDown(Level.Invocation)
        public void clearRemainder() {
            drainAll(totals);
        }

        @TearDown(Level.Trial)
        public void reportMetrics() {
            drainAll(totals);
            printSummary("drainTwoThousand", 100, "drain-2000", operations, totals);
        }
    }

    /** Common standalone client lifecycle inherited by each fork-owned state. */
    @State(Scope.Benchmark)
    public static class ClientState {
        @Param({"127.0.0.1"})
        public String host;

        @Param({"6379"})
        public int port;

        protected GlideClient client;
        protected long operations;

        @Setup(Level.Trial)
        public void connect() throws Exception {
            GlideClientConfiguration configuration =
                    GlideClientConfiguration.builder()
                            .address(NodeAddress.builder().host(host).port(port).build())
                            .build();
            client = GlideClient.createClient(configuration).get(10, TimeUnit.SECONDS);
            pingAndValidate(client);
        }

        @TearDown(Level.Trial)
        public void close() throws Exception {
            client.close();
        }
    }

    private enum ConsumerMode {
        SCHEDULED_DRAIN,
        FILL_WITHOUT_DRAIN,
        DRAIN_2000;

        private static ConsumerMode fromParameter(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT).replace('-', '_'));
        }
    }

    private static final class BatchTotals {
        private final AtomicLong drainedSamples = new AtomicLong();
        private final AtomicLong droppedSamples = new AtomicLong();
        private final AtomicLong drainCalls = new AtomicLong();
        private final AtomicLong maxBatchSamples = new AtomicLong();

        private void add(RequestMetricBatch batch) {
            long sampleCount = batch.getSamples().size();
            drainedSamples.addAndGet(sampleCount);
            droppedSamples.addAndGet(batch.getDroppedSamples());
            drainCalls.incrementAndGet();
            maxBatchSamples.accumulateAndGet(sampleCount, Math::max);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public String neverConfiguredBaseline(NeverConfiguredState state) {
        state.operations++;
        return pingAndValidate(state.client);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public String configuredZeroPercentPing(ConfiguredZeroPercentState state) {
        state.operations++;
        return pingAndValidate(state.client);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public String sampledPing(SampledCommandState state) {
        state.operations++;
        return pingAndValidate(state.client);
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public RequestMetricBatch drainTwoThousand(DrainTwoThousandState state) {
        RequestMetricBatch batch = RequestMetrics.drain(MEASURED_DRAIN_SAMPLES);
        if (batch.getSamples().size() != MEASURED_DRAIN_SAMPLES) {
            throw new IllegalStateException(
                    "Expected exactly "
                            + MEASURED_DRAIN_SAMPLES
                            + " samples, got "
                            + batch.getSamples().size());
        }
        state.operations++;
        state.totals.add(batch);
        return batch;
    }

    private static String pingAndValidate(GlideClient client) {
        String response = client.ping().join();
        if (!EXPECTED_PING_RESPONSE.equals(response)) {
            throw new IllegalStateException("PING result changed: " + response);
        }
        return response;
    }

    private static void configureRequestMetrics(int samplePercentage) {
        RequestMetrics.configure(
                RequestMetricsConfiguration.builder()
                        .samplePercentage(samplePercentage)
                        .bufferCapacity(BUFFER_CAPACITY)
                        .allowedCustomCommands(Set.of())
                        .build());
    }

    private static void drainAll(BatchTotals totals) {
        RequestMetricBatch batch;
        do {
            batch = RequestMetrics.drain(MAX_DRAIN_SAMPLES);
            if (totals != null) {
                totals.add(batch);
            }
        } while (batch.getHasMore());
    }

    private static void printSummary(
            String benchmark,
            int samplePercentage,
            String consumer,
            long operations,
            BatchTotals totals) {
        System.out.printf(
                Locale.ROOT,
                "REQUEST_METRICS_SUMMARY benchmark=%s samplePercentage=%d consumer=%s "
                        + "operations=%d drainedSamples=%d droppedSamples=%d drainCalls=%d "
                        + "maxBatchSamples=%d%n",
                benchmark,
                samplePercentage,
                consumer,
                operations,
                totals.drainedSamples.get(),
                totals.droppedSamples.get(),
                totals.drainCalls.get(),
                totals.maxBatchSamples.get());
    }
}

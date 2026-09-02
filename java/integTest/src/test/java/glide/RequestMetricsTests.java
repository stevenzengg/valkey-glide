/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide;

import static glide.TestUtilities.commonClientConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import glide.api.GlideClient;
import glide.api.RequestMetrics;
import glide.api.models.metrics.RequestMetricBatch;
import glide.api.models.metrics.RequestMetricPhase;
import glide.api.models.metrics.RequestMetricPhaseDuration;
import glide.api.models.metrics.RequestMetricResult;
import glide.api.models.metrics.RequestMetricSample;
import glide.api.models.metrics.RequestMetricsConfiguration;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
public class RequestMetricsTests {
    private static final String SUCCESS_RESPONSE = "request-metrics-secret-response";
    private static final String GRAPH_KEY = "request-metrics-secret-key";

    @BeforeAll
    static void configureRequestMetrics() {
        // TestConfiguration probes INFO during static initialization. Complete that unsampled
        // fixture setup before enabling process-global request metrics for this test class.
        TestConfiguration.SERVER_VERSION.toString();
        RequestMetrics.configure(
                RequestMetricsConfiguration.builder()
                        .samplePercentage(100)
                        .bufferCapacity(32)
                        .allowedCustomCommands(new HashSet<>(Arrays.asList("BLPOP", "GRAPH.QUERY", "PING")))
                        .build());
        drainAll();
    }

    @AfterAll
    static void disableRequestMetrics() {
        RequestMetrics.setSamplePercentage(0);
        drainAll();
    }

    @Test
    @SneakyThrows
    public void direct_callbacks_emit_success_and_failure_with_terminal_callback_phases() {
        try (GlideClient client = GlideClient.createClient(commonClientConfig().build()).get()) {
            assertEquals(
                    SUCCESS_RESPONSE, client.customCommand(new String[] {"PING", SUCCESS_RESPONSE}).get());

            assertThrows(
                    ExecutionException.class,
                    () ->
                            client
                                    .customCommand(new String[] {"GRAPH.QUERY", GRAPH_KEY, "MATCH (n) RETURN n"})
                                    .get());
        }

        List<RequestMetricSample> samples = drainUntilSampleCount(2);
        assertEquals(2, samples.size());

        Map<String, RequestMetricSample> samplesByOperation =
                samples.stream()
                        .collect(Collectors.toMap(RequestMetricSample::getOperation, Function.identity()));
        assertEquals(RequestMetricResult.SUCCESS, samplesByOperation.get("PING").getResult());
        assertEquals(RequestMetricResult.FAILURE, samplesByOperation.get("GRAPH.QUERY").getResult());

        for (RequestMetricSample sample : samples) {
            Map<RequestMetricPhase, Long> phases =
                    sample.getPhaseDurations().stream()
                            .collect(
                                    Collectors.toMap(
                                            RequestMetricPhaseDuration::getPhase,
                                            RequestMetricPhaseDuration::getDurationNanos));
            sample
                    .getPhaseDurations()
                    .forEach(
                            phase ->
                                    assertTrue(
                                            phase.getDurationNanos() > 0,
                                            () -> "non-positive populated phase: " + phase));

            Long callbackQueue = phases.get(RequestMetricPhase.CALLBACK_QUEUE);
            Long callbackComplete = phases.get(RequestMetricPhase.CALLBACK_COMPLETE);
            Long total = phases.get(RequestMetricPhase.TOTAL);
            assertNotNull(callbackQueue);
            assertNotNull(callbackComplete);
            assertNotNull(total);
            assertTrue(total >= callbackQueue);
            assertTrue(total >= callbackComplete);

            assertFalse(sample.toString().contains(GRAPH_KEY));
            assertFalse(sample.toString().contains(SUCCESS_RESPONSE));
            assertFalse(
                    Arrays.stream(RequestMetricSample.class.getDeclaredFields())
                            .map(Field::getName)
                            .anyMatch(name -> name.toLowerCase().contains("callback")));
        }

        RequestMetricBatch secondDrain = RequestMetrics.drain(32);
        assertTrue(secondDrain.getSamples().isEmpty());
        assertFalse(secondDrain.getHasMore());
    }

    @Test
    @SneakyThrows
    public void cancellation_emits_once_while_the_server_command_finishes_in_the_background() {
        drainAll();
        String blockingKey = "request-metrics-cancelled-blpop-" + System.nanoTime();

        try (GlideClient client = GlideClient.createClient(commonClientConfig().build()).get()) {
            CompletableFuture<Object> command =
                    client.customCommand(new String[] {"BLPOP", blockingKey, "0.1"});

            assertTrue(command.cancel(false));

            List<RequestMetricSample> samples = drainUntilSampleCount(1);
            assertEquals(1, samples.size());
            assertEquals("BLPOP", samples.get(0).getOperation());
            assertEquals(RequestMetricResult.CANCELLED, samples.get(0).getResult());

            assertNoSamplesFor(500, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    @SneakyThrows
    public void lowercase_custom_command_uses_allow_list_label_and_zero_percent_emits_nothing() {
        drainAll();
        try (GlideClient client = GlideClient.createClient(commonClientConfig().build()).get()) {
            assertEquals("PONG", client.customCommand(new String[] {"ping"}).get());

            List<RequestMetricSample> samples = drainUntilSampleCount(1);
            assertEquals(1, samples.size());
            assertEquals("PING", samples.get(0).getOperation());

            RequestMetrics.setSamplePercentage(0);
            try {
                assertEquals("PONG", client.customCommand(new String[] {"ping"}).get());
                assertNoSamplesFor(250, TimeUnit.MILLISECONDS);
            } finally {
                RequestMetrics.setSamplePercentage(100);
                drainAll();
            }
        }
    }

    private static List<RequestMetricSample> drainAll() {
        List<RequestMetricSample> samples = new ArrayList<>();
        RequestMetricBatch batch;
        do {
            batch = RequestMetrics.drain(1);
            samples.addAll(batch.getSamples());
        } while (batch.getHasMore());
        return samples;
    }

    private static List<RequestMetricSample> drainUntilSampleCount(int expectedCount) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        List<RequestMetricSample> samples = new ArrayList<>();
        RequestMetricBatch batch;
        do {
            batch = RequestMetrics.drain(1);
            samples.addAll(batch.getSamples());
            if (samples.size() < expectedCount && !batch.getHasMore()) {
                assertTrue(
                        System.nanoTime() < deadline,
                        () -> "Timed out waiting for " + expectedCount + " samples: " + samples);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            }
        } while (samples.size() < expectedCount || batch.getHasMore());
        return samples;
    }

    private static void assertNoSamplesFor(long duration, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(duration);
        do {
            RequestMetricBatch batch = RequestMetrics.drain(32);
            assertTrue(batch.getSamples().isEmpty(), () -> "Unexpected late samples: " + batch);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        } while (System.nanoTime() < deadline);
    }
}

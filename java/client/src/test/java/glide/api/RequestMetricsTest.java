/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import glide.api.models.exceptions.ConfigurationError;
import glide.api.models.metrics.RequestMetricBatch;
import glide.api.models.metrics.RequestMetricPhase;
import glide.api.models.metrics.RequestMetricPhaseDuration;
import glide.api.models.metrics.RequestMetricResult;
import glide.api.models.metrics.RequestMetricSample;
import glide.api.models.metrics.RequestMetricsConfiguration;
import glide.ffi.resolvers.RequestMetricsResolver;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RequestMetricsTest {

    @Test
    void rejectsInvalidConfigurationAndDrainArguments() {
        assertThrows(
                ConfigurationError.class,
                () -> RequestMetricsConfiguration.builder().samplePercentage(-1).build());
        assertThrows(
                ConfigurationError.class,
                () -> RequestMetricsConfiguration.builder().samplePercentage(101).build());
        assertThrows(
                ConfigurationError.class,
                () -> RequestMetricsConfiguration.builder().bufferCapacity(0).build());
        assertThrows(
                ConfigurationError.class,
                () -> RequestMetricsConfiguration.builder().bufferCapacity(1_000_001).build());
        assertThrows(
                ConfigurationError.class,
                () ->
                        RequestMetricsConfiguration.builder()
                                .allowedCustomCommands(Collections.singleton("graph.query"))
                                .build());
        assertThrows(IllegalArgumentException.class, () -> RequestMetrics.drain(0));
        assertThrows(IllegalArgumentException.class, () -> RequestMetrics.drain(10_001));
    }

    @Test
    void createsImmutableConfiguration() {
        RequestMetricsConfiguration config =
                RequestMetricsConfiguration.builder()
                        .samplePercentage(10)
                        .bufferCapacity(16_384)
                        .allowedCustomCommands(Collections.singleton("GRAPH.QUERY"))
                        .build();

        assertEquals(10, config.getSamplePercentage());
        assertEquals(16_384, config.getBufferCapacity());
        assertEquals(Collections.singleton("GRAPH.QUERY"), config.getAllowedCustomCommands());
        assertThrows(
                UnsupportedOperationException.class, () -> config.getAllowedCustomCommands().add("GET"));
    }

    @Test
    void mapsUnknownEnumValuesToUnspecified() {
        assertEquals(RequestMetricResult.UNSPECIFIED, RequestMetricResult.fromValue(99));
        assertEquals(RequestMetricPhase.UNSPECIFIED, RequestMetricPhase.fromValue(99));
    }

    @Test
    void createsImmutableValueModelsAndDefensivelyCopiesSamples() {
        RequestMetricPhaseDuration duration =
                new RequestMetricPhaseDuration(RequestMetricPhase.TOTAL, 42);
        List<RequestMetricPhaseDuration> durations = new ArrayList<>(Arrays.asList(duration));
        RequestMetricSample sample =
                new RequestMetricSample("GET", RequestMetricResult.SUCCESS, 1, durations);
        List<RequestMetricSample> samples = new ArrayList<>(Arrays.asList(sample));
        RequestMetricBatch batch = new RequestMetricBatch(samples, 2, 3, true);

        durations.clear();
        samples.clear();

        assertEquals(RequestMetricPhase.TOTAL, duration.getPhase());
        assertEquals(42, duration.getDurationNanos());
        assertEquals("GET", sample.getOperation());
        assertEquals(RequestMetricResult.SUCCESS, sample.getResult());
        assertEquals(1, sample.getAttemptCount());
        assertEquals(Arrays.asList(duration), sample.getPhaseDurations());
        assertEquals(Arrays.asList(sample), batch.getSamples());
        assertEquals(2, batch.getDroppedSamples());
        assertEquals(3, batch.getRemainingSamples());
        assertTrue(batch.getHasMore());
        assertFalse(new RequestMetricBatch(Collections.emptyList(), 0, 0, false).getHasMore());
        assertThrows(
                UnsupportedOperationException.class, () -> sample.getPhaseDurations().add(duration));
        assertThrows(UnsupportedOperationException.class, () -> batch.getSamples().add(sample));
        assertThrows(
                IllegalArgumentException.class,
                () -> new RequestMetricPhaseDuration(RequestMetricPhase.TOTAL, -1));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new RequestMetricSample(
                                "GET", RequestMetricResult.SUCCESS, -1, Collections.emptyList()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new RequestMetricBatch(Collections.emptyList(), -1, 0, false));
    }

    @Test
    void decodesNativeProtobufBatchIntoImmutableModels() {
        request_metrics.RequestMetrics.RequestMetricBatch protobufBatch =
                request_metrics.RequestMetrics.RequestMetricBatch.newBuilder()
                        .addSamples(
                                request_metrics.RequestMetrics.RequestMetricSample.newBuilder()
                                        .setOperation("GET")
                                        .setResultValue(99)
                                        .setAttemptCount(2)
                                        .addPhaseDurations(
                                                request_metrics.RequestMetrics.RequestMetricPhaseDuration.newBuilder()
                                                        .setPhaseValue(99)
                                                        .setDurationNanos(42))
                                        .addPhaseDurations(
                                                request_metrics.RequestMetrics.RequestMetricPhaseDuration.newBuilder()
                                                        .setPhaseValue(0)
                                                        .setDurationNanos(43))
                                        .addPhaseDurations(
                                                request_metrics.RequestMetrics.RequestMetricPhaseDuration.newBuilder()
                                                        .setPhaseValue(
                                                                request_metrics.RequestMetrics.RequestMetricPhase
                                                                        .REQUEST_METRIC_PHASE_TOTAL_VALUE)
                                                        .setDurationNanos(44)))
                        .setDroppedSamples(3)
                        .setRemainingSamples(4)
                        .setHasMore(true)
                        .setUnknownFields(
                                com.google.protobuf.UnknownFieldSet.newBuilder()
                                        .addField(
                                                99,
                                                com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                                        .build())
                        .build();

        RequestMetricBatch batch = RequestMetrics.decodeDrainResponse(protobufBatch.toByteArray());

        assertEquals(3, batch.getDroppedSamples());
        assertEquals(4, batch.getRemainingSamples());
        assertTrue(batch.getHasMore());
        assertEquals(1, batch.getSamples().size());
        assertEquals("GET", batch.getSamples().get(0).getOperation());
        assertEquals(RequestMetricResult.UNSPECIFIED, batch.getSamples().get(0).getResult());
        assertEquals(2, batch.getSamples().get(0).getAttemptCount());
        assertEquals(1, batch.getSamples().get(0).getPhaseDurations().size());
        assertEquals(
                RequestMetricPhase.TOTAL, batch.getSamples().get(0).getPhaseDurations().get(0).getPhase());
        assertEquals(44, batch.getSamples().get(0).getPhaseDurations().get(0).getDurationNanos());
    }

    @Test
    void mapsEveryNativeConfigurationStatusToAStableMessage() throws ReflectiveOperationException {
        Method mapper =
                RequestMetrics.class.getDeclaredMethod("throwForNonzeroStatus", int.class, String.class);
        mapper.setAccessible(true);
        Map<Integer, String> expectedMessages = new LinkedHashMap<>();
        expectedMessages.put(1, "Request metrics sample percentage must be between 0 and 100.");
        expectedMessages.put(2, "Request metrics buffer capacity must be between 1 and 1000000.");
        expectedMessages.put(3, "Request metrics allows at most 64 custom commands.");
        expectedMessages.put(4, "Request metrics custom commands must match [A-Z0-9_.-]{1,64}.");
        expectedMessages.put(
                5,
                "Request metrics are already configured with a different buffer capacity or"
                        + " custom-command allow-list.");
        expectedMessages.put(6, "Request metrics have not been configured.");

        for (Map.Entry<Integer, String> entry : expectedMessages.entrySet()) {
            InvocationTargetException thrown =
                    assertThrows(
                            InvocationTargetException.class,
                            () -> mapper.invoke(null, entry.getKey(), "fallback"));
            assertTrue(thrown.getCause() instanceof ConfigurationError);
            assertEquals(entry.getValue(), thrown.getCause().getMessage());
        }
    }

    @Test
    void exposesRustFriendlyNativeResolverContract() throws ReflectiveOperationException {
        Method configure =
                RequestMetricsResolver.class.getDeclaredMethod(
                        "configureRequestMetrics", int.class, int.class, String[].class);
        Method update =
                RequestMetricsResolver.class.getDeclaredMethod(
                        "setRequestMetricsSamplePercentage", int.class);
        Method drain = RequestMetricsResolver.class.getDeclaredMethod("drainRequestMetrics", int.class);

        assertTrue(Modifier.isNative(configure.getModifiers()));
        assertEquals(int.class, configure.getReturnType());
        assertTrue(Modifier.isNative(update.getModifiers()));
        assertEquals(int.class, update.getReturnType());
        assertTrue(Modifier.isNative(drain.getModifiers()));
        assertEquals(byte[].class, drain.getReturnType());
    }
}

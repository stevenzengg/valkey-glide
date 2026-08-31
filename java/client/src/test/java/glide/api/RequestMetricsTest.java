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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
                                .allowedCustomCommands(Set.of("graph.query"))
                                .build());
        assertThrows(IllegalArgumentException.class, () -> RequestMetrics.drain(0));
        assertThrows(IllegalArgumentException.class, () -> RequestMetrics.drain(10_001));
    }

    @Test
    void createsImmutableConfiguration() {
        var config =
                RequestMetricsConfiguration.builder()
                        .samplePercentage(10)
                        .bufferCapacity(16_384)
                        .allowedCustomCommands(Set.of("GRAPH.QUERY"))
                        .build();

        assertEquals(10, config.getSamplePercentage());
        assertEquals(16_384, config.getBufferCapacity());
        assertEquals(Set.of("GRAPH.QUERY"), config.getAllowedCustomCommands());
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
        var duration = new RequestMetricPhaseDuration(RequestMetricPhase.TOTAL, 42);
        var durations = new ArrayList<>(List.of(duration));
        var sample = new RequestMetricSample("GET", RequestMetricResult.SUCCESS, 1, durations);
        var samples = new ArrayList<>(List.of(sample));
        var batch = new RequestMetricBatch(samples, 2, 3, true);

        durations.clear();
        samples.clear();

        assertEquals(RequestMetricPhase.TOTAL, duration.getPhase());
        assertEquals(42, duration.getDurationNanos());
        assertEquals("GET", sample.getOperation());
        assertEquals(RequestMetricResult.SUCCESS, sample.getResult());
        assertEquals(1, sample.getAttemptCount());
        assertEquals(List.of(duration), sample.getPhaseDurations());
        assertEquals(List.of(sample), batch.getSamples());
        assertEquals(2, batch.getDroppedSamples());
        assertEquals(3, batch.getRemainingSamples());
        assertTrue(batch.getHasMore());
        assertFalse(new RequestMetricBatch(List.of(), 0, 0, false).getHasMore());
        assertThrows(
                UnsupportedOperationException.class, () -> sample.getPhaseDurations().add(duration));
        assertThrows(UnsupportedOperationException.class, () -> batch.getSamples().add(sample));
        assertThrows(
                IllegalArgumentException.class,
                () -> new RequestMetricPhaseDuration(RequestMetricPhase.TOTAL, -1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new RequestMetricSample("GET", RequestMetricResult.SUCCESS, -1, List.of()));
        assertThrows(
                IllegalArgumentException.class, () -> new RequestMetricBatch(List.of(), -1, 0, false));
    }
}

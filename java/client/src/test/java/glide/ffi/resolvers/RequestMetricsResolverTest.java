/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.ffi.resolvers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import glide.api.RequestMetrics;
import glide.api.models.exceptions.ConfigurationError;
import glide.api.models.metrics.RequestMetricBatch;
import glide.api.models.metrics.RequestMetricsConfiguration;
import java.util.Arrays;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RequestMetricsResolverTest {
    private static final int BUFFER_CAPACITY = 16_384;

    @Test
    void configuresUpdatesAndDrainsWithinOneNativeLifecycle() {
        assertEquals(
                1, RequestMetricsResolver.configureRequestMetrics(-1, BUFFER_CAPACITY, new String[0]));
        assertEquals(2, RequestMetricsResolver.configureRequestMetrics(100, -1, new String[0]));

        String[] tooManyCustomCommands = new String[65];
        Arrays.setAll(tooManyCustomCommands, index -> "CUSTOM." + index);
        assertEquals(
                3,
                RequestMetricsResolver.configureRequestMetrics(
                        100, BUFFER_CAPACITY, tooManyCustomCommands));
        assertEquals(
                4,
                RequestMetricsResolver.configureRequestMetrics(
                        100, BUFFER_CAPACITY, new String[] {"graph.query"}));
        assertEquals(1, RequestMetricsResolver.setRequestMetricsSamplePercentage(-1));

        ConfigurationError notConfigured =
                assertThrows(ConfigurationError.class, () -> RequestMetrics.setSamplePercentage(100));
        assertEquals("Request metrics have not been configured.", notConfigured.getMessage());

        RequestMetrics.configure(
                RequestMetricsConfiguration.builder()
                        .samplePercentage(100)
                        .bufferCapacity(BUFFER_CAPACITY)
                        .allowedCustomCommands(Set.of("GRAPH.QUERY"))
                        .build());

        RequestMetricBatch batch = RequestMetrics.drain(10);
        assertTrue(batch.getSamples().isEmpty());
        assertEquals(0, batch.getDroppedSamples());
        assertEquals(0, batch.getRemainingSamples());
        assertFalse(batch.getHasMore());

        RequestMetrics.setSamplePercentage(0);
        RequestMetrics.setSamplePercentage(100);
        assertThrows(IllegalArgumentException.class, () -> RequestMetrics.drain(0));
        assertThrows(IllegalArgumentException.class, () -> RequestMetrics.drain(10_001));
        assertThrows(
                IllegalArgumentException.class, () -> RequestMetricsResolver.drainRequestMetrics(-1));

        assertEquals(
                5,
                RequestMetricsResolver.configureRequestMetrics(
                        100, BUFFER_CAPACITY + 1, new String[] {"GRAPH.QUERY"}));
    }
}

/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api;

import glide.api.models.metrics.RequestMetricBatch;
import glide.api.models.metrics.RequestMetricsConfiguration;
import glide.ffi.resolvers.RequestMetricsResolver;
import java.util.Objects;

/** Public API for configuring and draining native request-phase metric samples. */
public final class RequestMetrics {
    private static final int MAX_DRAIN_SAMPLES = 10_000;

    private RequestMetrics() {}

    /** Configures native request metrics collection. */
    public static void configure(RequestMetricsConfiguration configuration) {
        RequestMetricsConfiguration config =
                Objects.requireNonNull(configuration, "configuration must not be null");
        RequestMetricsResolver.configure(
                config.getSamplePercentage(),
                config.getBufferCapacity(),
                config.getAllowedCustomCommands().stream().sorted().toArray(String[]::new));
    }

    /** Updates the percentage of requests sampled by native request metrics collection. */
    public static void setSamplePercentage(int samplePercentage) {
        RequestMetricsConfiguration.validateSamplePercentage(samplePercentage);
        RequestMetricsResolver.setSamplePercentage(samplePercentage);
    }

    /** Drains at most {@code maxSamples} request metric samples without performing I/O. */
    public static RequestMetricBatch drain(int maxSamples) {
        if (maxSamples < 1 || maxSamples > MAX_DRAIN_SAMPLES) {
            throw new IllegalArgumentException("maxSamples must be between 1 and 10000");
        }
        return RequestMetricsResolver.drain(maxSamples);
    }
}

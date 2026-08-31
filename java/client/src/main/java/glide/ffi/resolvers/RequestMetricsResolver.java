/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.ffi.resolvers;

import glide.api.models.metrics.RequestMetricBatch;

/** Resolver for native request-phase metric collection. */
public final class RequestMetricsResolver {
    static {
        NativeUtils.loadGlideLib();
    }

    private RequestMetricsResolver() {}

    public static native void configure(
            int samplePercentage, int bufferCapacity, String[] allowedCustomCommands);

    public static native void setSamplePercentage(int samplePercentage);

    public static native RequestMetricBatch drain(int maxSamples);
}

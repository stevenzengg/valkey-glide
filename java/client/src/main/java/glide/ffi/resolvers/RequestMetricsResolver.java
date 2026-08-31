/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.ffi.resolvers;

/** Resolver for native request-phase metric collection. */
public final class RequestMetricsResolver {
    static {
        NativeUtils.loadGlideLib();
    }

    private RequestMetricsResolver() {}

    public static native int configureRequestMetrics(
            int samplePercentage, int bufferCapacity, String[] allowedCustomCommands);

    public static native int setRequestMetricsSamplePercentage(int samplePercentage);

    public static native byte[] drainRequestMetrics(int maxSamples);
}

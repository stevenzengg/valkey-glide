/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.ffi.resolvers;

/** Resolver for native request-phase metric collection. */
public final class RequestMetricsResolver {
    /** Stable JNI configuration status codes shared with the native bridge. */
    public static final int STATUS_OK = 0;

    public static final int STATUS_INVALID_SAMPLE_PERCENTAGE = 1;
    public static final int STATUS_INVALID_CAPACITY = 2;
    public static final int STATUS_TOO_MANY_ALLOWED_CUSTOM_COMMANDS = 3;
    public static final int STATUS_INVALID_ALLOWED_CUSTOM_COMMAND = 4;
    public static final int STATUS_CONFIGURATION_MISMATCH = 5;
    public static final int STATUS_NOT_CONFIGURED = 6;

    static {
        NativeUtils.loadGlideLib();
    }

    private RequestMetricsResolver() {}

    public static native int configureRequestMetrics(
            int samplePercentage, int bufferCapacity, String[] allowedCustomCommands);

    public static native int setRequestMetricsSamplePercentage(int samplePercentage);

    public static native byte[] drainRequestMetrics(int maxSamples);
}

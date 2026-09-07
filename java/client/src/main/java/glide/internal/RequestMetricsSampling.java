/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import java.util.concurrent.ThreadLocalRandom;

/** Binding-local request-metrics selection for Java direct commands. */
public final class RequestMetricsSampling {
    private static final int MAX_PERCENTAGE = 100;
    private static volatile int samplePercentage;

    private RequestMetricsSampling() {}

    /** Selects one request using the current percentage. */
    public static boolean shouldSample() {
        int percentage = samplePercentage;
        return percentage == MAX_PERCENTAGE
                || (percentage > 0
                        && shouldSample(percentage, ThreadLocalRandom.current().nextInt(MAX_PERCENTAGE)));
    }

    static boolean shouldSample(int percentile) {
        return shouldSample(samplePercentage, percentile);
    }

    private static boolean shouldSample(int percentage, int percentile) {
        return percentile < percentage;
    }

    /** Updates the percentage after successful native request-metrics configuration. */
    public static void updateSamplePercentage(int percentage) {
        samplePercentage = percentage;
    }

    /** Returns the current percentage for configuration lifecycle tests. */
    public static int getSamplePercentage() {
        return samplePercentage;
    }
}

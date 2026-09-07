/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.metrics;

/** The outcome of a sampled request. */
public enum RequestMetricResult {
    UNSPECIFIED(0),
    SUCCESS(1),
    FAILURE(2),
    TIMEOUT(3),
    CANCELLED(4);

    private final int value;

    RequestMetricResult(int value) {
        this.value = value;
    }

    /** Returns the protobuf numeric value for this result. */
    public int getValue() {
        return value;
    }

    /** Maps a protobuf numeric value to a result, falling back to {@link #UNSPECIFIED}. */
    public static RequestMetricResult fromValue(int value) {
        for (RequestMetricResult result : values()) {
            if (result.value == value) {
                return result;
            }
        }
        return UNSPECIFIED;
    }
}

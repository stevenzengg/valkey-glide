/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.metrics;

/** A phase measured while processing a sampled request. */
public enum RequestMetricPhase {
    UNSPECIFIED(0),
    JNI_INGRESS(1),
    CLIENT_QUEUE(2),
    COMMAND_PREPARE(3),
    CONNECTION_WAIT(4),
    PIPELINE_QUEUE(5),
    SOCKET_WRITE(6),
    RESPONSE_WAIT(7),
    RETRY_BACKOFF(8),
    CORE_DECODE(9),
    CALLBACK_QUEUE(10),
    CALLBACK_COMPLETE(11),
    TOTAL(12);

    private final int value;

    RequestMetricPhase(int value) {
        this.value = value;
    }

    /** Returns the protobuf numeric value for this phase. */
    public int getValue() {
        return value;
    }

    /** Maps a protobuf numeric value to a phase, falling back to {@link #UNSPECIFIED}. */
    public static RequestMetricPhase fromValue(int value) {
        for (RequestMetricPhase phase : values()) {
            if (phase.value == value) {
                return phase;
            }
        }
        return UNSPECIFIED;
    }
}

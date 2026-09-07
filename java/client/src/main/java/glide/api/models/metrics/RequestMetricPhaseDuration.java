/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.metrics;

import java.util.Objects;

/** The elapsed duration of one request processing phase. */
public final class RequestMetricPhaseDuration {
    private final RequestMetricPhase phase;
    private final long durationNanos;

    public RequestMetricPhaseDuration(RequestMetricPhase phase, long durationNanos) {
        this.phase = Objects.requireNonNull(phase, "phase must not be null");
        if (durationNanos < 0) {
            throw new IllegalArgumentException("durationNanos must not be negative");
        }
        this.durationNanos = durationNanos;
    }

    public RequestMetricPhase getPhase() {
        return phase;
    }

    public long getDurationNanos() {
        return durationNanos;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof RequestMetricPhaseDuration)) {
            return false;
        }
        RequestMetricPhaseDuration that = (RequestMetricPhaseDuration) object;
        return durationNanos == that.durationNanos && phase == that.phase;
    }

    @Override
    public int hashCode() {
        return Objects.hash(phase, durationNanos);
    }

    @Override
    public String toString() {
        return "RequestMetricPhaseDuration{"
                + "phase="
                + phase
                + ", durationNanos="
                + durationNanos
                + '}';
    }
}

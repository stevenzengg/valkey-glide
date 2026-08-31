/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.metrics;

import java.util.List;
import java.util.Objects;

/** A sampled request with its result and measured phase durations. */
public final class RequestMetricSample {
    private final String operation;
    private final RequestMetricResult result;
    private final int attemptCount;
    private final List<RequestMetricPhaseDuration> phaseDurations;

    public RequestMetricSample(
            String operation,
            RequestMetricResult result,
            int attemptCount,
            List<RequestMetricPhaseDuration> phaseDurations) {
        this.operation = Objects.requireNonNull(operation, "operation must not be null");
        this.result = Objects.requireNonNull(result, "result must not be null");
        if (attemptCount < 0) {
            throw new IllegalArgumentException("attemptCount must not be negative");
        }
        this.attemptCount = attemptCount;
        this.phaseDurations =
                List.copyOf(Objects.requireNonNull(phaseDurations, "phaseDurations must not be null"));
    }

    public String getOperation() {
        return operation;
    }

    public RequestMetricResult getResult() {
        return result;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public List<RequestMetricPhaseDuration> getPhaseDurations() {
        return phaseDurations;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof RequestMetricSample)) {
            return false;
        }
        RequestMetricSample that = (RequestMetricSample) object;
        return attemptCount == that.attemptCount
                && operation.equals(that.operation)
                && result == that.result
                && phaseDurations.equals(that.phaseDurations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operation, result, attemptCount, phaseDurations);
    }

    @Override
    public String toString() {
        return "RequestMetricSample{"
                + "operation='"
                + operation
                + '\''
                + ", result="
                + result
                + ", attemptCount="
                + attemptCount
                + ", phaseDurations="
                + phaseDurations
                + '}';
    }
}

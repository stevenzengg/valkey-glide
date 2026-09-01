/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** A bounded batch of request metric samples drained from the native buffer. */
public final class RequestMetricBatch {
    private final List<RequestMetricSample> samples;
    private final long droppedSamples;
    private final long remainingSamples;
    private final boolean hasMore;

    public RequestMetricBatch(
            List<RequestMetricSample> samples,
            long droppedSamples,
            long remainingSamples,
            boolean hasMore) {
        this.samples =
                Collections.unmodifiableList(
                        new ArrayList<>(Objects.requireNonNull(samples, "samples must not be null")));
        if (droppedSamples < 0) {
            throw new IllegalArgumentException("droppedSamples must not be negative");
        }
        if (remainingSamples < 0) {
            throw new IllegalArgumentException("remainingSamples must not be negative");
        }
        this.droppedSamples = droppedSamples;
        this.remainingSamples = remainingSamples;
        this.hasMore = hasMore;
    }

    public List<RequestMetricSample> getSamples() {
        return samples;
    }

    public long getDroppedSamples() {
        return droppedSamples;
    }

    public long getRemainingSamples() {
        return remainingSamples;
    }

    public boolean getHasMore() {
        return hasMore;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof RequestMetricBatch)) {
            return false;
        }
        RequestMetricBatch that = (RequestMetricBatch) object;
        return droppedSamples == that.droppedSamples
                && remainingSamples == that.remainingSamples
                && hasMore == that.hasMore
                && samples.equals(that.samples);
    }

    @Override
    public int hashCode() {
        return Objects.hash(samples, droppedSamples, remainingSamples, hasMore);
    }

    @Override
    public String toString() {
        return "RequestMetricBatch{"
                + "samples="
                + samples
                + ", droppedSamples="
                + droppedSamples
                + ", remainingSamples="
                + remainingSamples
                + ", hasMore="
                + hasMore
                + '}';
    }
}

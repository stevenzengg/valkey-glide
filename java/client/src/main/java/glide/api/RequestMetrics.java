/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api;

import com.google.protobuf.InvalidProtocolBufferException;
import glide.api.models.exceptions.ConfigurationError;
import glide.api.models.metrics.RequestMetricBatch;
import glide.api.models.metrics.RequestMetricPhase;
import glide.api.models.metrics.RequestMetricPhaseDuration;
import glide.api.models.metrics.RequestMetricResult;
import glide.api.models.metrics.RequestMetricSample;
import glide.api.models.metrics.RequestMetricsConfiguration;
import glide.ffi.resolvers.RequestMetricsResolver;
import glide.internal.RequestMetricsSampling;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Public API for configuring and draining native request-phase metric samples. */
public final class RequestMetrics {
    private static final int MAX_DRAIN_SAMPLES = 10_000;
    private static final String UNKNOWN_CONFIGURATION_ERROR_MESSAGE =
            "Native request metrics configuration failed.";

    private RequestMetrics() {}

    /** Configures native request metrics collection. */
    public static synchronized void configure(RequestMetricsConfiguration configuration) {
        RequestMetricsConfiguration config =
                Objects.requireNonNull(configuration, "configuration must not be null");
        int status =
                RequestMetricsResolver.configureRequestMetrics(
                        config.getSamplePercentage(),
                        config.getBufferCapacity(),
                        config.getAllowedCustomCommands().stream().sorted().toArray(String[]::new));
        throwForNonzeroStatus(status, UNKNOWN_CONFIGURATION_ERROR_MESSAGE);
        RequestMetricsSampling.updateSamplePercentage(config.getSamplePercentage());
    }

    /** Updates the percentage of requests sampled by native request metrics collection. */
    public static synchronized void setSamplePercentage(int samplePercentage) {
        RequestMetricsConfiguration.validateSamplePercentage(samplePercentage);
        int status = RequestMetricsResolver.setRequestMetricsSamplePercentage(samplePercentage);
        throwForNonzeroStatus(status, UNKNOWN_CONFIGURATION_ERROR_MESSAGE);
        RequestMetricsSampling.updateSamplePercentage(samplePercentage);
    }

    /** Drains at most {@code maxSamples} request metric samples without performing I/O. */
    public static RequestMetricBatch drain(int maxSamples) {
        if (maxSamples < 1 || maxSamples > MAX_DRAIN_SAMPLES) {
            throw new IllegalArgumentException("maxSamples must be between 1 and 10000");
        }
        return decodeDrainResponse(RequestMetricsResolver.drainRequestMetrics(maxSamples));
    }

    static RequestMetricBatch decodeDrainResponse(byte[] serializedBatch) {
        Objects.requireNonNull(serializedBatch, "serialized request metrics batch must not be null");
        try {
            return toRequestMetricBatch(
                    request_metrics.RequestMetrics.RequestMetricBatch.parseFrom(serializedBatch));
        } catch (InvalidProtocolBufferException exception) {
            throw new IllegalArgumentException("Unable to decode request metrics batch.", exception);
        }
    }

    private static void throwForNonzeroStatus(int status, String message) {
        if (status == RequestMetricsResolver.STATUS_OK) {
            return;
        }
        String statusMessage;
        switch (status) {
            case RequestMetricsResolver.STATUS_INVALID_SAMPLE_PERCENTAGE:
                statusMessage = "Request metrics sample percentage must be between 0 and 100.";
                break;
            case RequestMetricsResolver.STATUS_INVALID_CAPACITY:
                statusMessage = "Request metrics buffer capacity must be between 1 and 1000000.";
                break;
            case RequestMetricsResolver.STATUS_TOO_MANY_ALLOWED_CUSTOM_COMMANDS:
                statusMessage = "Request metrics allows at most 64 custom commands.";
                break;
            case RequestMetricsResolver.STATUS_INVALID_ALLOWED_CUSTOM_COMMAND:
                statusMessage = "Request metrics custom commands must match [A-Z0-9_.-]{1,64}.";
                break;
            case RequestMetricsResolver.STATUS_CONFIGURATION_MISMATCH:
                statusMessage =
                        "Request metrics are already configured with a different buffer capacity or"
                                + " custom-command allow-list.";
                break;
            case RequestMetricsResolver.STATUS_NOT_CONFIGURED:
                statusMessage = "Request metrics have not been configured.";
                break;
            default:
                statusMessage = message;
        }
        throw new ConfigurationError(statusMessage);
    }

    private static RequestMetricBatch toRequestMetricBatch(
            request_metrics.RequestMetrics.RequestMetricBatch batch) {
        List<RequestMetricSample> samples =
                batch.getSamplesList().stream()
                        .map(RequestMetrics::toRequestMetricSample)
                        .collect(Collectors.toList());
        return new RequestMetricBatch(
                samples, batch.getDroppedSamples(), batch.getRemainingSamples(), batch.getHasMore());
    }

    private static RequestMetricSample toRequestMetricSample(
            request_metrics.RequestMetrics.RequestMetricSample sample) {
        List<RequestMetricPhaseDuration> phaseDurations =
                sample.getPhaseDurationsList().stream()
                        .map(RequestMetrics::toRequestMetricPhaseDuration)
                        .filter(duration -> duration.getPhase() != RequestMetricPhase.UNSPECIFIED)
                        .collect(Collectors.toList());
        return new RequestMetricSample(
                sample.getOperation(),
                RequestMetricResult.fromValue(sample.getResultValue()),
                sample.getAttemptCount(),
                phaseDurations);
    }

    private static RequestMetricPhaseDuration toRequestMetricPhaseDuration(
            request_metrics.RequestMetrics.RequestMetricPhaseDuration phaseDuration) {
        return new RequestMetricPhaseDuration(
                RequestMetricPhase.fromValue(phaseDuration.getPhaseValue()),
                phaseDuration.getDurationNanos());
    }
}

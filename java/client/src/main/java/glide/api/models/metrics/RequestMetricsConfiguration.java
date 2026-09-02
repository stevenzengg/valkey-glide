/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.metrics;

import glide.api.models.exceptions.ConfigurationError;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/** Configuration for native request-phase metric collection. */
public final class RequestMetricsConfiguration {
    public static final int DEFAULT_SAMPLE_PERCENTAGE = 0;
    public static final int DEFAULT_BUFFER_CAPACITY = 16_384;
    private static final int MAX_BUFFER_CAPACITY = 1_000_000;
    private static final int MAX_ALLOWED_CUSTOM_COMMANDS = 64;
    private static final Pattern CUSTOM_COMMAND_PATTERN = Pattern.compile("[A-Z][A-Z0-9._-]{0,63}");

    private final int samplePercentage;
    private final int bufferCapacity;
    private final Set<String> allowedCustomCommands;

    private RequestMetricsConfiguration(
            int samplePercentage, int bufferCapacity, Set<String> allowedCustomCommands) {
        this.samplePercentage = samplePercentage;
        this.bufferCapacity = bufferCapacity;
        this.allowedCustomCommands = Collections.unmodifiableSet(new HashSet<>(allowedCustomCommands));
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getSamplePercentage() {
        return samplePercentage;
    }

    /** Returns the native buffer capacity. Capacity is immutable after the first configuration. */
    public int getBufferCapacity() {
        return bufferCapacity;
    }

    /**
     * Returns custom commands allowed for sampling. This allow-list is immutable after the first
     * configuration.
     */
    public Set<String> getAllowedCustomCommands() {
        return allowedCustomCommands;
    }

    /** Builder for {@link RequestMetricsConfiguration}. */
    public static final class Builder {
        private int samplePercentage = DEFAULT_SAMPLE_PERCENTAGE;
        private int bufferCapacity = DEFAULT_BUFFER_CAPACITY;
        private Set<String> allowedCustomCommands = Collections.emptySet();

        private Builder() {}

        public Builder samplePercentage(int samplePercentage) {
            this.samplePercentage = samplePercentage;
            return this;
        }

        public Builder bufferCapacity(int bufferCapacity) {
            this.bufferCapacity = bufferCapacity;
            return this;
        }

        public Builder allowedCustomCommands(Set<String> allowedCustomCommands) {
            this.allowedCustomCommands =
                    Collections.unmodifiableSet(new HashSet<>(allowedCustomCommands));
            return this;
        }

        public RequestMetricsConfiguration build() {
            validateSamplePercentage(samplePercentage);
            if (bufferCapacity < 1 || bufferCapacity > MAX_BUFFER_CAPACITY) {
                throw new ConfigurationError("Buffer capacity must be between 1 and 1000000");
            }
            if (allowedCustomCommands.size() > MAX_ALLOWED_CUSTOM_COMMANDS) {
                throw new ConfigurationError("At most 64 custom commands may be allowed");
            }
            for (String command : allowedCustomCommands) {
                if (!CUSTOM_COMMAND_PATTERN.matcher(command).matches()) {
                    throw new ConfigurationError("Custom command names must be uppercase");
                }
            }
            return new RequestMetricsConfiguration(
                    samplePercentage, bufferCapacity, allowedCustomCommands);
        }
    }

    /** Validates a request metric sample percentage. */
    public static void validateSamplePercentage(int samplePercentage) {
        if (samplePercentage < 0 || samplePercentage > 100) {
            throw new ConfigurationError("Sample percentage must be between 0 and 100");
        }
    }
}

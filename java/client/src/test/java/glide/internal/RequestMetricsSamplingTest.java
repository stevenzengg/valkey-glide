/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RequestMetricsSamplingTest {
    @AfterEach
    void resetSampling() {
        RequestMetricsSampling.updateSamplePercentage(0);
    }

    @Test
    void percentageBoundariesUseOneDeterministicPercentile() {
        RequestMetricsSampling.updateSamplePercentage(0);
        assertFalse(RequestMetricsSampling.shouldSample(0));

        RequestMetricsSampling.updateSamplePercentage(1);
        assertTrue(RequestMetricsSampling.shouldSample(0));
        assertFalse(RequestMetricsSampling.shouldSample(1));

        RequestMetricsSampling.updateSamplePercentage(5);
        assertTrue(RequestMetricsSampling.shouldSample(4));
        assertFalse(RequestMetricsSampling.shouldSample(5));

        RequestMetricsSampling.updateSamplePercentage(100);
        assertTrue(RequestMetricsSampling.shouldSample(99));
    }
}

/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.ffi.resolvers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import glide.api.RequestMetrics;
import glide.api.models.exceptions.ConfigurationError;
import glide.api.models.metrics.RequestMetricBatch;
import glide.api.models.metrics.RequestMetricsConfiguration;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class RequestMetricsResolverTest {
    private static final int BUFFER_CAPACITY = 16_384;
    private static final String SUCCESS_MARKER = "REQUEST_METRICS_CHILD_OK";

    @Test
    void runsConfigurationAndDrainLifecycleInAnIsolatedJvm() throws Exception {
        Path outputFile = Files.createTempFile("request-metrics-child", ".log");
        Process child = null;
        try {
            child =
                    new ProcessBuilder(
                                    javaExecutable(),
                                    "-Xcheck:jni",
                                    "-cp",
                                    currentTestClasspath(),
                                    IsolatedRequestMetricsLifecycle.class.getName())
                            .redirectErrorStream(true)
                            .redirectOutput(outputFile.toFile())
                            .start();

            boolean exited = child.waitFor(60, TimeUnit.SECONDS);
            if (!exited) {
                child.destroyForcibly();
                child.waitFor(10, TimeUnit.SECONDS);
            }
            String output = new String(Files.readAllBytes(outputFile), StandardCharsets.UTF_8);

            assertTrue(exited, () -> "Child JVM timed out. Output:\n" + output);
            assertEquals(0, child.exitValue(), () -> "Child JVM failed. Output:\n" + output);
            assertTrue(output.contains(SUCCESS_MARKER), () -> "Missing success marker:\n" + output);
            assertFalse(
                    output.contains("WARNING: JNI local refs"),
                    () -> "JNI local references were not bounded:\n" + output);
        } finally {
            if (child != null && child.isAlive()) {
                child.destroyForcibly();
                child.waitFor(10, TimeUnit.SECONDS);
            }
            Files.deleteIfExists(outputFile);
        }
    }

    private static String javaExecutable() {
        String executable = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        return Paths.get(System.getProperty("java.home"), "bin", executable).toString();
    }

    private static String currentTestClasspath() throws Exception {
        Set<String> entries = new LinkedHashSet<>();
        for (ClassLoader loader = RequestMetricsResolverTest.class.getClassLoader();
                loader != null;
                loader = loader.getParent()) {
            if (loader instanceof URLClassLoader) {
                for (URL url : ((URLClassLoader) loader).getURLs()) {
                    if ("file".equals(url.getProtocol())) {
                        entries.add(Paths.get(url.toURI()).toString());
                    }
                }
            }
        }
        String currentClasspath = System.getProperty("java.class.path", "");
        if (!currentClasspath.isEmpty()) {
            entries.addAll(Arrays.asList(currentClasspath.split(Pattern.quote(File.pathSeparator))));
        }
        if (entries.isEmpty()) {
            throw new IllegalStateException("Unable to resolve the current test runtime classpath");
        }
        return String.join(File.pathSeparator, entries);
    }

    /** Runs in a child process so request-metrics global configuration starts from a clean JVM. */
    public static final class IsolatedRequestMetricsLifecycle {
        private IsolatedRequestMetricsLifecycle() {}

        public static void main(String[] arguments) {
            requireStatus(
                    1, RequestMetricsResolver.configureRequestMetrics(-1, BUFFER_CAPACITY, new String[0]));
            requireStatus(2, RequestMetricsResolver.configureRequestMetrics(100, -1, new String[0]));

            String[] tooManyCustomCommands = new String[65];
            Arrays.setAll(tooManyCustomCommands, index -> "CUSTOM." + index);
            requireStatus(
                    3,
                    RequestMetricsResolver.configureRequestMetrics(
                            100, BUFFER_CAPACITY, tooManyCustomCommands));
            requireStatus(
                    4,
                    RequestMetricsResolver.configureRequestMetrics(
                            100, BUFFER_CAPACITY, new String[] {"graph.query"}));
            requireStatus(1, RequestMetricsResolver.setRequestMetricsSamplePercentage(-1));

            try {
                RequestMetrics.setSamplePercentage(100);
                throw new AssertionError("Expected request metrics to be unconfigured");
            } catch (ConfigurationError error) {
                require(
                        "Request metrics have not been configured.".equals(error.getMessage()),
                        "unexpected not-configured message: " + error.getMessage());
            }

            Set<String> allowedCustomCommands =
                    IntStream.range(0, 63)
                            .mapToObj(index -> "CUSTOM." + index)
                            .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
            allowedCustomCommands.add("GRAPH.QUERY");
            require(allowedCustomCommands.size() == 64, "expected 64 allowed custom commands");

            RequestMetrics.configure(
                    RequestMetricsConfiguration.builder()
                            .samplePercentage(100)
                            .bufferCapacity(BUFFER_CAPACITY)
                            .allowedCustomCommands(allowedCustomCommands)
                            .build());

            RequestMetricBatch batch = RequestMetrics.drain(10);
            require(batch.getSamples().isEmpty(), "expected empty initial drain");
            require(batch.getDroppedSamples() == 0, "expected no dropped samples");
            require(batch.getRemainingSamples() == 0, "expected no remaining samples");
            require(!batch.getHasMore(), "expected hasMore=false");

            RequestMetrics.setSamplePercentage(0);
            RequestMetrics.setSamplePercentage(100);
            expectIllegalArgument(() -> RequestMetrics.drain(0));
            expectIllegalArgument(() -> RequestMetrics.drain(10_001));
            expectIllegalArgument(() -> RequestMetricsResolver.drainRequestMetrics(-1));

            requireStatus(
                    5,
                    RequestMetricsResolver.configureRequestMetrics(
                            100, BUFFER_CAPACITY + 1, allowedCustomCommands.toArray(new String[0])));
            System.out.println(SUCCESS_MARKER + " allowedCustomCommands=" + allowedCustomCommands.size());
        }

        private static void expectIllegalArgument(Runnable action) {
            try {
                action.run();
                throw new AssertionError("Expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
                // Expected.
            }
        }

        private static void requireStatus(int expected, int actual) {
            require(actual == expected, "expected status " + expected + ", got " + actual);
        }

        private static void require(boolean condition, String message) {
            if (!condition) {
                throw new AssertionError(message);
            }
        }
    }
}

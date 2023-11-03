package com.bii.kafka.utils;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author bihaiyang
 * @desc
 * @since 2023/11/02
 */
public class JTestUtils {
    
    
    private static final Logger log = LoggerFactory.getLogger(JTestUtils.class);
    
    public static final long DEFAULT_POLL_INTERVAL_MS = 100;
    public static final long DEFAULT_MAX_WAIT_MS = 15000;
    
    public static File tempDirectory() {
        return tempDirectory(null);
    }
    
    
    public static File tempDirectory(final String prefix) {
        return tempDirectory(null, prefix);
    }
    
    
    public static File tempDirectory(final Path parent, String prefix) {
        final File file;
        prefix = prefix == null ? "kafka-" : prefix;
        try {
            file = parent == null ?
                    Files.createTempDirectory(prefix).toFile() : Files.createTempDirectory(parent, prefix).toFile();
        } catch (final IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }
        file.deleteOnExit();
        
        Exit.addShutdownHook("delete-temp-file-shutdown-hook", () -> {
            try {
                Utils.delete(file);
            } catch (IOException e) {
                log.error("Error deleting {}", file.getAbsolutePath(), e);
            }
        });
        
        return file;
    }
    
    
    
    /**
     * uses default value of 15 seconds for timeout
     */
    public static void waitForCondition(final TestCondition testCondition, final String conditionDetails) throws InterruptedException {
        waitForCondition(testCondition, DEFAULT_MAX_WAIT_MS, () -> conditionDetails);
    }
    
    /**
     * uses default value of 15 seconds for timeout
     */
    public static void waitForCondition(final TestCondition testCondition, final Supplier<String> conditionDetailsSupplier) throws InterruptedException {
        waitForCondition(testCondition, DEFAULT_MAX_WAIT_MS, conditionDetailsSupplier);
    }
    
    /**
     * Wait for condition to be met for at most {@code maxWaitMs} and throw assertion failure otherwise.
     * This should be used instead of {@code Thread.sleep} whenever possible as it allows a longer timeout to be used
     * without unnecessarily increasing test time (as the condition is checked frequently). The longer timeout is needed to
     * avoid transient failures due to slow or overloaded machines.
     */
    public static void waitForCondition(final TestCondition testCondition, final long maxWaitMs, String conditionDetails) throws InterruptedException {
        waitForCondition(testCondition, maxWaitMs, () -> conditionDetails);
    }
    
    /**
     * Wait for condition to be met for at most {@code maxWaitMs} and throw assertion failure otherwise.
     * This should be used instead of {@code Thread.sleep} whenever possible as it allows a longer timeout to be used
     * without unnecessarily increasing test time (as the condition is checked frequently). The longer timeout is needed to
     * avoid transient failures due to slow or overloaded machines.
     */
    public static void waitForCondition(final TestCondition testCondition, final long maxWaitMs, Supplier<String> conditionDetailsSupplier) throws InterruptedException {
        waitForCondition(testCondition, maxWaitMs, DEFAULT_POLL_INTERVAL_MS, conditionDetailsSupplier);
    }
    
    /**
     * Wait for condition to be met for at most {@code maxWaitMs} with a polling interval of {@code pollIntervalMs}
     * and throw assertion failure otherwise. This should be used instead of {@code Thread.sleep} whenever possible
     * as it allows a longer timeout to be used without unnecessarily increasing test time (as the condition is
     * checked frequently). The longer timeout is needed to avoid transient failures due to slow or overloaded
     * machines.
     */
    public static void waitForCondition(
            final TestCondition testCondition,
            final long maxWaitMs,
            final long pollIntervalMs,
            Supplier<String> conditionDetailsSupplier
    ) throws InterruptedException {
        retryOnExceptionWithTimeout(maxWaitMs, pollIntervalMs, () -> {
            String conditionDetailsSupplied = conditionDetailsSupplier != null ? conditionDetailsSupplier.get() : null;
            String conditionDetails = conditionDetailsSupplied != null ? conditionDetailsSupplied : "";
            assertTrue(testCondition.conditionMet(),
                    "Condition not met within timeout " + maxWaitMs + ". " + conditionDetails);
        });
    }
    
    /**
     * Wait for the given runnable to complete successfully, i.e. throw now {@link Exception}s or
     * {@link AssertionError}s, or for the given timeout to expire. If the timeout expires then the
     * last exception or assertion failure will be thrown thus providing context for the failure.
     *
     * @param timeoutMs the total time in milliseconds to wait for {@code runnable} to complete successfully.
     * @param runnable the code to attempt to execute successfully.
     * @throws InterruptedException if the current thread is interrupted while waiting for {@code runnable} to complete successfully.
     */
    public static void retryOnExceptionWithTimeout(final long timeoutMs,
            final ValuelessCallable runnable) throws InterruptedException {
        retryOnExceptionWithTimeout(timeoutMs, DEFAULT_POLL_INTERVAL_MS, runnable);
    }
    
    /**
     * Wait for the given runnable to complete successfully, i.e. throw now {@link Exception}s or
     * {@link AssertionError}s, or for the default timeout to expire. If the timeout expires then the
     * last exception or assertion failure will be thrown thus providing context for the failure.
     *
     * @param runnable the code to attempt to execute successfully.
     * @throws InterruptedException if the current thread is interrupted while waiting for {@code runnable} to complete successfully.
     */
    public static void retryOnExceptionWithTimeout(final ValuelessCallable runnable) throws InterruptedException {
        retryOnExceptionWithTimeout(DEFAULT_MAX_WAIT_MS, DEFAULT_POLL_INTERVAL_MS, runnable);
    }
    
    /**
     * Wait for the given runnable to complete successfully, i.e. throw now {@link Exception}s or
     * {@link AssertionError}s, or for the given timeout to expire. If the timeout expires then the
     * last exception or assertion failure will be thrown thus providing context for the failure.
     *
     * @param timeoutMs the total time in milliseconds to wait for {@code runnable} to complete successfully.
     * @param pollIntervalMs the interval in milliseconds to wait between invoking {@code runnable}.
     * @param runnable the code to attempt to execute successfully.
     * @throws InterruptedException if the current thread is interrupted while waiting for {@code runnable} to complete successfully.
     */
    public static void retryOnExceptionWithTimeout(final long timeoutMs,
            final long pollIntervalMs,
            final ValuelessCallable runnable) throws InterruptedException {
        final long expectedEnd = System.currentTimeMillis() + timeoutMs;
        
        while (true) {
            try {
                runnable.call();
                return;
            } catch (final RuntimeException e) {
                throw e;
            } catch (final AssertionError t) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw t;
                }
            } catch (final Exception e) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw new AssertionError(String.format("Assertion failed with an exception after %s ms", timeoutMs), e);
                }
            }
            Thread.sleep(Math.min(pollIntervalMs, timeoutMs));
        }
    }
    
}

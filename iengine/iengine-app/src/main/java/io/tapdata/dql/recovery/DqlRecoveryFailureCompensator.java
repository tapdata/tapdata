package io.tapdata.dql.recovery;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns best-effort cleanup for one recovery batch.
 *
 * <p>Cleanup actions are registered in lifecycle order and run in reverse
 * order. Cleanup and failure reporting are independently guarded so a
 * callback failure after cleanup can still produce one BATCH_FAILED report,
 * while duplicate failure notifications never repeat cleanup or reporting.</p>
 */
public final class DqlRecoveryFailureCompensator {
    @FunctionalInterface
    public interface CleanupAction {
        void run() throws Exception;
    }

    @FunctionalInterface
    public interface FailureReporter {
        void report(String message);
    }

    private final ConcurrentLinkedDeque<CleanupAction> cleanupActions = new ConcurrentLinkedDeque<>();
    private final AtomicBoolean cleanupStarted = new AtomicBoolean();
    private final AtomicBoolean failureReported = new AtomicBoolean();
    private final FailureReporter failureReporter;
    private final Object registrationMonitor = new Object();

    public DqlRecoveryFailureCompensator(FailureReporter failureReporter) {
        this.failureReporter = Objects.requireNonNull(failureReporter, "failureReporter must not be null");
    }

    /** Registers a resource in creation order; it will be closed LIFO. */
    public void addCleanup(CleanupAction action) {
        Objects.requireNonNull(action, "cleanup action must not be null");
        synchronized (registrationMonitor) {
            if (!cleanupStarted.get()) {
                cleanupActions.push(action);
                return;
            }
        }
        runSafely(action);
    }

    /**
     * Runs cleanup once and returns the first cleanup failure, if any. A
     * normal successful batch uses this before sending BATCH_FINISHED.
     */
    public Throwable cleanup() {
        if (!cleanupStarted.compareAndSet(false, true)) {
            return null;
        }
        Throwable firstFailure = null;
        CleanupAction action;
        while ((action = cleanupActions.poll()) != null) {
            try {
                action.run();
            } catch (Throwable failure) {
                if (firstFailure == null) {
                    firstFailure = failure;
                } else {
                    firstFailure.addSuppressed(failure);
                }
            }
        }
        return firstFailure;
    }

    /**
     * Performs cleanup and best-effort BATCH_FAILED reporting. Neither a
     * cleanup failure nor a reporting failure replaces or escapes the
     * original batch failure.
     */
    public boolean compensate(Throwable failure) {
        Objects.requireNonNull(failure, "failure must not be null");
        cleanup();
        if (!failureReported.compareAndSet(false, true)) {
            return false;
        }
        try {
            failureReporter.report(message(failure));
        } catch (Throwable ignored) {
            // Compensation must never mask the original recovery failure.
        }
        return true;
    }

    public boolean isCleanupStarted() {
        return cleanupStarted.get();
    }

    private void runSafely(CleanupAction action) {
        try {
            action.run();
        } catch (Throwable ignored) {
            // The batch failure has already been published; late resources
            // are still given a close attempt without escaping the callback.
        }
    }

    private String message(Throwable failure) {
        String message = failure.getMessage();
        return message == null || message.isBlank()
                ? failure.getClass().getSimpleName()
                : message;
    }
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import io.tapdata.pdk.core.error.TapPdkRunnerUnknownException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ExactlyOnceWriteCancelledExceptionTest {
    @Test
    void pdkWrappedCancellationMustMatchOnInterruptedThreadWithoutClearingFlag() {
        Throwable error = new TapPdkRunnerUnknownException(new ExactlyOnceWriteCancelledException());
        withInterrupt(() -> assertTrue(ExactlyOnceWriteCancelledException.isCleanCancellation(error)));
    }

    @Test
    void multiplyWrappedCancellationMustMatchOnInterruptedThread() {
        Throwable error = new RuntimeException(new TapPdkRunnerUnknownException(new ExactlyOnceWriteCancelledException()));
        withInterrupt(() -> assertTrue(ExactlyOnceWriteCancelledException.isCleanCancellation(error)));
    }

    @Test
    void rawCancellationMustStillMatchOnInterruptedThread() {
        withInterrupt(() -> assertTrue(ExactlyOnceWriteCancelledException.isCleanCancellation(new ExactlyOnceWriteCancelledException())));
    }

    @Test
    void wrappedRollbackFailureMustNotBeTreatedAsCleanCancellation() {
        ExactlyOnceWriteCancelledException cancelled = new ExactlyOnceWriteCancelledException();
        cancelled.addSuppressed(new RuntimeException("rollback failed"));
        Throwable error = new TapPdkRunnerUnknownException(cancelled);
        withInterrupt(() -> assertFalse(ExactlyOnceWriteCancelledException.isCleanCancellation(error)));
    }

    @Test
    void unrelatedErrorAndNullMustNotBeCancellation() {
        withInterrupt(() -> {
            assertFalse(ExactlyOnceWriteCancelledException.isCleanCancellation(new RuntimeException("write failed")));
            assertFalse(ExactlyOnceWriteCancelledException.isCleanCancellation(null));
        });
    }

    @Test
    void cyclicCauseChainMustTerminateWithoutMatchingCancellation() {
        Throwable first = new RuntimeException("first");
        Throwable second = new RuntimeException("second", first);
        first.initCause(second);
        assertTimeoutPreemptively(java.time.Duration.ofSeconds(1), () ->
                assertFalse(ExactlyOnceWriteCancelledException.isCleanCancellation(first)));
    }

    private void withInterrupt(Runnable assertion) {
        boolean interrupted = Thread.currentThread().isInterrupted();
        Thread.currentThread().interrupt();
        try {
            assertion.run();
            assertTrue(Thread.currentThread().isInterrupted(), "Cancellation matching must preserve the interrupt flag");
        } finally {
            if (!interrupted) Thread.interrupted();
        }
    }
}

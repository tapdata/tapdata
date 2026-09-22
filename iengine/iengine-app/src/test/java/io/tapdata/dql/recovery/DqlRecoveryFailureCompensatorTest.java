package io.tapdata.dql.recovery;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlRecoveryFailureCompensatorTest {

    @Test
    void cleansResourcesInReverseOrderAndReportsTheOriginalFailureOnlyOnce() {
        List<String> closed = new ArrayList<>();
        List<String> reported = new ArrayList<>();
        DqlRecoveryFailureCompensator compensator = new DqlRecoveryFailureCompensator(reported::add);
        compensator.addCleanup(() -> closed.add("source-gate"));
        compensator.addCleanup(() -> closed.add("runner"));
        compensator.addCleanup(() -> closed.add("executor"));

        RuntimeException failure = new RuntimeException("task stopped");

        assertTrue(compensator.compensate(failure));
        assertFalse(compensator.compensate(new RuntimeException("late failure")));

        assertEquals(List.of("executor", "runner", "source-gate"), closed);
        assertEquals(List.of("task stopped"), reported);
    }

    @Test
    void cleanupFailureDoesNotReplaceOriginalFailureAndAReportFailureIsBestEffort() {
        AtomicInteger cleanupCount = new AtomicInteger();
        AtomicInteger reportCount = new AtomicInteger();
        DqlRecoveryFailureCompensator compensator = new DqlRecoveryFailureCompensator(message -> {
            reportCount.incrementAndGet();
            throw new IllegalStateException("TM unavailable");
        });
        compensator.addCleanup(() -> {
            cleanupCount.incrementAndGet();
            throw new IllegalStateException("gate restore failed");
        });
        compensator.addCleanup(cleanupCount::incrementAndGet);

        assertTrue(compensator.compensate(new RuntimeException("runner init failed")));
        assertEquals(2, cleanupCount.get());
        assertEquals(1, reportCount.get());
    }

    @Test
    void cleanupIsIdempotentWhenAResourceIsAddedAfterCompensation() {
        AtomicInteger cleanupCount = new AtomicInteger();
        DqlRecoveryFailureCompensator compensator = new DqlRecoveryFailureCompensator(message -> {
        });

        compensator.compensate(new RuntimeException("source gate failed"));
        compensator.addCleanup(cleanupCount::incrementAndGet);
        compensator.cleanup();

        assertEquals(1, cleanupCount.get());
    }
}

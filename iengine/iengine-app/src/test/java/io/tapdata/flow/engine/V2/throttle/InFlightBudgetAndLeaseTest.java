package io.tapdata.flow.engine.V2.throttle;

import io.tapdata.observable.metric.throttle.MonotonicClock;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class InFlightBudgetAndLeaseTest {

    @Test
    void rollsBackTaskLocalStateWhenAgentPoolIsExhausted() {
        AgentMemoryBudgetPool pool = new AgentMemoryBudgetPool(10_000L, 100L, 10L, 1_000L, MonotonicClock.SYSTEM);
        TaskRunKey key1 = new TaskRunKey("t1", "sync", "task-a", 1L);
        TaskRunKey key2 = new TaskRunKey("t1", "sync", "task-b", 1L);
        InFlightBudget b1 = new InFlightBudget(key1, pool, 10_000L, 100L, 10L, 0L);
        InFlightBudget b2 = new InFlightBudget(key2, pool, 10_000L, 100L, 10L, 0L);

        CapacityLease l1 = b1.tryAcquireLease(7_000L, 10L, 1L, "src", false);
        assertNotNull(l1);

        // Agent pool only has 10_000 - 1_000(reserve) - 7_000 = 2_000 bytes left; b2 asking 3_000 MUST fail with 0 residual on b2!
        assertNull(b2.tryAcquireLease(3_000L, 5L, 1L, "src", true));
        InFlightBudget.Snapshot snap2 = b2.snapshot();
        assertEquals(0L, snap2.usedBytes());
        assertEquals(0L, snap2.usedRecords());
        assertEquals(0L, snap2.usedBatches());
        assertEquals(1, pool.snapshot().waitingTaskCount());

        l1.release(l1.getHolderVersion());
        b2.cancelWaiting();
        assertEquals(0L, pool.snapshot().totalReservedBytes());
    }

    @Test
    void handoffFencesStaleReleaseSharedFanOutAndIncrementalCopyWorkCorrectly() throws Exception {
        AgentMemoryBudgetPool pool = new AgentMemoryBudgetPool(100_000L, 1_000L, 100L, 0L, MonotonicClock.SYSTEM);
        InFlightBudget budget = new InFlightBudget(new TaskRunKey("t1", "sync", "task-handoff", 1L),
                pool, 50_000L, 500L, 50L, 0L);

        CapacityLease lease = budget.tryAcquireLease(2_000L, 10L, 1L, "source", false);
        assertNotNull(lease);
        long v1 = lease.getHolderVersion();

        // Cross-thread handoff increments holderVersion without duplicating reservation
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            long v2 = exec.submit(() -> lease.transferOwnership(v1, "queue-consumer")).get(5, TimeUnit.SECONDS);
            assertEquals(v1 + 1, v2);
            assertEquals(2_000L, budget.snapshot().usedBytes());

            // Stale holder v1 late release must be rejected!
            assertFalse(lease.release(v1));
            assertFalse(lease.isReleased());
            assertEquals(2_000L, budget.snapshot().usedBytes());

            // Real fan-out copy reserves incremental capacity independently
            CapacityLease copyLease = budget.tryReserveIncrementalCopy(1_000L, 5L, 1L, "branch-copy");
            assertNotNull(copyLease);
            assertEquals(3_000L, budget.snapshot().usedBytes());

            // Shared fan-out reference retains primary lease until ALL refs + primary release
            CapacityLease.SharedRef branchB = lease.retainSharedReference(v2, "branch-b");
            assertNotNull(branchB);
            assertTrue(lease.release(v2)); // primary drops refCount from 2 -> 1
            assertFalse(lease.isReleased());
            assertEquals(3_000L, budget.snapshot().usedBytes());

            // Final shared release is idempotent and returns primary 2_000B once
            assertTrue(branchB.release());
            assertFalse(branchB.release());
            assertTrue(lease.isReleased());
            assertEquals(1_000L, budget.snapshot().usedBytes());

            assertTrue(copyLease.release(copyLease.getHolderVersion()));
            assertEquals(0L, budget.snapshot().usedBytes());
            assertEquals(0L, pool.snapshot().totalReservedBytes());
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    void progressiveAckAndOlderBatchSeqSweepPreventFilterLeakage() {
        AgentMemoryBudgetPool pool = new AgentMemoryBudgetPool(100_000L, 1_000L, 100L, 0L, MonotonicClock.SYSTEM);
        InFlightBudget budget = new InFlightBudget(new TaskRunKey("t1", "sync", "task-ack", 1L),
                pool, 50_000L, 500L, 50L, 0L);

        CapacityLease lease1 = budget.tryAcquireLease(1_000L, 10L, 1L, "src", "src-node-1", 1L, false);
        assertNotNull(lease1);
        assertTrue(lease1.ackProcessedEvents(4L));
        assertEquals(6L, lease1.getRemainingRecords());
        assertEquals(600L, lease1.getRemainingBytes());
        assertEquals(600L, budget.snapshot().usedBytes());
        assertEquals(1L, budget.snapshot().usedBatches()); // Batch still held until final slice or sweep

        // Simulate 6 records of seq=1 filtered out upstream while seq=2 completes -> sweep reclaims residual of seq=1
        CapacityLease lease2 = budget.tryAcquireLease(500L, 5L, 1L, "src", "src-node-1", 2L, false);
        assertNotNull(lease2);
        int swept = budget.flushOlderLeasesForSource("src-node-1", 2L);
        assertEquals(1, swept);
        assertTrue(lease1.isReleased());
        assertEquals(500L, budget.snapshot().usedBytes());
        assertEquals(1L, budget.snapshot().usedBatches());

        lease2.ackProcessedEvents(5L);
        assertTrue(lease2.isReleased());
        assertEquals(0L, budget.snapshot().usedBytes());
        assertEquals(0L, budget.snapshot().usedRecords());
        assertEquals(0L, budget.snapshot().usedBatches());
        assertEquals(0L, pool.snapshot().totalReservedBytes());
    }

    @Test
    void enforces80And60HysteresisAndReclaimsUnsettledPrefetchOnEpochInvalidation() {
        AgentMemoryBudgetPool pool = new AgentMemoryBudgetPool(100_000L, 1_000L, 100L, 0L, MonotonicClock.SYSTEM);
        InFlightBudget budget = new InFlightBudget(new TaskRunKey("t1", "sync", "task-hw", 2L),
                pool, 1_000L, 100L, 20L, 0L);

        CapacityLease l1 = budget.tryAcquireLease(790L, 10L, 1L, "s1", false);
        assertFalse(budget.isHighWatermarkPaused());

        // Cross 80% threshold -> high watermark paused triggers
        CapacityLease l2 = budget.tryAcquireLease(20L, 1L, 1L, "s1", false);
        assertTrue(budget.isHighWatermarkPaused()); // 810/1000 = 0.81

        // Drop to 70% -> stays paused due to hysteresis
        l2.release(l2.getHolderVersion()); // now 790
        CapacityLease l3 = budget.tryAcquireLease(0L, 0L, 0L, "noop", false);
        assertNotNull(l3);
        l3.release(l3.getHolderVersion());
        // Drop by acking part of l1 down to 650 (still > 60%) -> still paused
        l1.ackProcessedEvents(1L); // remaining = 790 - 79 = 711 (>600)
        assertTrue(budget.isHighWatermarkPaused());

        // Ack 2 more -> remaining <= 600 (711 - ~158 = 553 <= 600) -> exits hysteresis pause!
        l1.ackProcessedEvents(3L);
        assertFalse(budget.isHighWatermarkPaused());

        // Prefetch unsettled + invalidateAndReclaimAll zeroes both bytes, records, batches in task and agent pool
        InFlightBudget.PrefetchToken prefetch = budget.tryReservePrefetch(150L, 12L, 3L, false);
        assertNotNull(prefetch);
        assertTrue(pool.snapshot().totalReservedBytes() > 0L);

        budget.invalidateAndReclaimAll();
        assertTrue(budget.isInvalidated());
        assertEquals(0L, budget.snapshot().usedBytes());
        assertEquals(0L, budget.snapshot().committedPrefetchBytes());
        assertEquals(0L, budget.snapshot().usedRecords());
        assertEquals(0L, budget.snapshot().usedBatches());
        assertEquals(0L, pool.snapshot().totalReservedBytes());
        assertEquals(0L, pool.snapshot().occupiedRecords());
        assertEquals(0L, pool.snapshot().occupiedBatches());
    }

    @Test
    void agentPoolRoundRobinLetsSmallerEligibleTaskProgressWithoutStarvingWaitingTasks() {
        AgentMemoryBudgetPool pool = new AgentMemoryBudgetPool(1_000L, 100L, 20L, 0L, MonotonicClock.SYSTEM);

        // Task hold occupies 800B, leaving 200B free
        assertTrue(pool.tryReserve("holder", 800L, 10L, 1L, false, false));

        // Large task A needs 500B -> queued as waiting head
        assertFalse(pool.tryReserve("task-A", 500L, 5L, 1L, false, true));
        // Small task B needs 150B -> queued behind task-A initially because queue non-empty
        assertFalse(pool.tryReserve("task-B", 150L, 2L, 1L, false, true));
        assertEquals(2, pool.snapshot().waitingTaskCount());

        // Task A retries and still cannot fit 500B -> rotates itself to tail so task-B moves to head
        assertFalse(pool.tryReserve("task-A", 500L, 5L, 1L, false, true));
        // Now task B at head fits 150B and makes progress!
        assertTrue(pool.tryReserve("task-B", 150L, 2L, 1L, false, true));

        // Holder releases 800B -> remaining 150B used -> 850B free -> task A fits and succeeds!
        pool.release(800L, 10L, 1L, false);
        assertTrue(pool.tryReserve("task-A", 500L, 5L, 1L, false, true));
    }
}

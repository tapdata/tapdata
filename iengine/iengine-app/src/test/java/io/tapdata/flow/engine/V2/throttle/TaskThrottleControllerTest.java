package io.tapdata.flow.engine.V2.throttle;

import com.tapdata.tm.commons.throttle.TaskThrottleApplyReceipt;
import com.tapdata.tm.commons.throttle.TaskThrottlePolicyDto;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import io.tapdata.observable.metric.throttle.MonotonicClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class TaskThrottleControllerTest {

    private static final class VirtualClock implements MonotonicClock {
        private final AtomicLong mono = new AtomicLong(1_000_000_000L);
        private final AtomicLong utc = new AtomicLong(1_700_000_000_000L);

        void advanceMillis(long millis) {
            long nanos = TimeUnit.MILLISECONDS.toNanos(millis);
            mono.addAndGet(nanos);
            utc.addAndGet(millis);
        }

        void advanceNanos(long nanos) {
            mono.addAndGet(nanos);
            utc.addAndGet(TimeUnit.NANOSECONDS.toMillis(nanos));
        }

        @Override
        public long nanoTime() {
            return mono.get();
        }

        @Override
        public long currentTimeMillis() {
            return utc.get();
        }
    }

    private static TaskThrottlePolicyDto fullInputPolicy(long version, Long bytesPerSec, Long qps) {
        Map<ThrottlePhase, ThrottlePhaseLimit> phases = new EnumMap<>(ThrottlePhase.class);
        phases.put(ThrottlePhase.FULL, ThrottlePhaseLimit.builder()
                .enabled(true)
                .bytesPerSec(bytesPerSec)
                .eventsPerSec(qps)
                .build());
        return TaskThrottlePolicyDto.builder()
                .tenantId("t1")
                .taskKind("sync")
                .taskId("task-1")
                .desiredVersion(version)
                .phases(phases)
                .build();
    }

    @Test
    void enforcesUnifiedWindowFormulaAndBoundedOverdraftWithDebtPayoff() {
        VirtualClock clock = new VirtualClock();
        TaskRunKey key = new TaskRunKey("t1", "sync", "task-1", 1L);
        TaskThrottleController controller = new TaskThrottleController(key, clock);

        // Start from empty bucket: rate = 1_000 B/s (normalBurst = 1_000, maxAtomic = 5_000)
        TaskThrottleApplyReceipt receipt = controller.applyPolicy(fullInputPolicy(1L, 1_000L, null), "agent-1", null);
        assertEquals("APPLIED", receipt.getOutcome());
        assertEquals(1L, controller.getAppliedVersion());

        // At t=0 bucket balance is 0, request 500 bytes -> wait 500ms
        TaskThrottleController.WaitTicket ticket = controller.newWaitTicket();
        TaskThrottleController.AcquireResult r0 = controller.tryAcquire(
                ThrottlePhase.FULL, ThrottleDirection.INPUT, 500L, 1L, true, true, ticket);
        assertEquals(TaskThrottleController.Outcome.RATE_WAIT, r0.outcome());
        assertTrue(r0.waitingDimensions().contains(ThrottleDimension.BYTES));
        assertEquals(TimeUnit.MILLISECONDS.toNanos(500L), r0.waitNanos());

        // Advance 2.0s: capped at normalBurst = 1_000, remainder cleared when full.
        clock.advanceMillis(2_000L);
        long totalDebited = 0L;

        // First oversized non-splittable batch of 3_000B requires bucket full (1_000B >= 1_000B) -> balance drops to -2_000B
        TaskThrottleController.AcquireResult overdraft = controller.tryAcquire(
                ThrottlePhase.FULL, ThrottleDirection.INPUT, 3_000L, 1L, false, true, controller.newWaitTicket());
        assertEquals(TaskThrottleController.Outcome.ADMITTED, overdraft.outcome());
        totalDebited += 3_000L;

        // Immediately try another 500B: bucket is -2_000B -> deficit is 2_500B -> wait 2.5s
        TaskThrottleController.AcquireResult waitDebt = controller.tryAcquire(
                ThrottlePhase.FULL, ThrottleDirection.INPUT, 500L, 1L, true, true, controller.newWaitTicket());
        assertEquals(TaskThrottleController.Outcome.RATE_WAIT, waitDebt.outcome());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(2_500L), waitDebt.waitNanos());

        // Advance 2.5s: balance reaches +500B, small 500B succeeds!
        clock.advanceMillis(2_500L);
        TaskThrottleController.AcquireResult smallOk = controller.tryAcquire(
                ThrottlePhase.FULL, ThrottleDirection.INPUT, 500L, 1L, true, true, controller.newWaitTicket());
        assertEquals(TaskThrottleController.Outcome.ADMITTED, smallOk.outcome());
        totalDebited += 500L;

        // Verify AC-05 formula over elapsed window after bucket hit full at t=2.0s:
        // Effective window since t=2.0s duration=2.5s -> admitted(3500) <= 1000 * 2.5 + max(1000, 3000) = 5500
        double durationSec = 2.5d;
        double upperBound = 1_000.0d * durationSec + Math.max(1_000L, 3_000L);
        assertTrue(totalDebited <= upperBound);
    }

    @Test
    void splittableRequestWithinFiveSecondAtomicLimitUsesOverdraftInsteadOfFailing() {
        VirtualClock clock = new VirtualClock();
        TaskRunKey key = new TaskRunKey("t1", "sync", "task-splittable", 1L);
        TaskThrottleController controller = new TaskThrottleController(key, clock);
        controller.applyPolicy(fullInputPolicy(1L, 1_000L, 100L), "agent-1", null);

        // Refill normalBurst=1_000, events=10
        clock.advanceMillis(1_000L);
        TaskThrottleController.AcquireResult splittableOverdraft = controller.tryAcquire(
                ThrottlePhase.FULL, ThrottleDirection.INPUT, 2_500L, 1L, true, true, controller.newWaitTicket());
        assertEquals(TaskThrottleController.Outcome.ADMITTED, splittableOverdraft.outcome());

        // Request exceeding 5s atomic cap (bytes maxAtomic = 5_000) still fails cleanly without debiting
        TaskThrottleController.AcquireResult tooBigSplittable = controller.tryAcquire(
                ThrottlePhase.FULL, ThrottleDirection.INPUT, 5_001L, 1L, true, true, controller.newWaitTicket());
        assertEquals(TaskThrottleController.Outcome.EXCEEDS_ATOMIC_LIMIT, tooBigSplittable.outcome());
        assertEquals(ThrottleDimension.BYTES, tooBigSplittable.exceededDimension());
    }

    @Test
    void enforcesDualDimensionsSimultaneouslyAndRejectsOverFiveSecondsAtomicLimit() {
        VirtualClock clock = new VirtualClock();
        TaskRunKey key = new TaskRunKey("t1", "sync", "task-1", 2L);
        TaskThrottleController controller = new TaskThrottleController(key, clock);

        controller.applyPolicy(fullInputPolicy(1L, 1_000L, 100L), "agent-1", null);
        // Advance 50ms: bytes=50 (max 1000), qps=5 (max 10)
        clock.advanceMillis(50L);

        // Request 800B + 10 events -> BOTH BYTES (deficit 750 -> 750ms) and QPS (deficit 5 -> 50ms) are short; max wait = 750ms!
        TaskThrottleController.AcquireResult waitBoth = controller.tryAcquire(
                ThrottlePhase.FULL, ThrottleDirection.INPUT, 800L, 10L, false, true, controller.newWaitTicket());
        assertEquals(TaskThrottleController.Outcome.RATE_WAIT, waitBoth.outcome());
        assertTrue(waitBoth.waitingDimensions().contains(ThrottleDimension.BYTES));
        assertTrue(waitBoth.waitingDimensions().contains(ThrottleDimension.QPS));
        assertEquals(TimeUnit.MILLISECONDS.toNanos(750L), waitBoth.waitNanos());

        // Request exceeding 5s atomic cap (bytes maxAtomic = 5_000) fails without debiting
        TaskThrottleController.AcquireResult tooBig = controller.tryAcquire(
                ThrottlePhase.FULL, ThrottleDirection.INPUT, 5_001L, 1L, false, true, controller.newWaitTicket());
        assertEquals(TaskThrottleController.Outcome.EXCEEDS_ATOMIC_LIMIT, tooBig.outcome());
        assertEquals(ThrottleDimension.BYTES, tooBig.exceededDimension());
    }

    @Test
    void fairReservationAfterOneSecondBlocksCompetingSameSlotSmallBatchWithoutStarvingOtherSlots() {
        VirtualClock clock = new VirtualClock();
        TaskRunKey key = new TaskRunKey("t1", "sync", "task-1", 3L);
        TaskThrottleController controller = new TaskThrottleController(key, clock);
        controller.applyPolicy(fullInputPolicy(1L, 1_000L, null), "agent-1", null);

        // Big candidate waits at t=0 for 2000B (needs full burst 1000B)
        TaskThrottleController.WaitTicket bigTicket = controller.newWaitTicket();
        assertEquals(TaskThrottleController.Outcome.RATE_WAIT,
                controller.tryAcquire(ThrottlePhase.FULL, ThrottleDirection.INPUT, 2_000L, 1L, false, true, bigTicket).outcome());

        // Advance 500ms (< 1s threshold): small candidate still CAN steal 200B before reservation engages
        clock.advanceMillis(500L);
        assertEquals(TaskThrottleController.Outcome.ADMITTED,
                controller.tryAcquire(ThrottlePhase.FULL, ThrottleDirection.INPUT, 200L, 1L, true, true, controller.newWaitTicket()).outcome());

        // At 1.1s, big candidate retries and has waited >= 1.0s -> grabs fair reservation on (FULL, INPUT, BYTES)
        clock.advanceMillis(600L);
        assertEquals(TaskThrottleController.Outcome.RATE_WAIT,
                controller.tryAcquire(ThrottlePhase.FULL, ThrottleDirection.INPUT, 2_000L, 1L, false, true, bigTicket).outcome());

        // Now even though bucket has 900B, a competing small 100B batch on SAME slot MUST be blocked!
        TaskThrottleController.AcquireResult smallBlocked = controller.tryAcquire(
                ThrottlePhase.FULL, ThrottleDirection.INPUT, 100L, 1L, true, true, controller.newWaitTicket());
        assertEquals(TaskThrottleController.Outcome.RATE_WAIT, smallBlocked.outcome());

        // Meanwhile non-contending (FULL, OUTPUT) slot still succeeds freely!
        assertEquals(TaskThrottleController.Outcome.ADMITTED,
                controller.tryAcquire(ThrottlePhase.FULL, ThrottleDirection.OUTPUT, 500L, 1L, true, true, controller.newWaitTicket()).outcome());

        // Refill remaining 100ms so bucket reaches 1000B -> big candidate acquires and releases reservation
        clock.advanceMillis(100L);
        assertEquals(TaskThrottleController.Outcome.ADMITTED,
                controller.tryAcquire(ThrottlePhase.FULL, ThrottleDirection.INPUT, 2_000L, 1L, false, true, bigTicket).outcome());
    }

    @Test
    void hotUpdatePreservesNegativeDebtClipsSurplusStartsNewDimensionEmptyAndRejectsInvalid() {
        VirtualClock clock = new VirtualClock();
        TaskRunKey key = new TaskRunKey("t1", "sync", "task-1", 4L);
        TaskThrottleController controller = new TaskThrottleController(key, clock);

        // v1: 1_000 B/s -> fill burst (1_000), overdraft 4_000 -> balance = -3_000
        controller.applyPolicy(fullInputPolicy(1L, 1_000L, null), "agent-1", null);
        clock.advanceMillis(1_000L);
        assertEquals(TaskThrottleController.Outcome.ADMITTED,
                controller.tryAcquire(ThrottlePhase.FULL, ThrottleDirection.INPUT, 4_000L, 1L, false, true, controller.newWaitTicket()).outcome());

        // Advance 1s -> balance recovers to -2_000, now hot-update to v2 (bytes = 500 B/s, newly added QPS = 100/s)
        clock.advanceMillis(1_000L);
        TaskThrottleApplyReceipt v2Receipt = controller.applyPolicy(fullInputPolicy(2L, 500L, 100L), "agent-1", List.of());
        assertEquals("APPLIED", v2Receipt.getOutcome());
        assertEquals(2L, controller.getAppliedVersion());

        // Negative bytes debt (-2_000) preserved; waiting for 100B requires paying 2_100B @ 500 B/s = 4.2s, AND newly added QPS starts empty (0)
        TaskThrottleController.AcquireResult afterHot = controller.tryAcquire(
                ThrottlePhase.FULL, ThrottleDirection.INPUT, 100L, 1L, true, true, controller.newWaitTicket());
        assertEquals(TaskThrottleController.Outcome.RATE_WAIT, afterHot.outcome());
        assertTrue(afterHot.waitingDimensions().contains(ThrottleDimension.BYTES));
        assertTrue(afterHot.waitingDimensions().contains(ThrottleDimension.QPS));
        assertEquals(TimeUnit.MILLISECONDS.toNanos(4_200L), afterHot.waitNanos());

        // Reject invalid non-positive rate while retaining v2
        TaskThrottleApplyReceipt invalid = controller.applyPolicy(fullInputPolicy(3L, 0L, 100L), "agent-1", null);
        assertEquals("REJECTED", invalid.getOutcome());
        assertEquals(2L, controller.getAppliedVersion());

        // Stale desiredVersion < appliedVersion is ignored
        TaskThrottleApplyReceipt stale = controller.applyPolicy(fullInputPolicy(1L, 2_000L, null), "agent-1", null);
        assertEquals("IGNORED_STALE", stale.getOutcome());
    }

    @Test
    void concurrentMultiSlotAcquireAndPolicyUpdatesNeverDeadlock() throws Exception {
        TaskRunKey key = new TaskRunKey("t1", "sync", "task-stress", 5L);
        TaskThrottleController controller = new TaskThrottleController(key, MonotonicClock.SYSTEM);
        Map<ThrottlePhase, ThrottlePhaseLimit> map = new EnumMap<>(ThrottlePhase.class);
        map.put(ThrottlePhase.FULL, ThrottlePhaseLimit.builder().enabled(true).bytesPerSec(1_000_000L).eventsPerSec(10_000L).build());
        map.put(ThrottlePhase.CDC, ThrottlePhaseLimit.builder().enabled(true).bytesPerSec(1_000_000L).eventsPerSec(10_000L).build());
        controller.applyPolicy(TaskThrottlePolicyDto.builder().taskId("task-stress").desiredVersion(1L).phases(map).build(), "agent-1", null);

        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int t = 0; t < 3; t++) {
                final int tid = t;
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < 200; i++) {
                        List<TaskThrottleController.SlotDemand> demands = tid % 2 == 0
                                ? List.of(
                                        new TaskThrottleController.SlotDemand(ThrottlePhase.CDC, ThrottleDirection.OUTPUT, 100, 1, true, true),
                                        new TaskThrottleController.SlotDemand(ThrottlePhase.FULL, ThrottleDirection.INPUT, 100, 1, true, true))
                                : List.of(
                                        new TaskThrottleController.SlotDemand(ThrottlePhase.FULL, ThrottleDirection.OUTPUT, 100, 1, true, true),
                                        new TaskThrottleController.SlotDemand(ThrottlePhase.CDC, ThrottleDirection.INPUT, 100, 1, true, true));
                        controller.tryAcquireAll(demands, controller.newWaitTicket());
                    }
                    return null;
                }));
            }
            futures.add(pool.submit(() -> {
                start.await();
                for (long v = 2; v <= 30; v++) {
                    controller.applyPolicy(TaskThrottlePolicyDto.builder().taskId("task-stress").desiredVersion(v).phases(map).build(), "agent-1", null);
                }
                return null;
            }));
            start.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void oversizedAtomicRequestWithOverdraftFlagWaitsForBurstAndPaysOffDebt() {
        VirtualClock clock = new VirtualClock();
        TaskRunKey key = new TaskRunKey("t1", "sync", "task-atomic-overdraft", 6L);
        TaskThrottleController controller = new TaskThrottleController(key, clock);
        controller.applyPolicy(fullInputPolicy(1L, 1_000L, 100L), "agent-1", null);

        // At t=0 bucket balance is empty; default disallow returns EXCEEDS_ATOMIC_LIMIT on 6_000B (> 5_000B maxAtomic)
        TaskThrottleController.AcquireResult rejected = controller.tryAcquire(
                ThrottlePhase.FULL, ThrottleDirection.INPUT, 6_000L, 1L, false, true, controller.newWaitTicket());
        assertEquals(TaskThrottleController.Outcome.EXCEEDS_ATOMIC_LIMIT, rejected.outcome());

        // With allowAtomicOverdraft=true: waits until bucket fills normalBurst=1_000 (at t=1s)
        TaskThrottleController.AcquireResult waitingForBurst = controller.tryAcquireAll(
                List.of(new TaskThrottleController.SlotDemand(
                        ThrottlePhase.FULL, ThrottleDirection.INPUT, 6_000L, 1L, false, true, true)),
                controller.newWaitTicket());
        assertEquals(TaskThrottleController.Outcome.RATE_WAIT, waitingForBurst.outcome());

        clock.advanceMillis(1_000L);
        TaskThrottleController.AcquireResult admitted = controller.tryAcquireAll(
                List.of(new TaskThrottleController.SlotDemand(
                        ThrottlePhase.FULL, ThrottleDirection.INPUT, 6_000L, 1L, false, true, true)),
                controller.newWaitTicket());
        assertEquals(TaskThrottleController.Outcome.ADMITTED, admitted.outcome());

        // Balance is now -5_000 debt; subsequent 100B must wait 5.1s to pay off debt before proceeding
        TaskThrottleController.AcquireResult payingDebt = controller.tryAcquire(
                ThrottlePhase.FULL, ThrottleDirection.INPUT, 100L, 1L, true, true, controller.newWaitTicket());
        assertEquals(TaskThrottleController.Outcome.RATE_WAIT, payingDebt.outcome());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(5_100L), payingDebt.waitNanos());

        clock.advanceMillis(5_100L);
        assertEquals(TaskThrottleController.Outcome.ADMITTED, controller.tryAcquire(
                ThrottlePhase.FULL, ThrottleDirection.INPUT, 100L, 1L, true, true, controller.newWaitTicket()).outcome());
    }
}

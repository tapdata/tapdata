package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataCountDownLatchEvent;
import com.tapdata.entity.TapdataDqlRecoveryEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlRecoveryBarrierCoordinatorTest {

    @Test
    void enqueuesOneCountDownEventAfterDataAndWaitsForTargetCompletion() throws Exception {
        RecordingBoundary boundary = new RecordingBoundary(true);
        DqlRecoveryBarrierCoordinator coordinator = new DqlRecoveryBarrierCoordinator(boundary);
        boundary.recordData();

        DqlRecoveryBarrier.Outcome outcome = coordinator.await("event-1", 500L);

        assertEquals(DqlRecoveryBarrier.Outcome.SUCCESS, outcome);
        assertEquals(List.of("data", "barrier"), boundary.calls);
        assertEquals(0, coordinator.activeBarrierCount());
    }

    @Test
    void targetFailureCompletesTheMatchingBarrierAsFailed() throws Exception {
        RecordingBoundary boundary = new RecordingBoundary(false);
        DqlRecoveryBarrierCoordinator coordinator = new DqlRecoveryBarrierCoordinator(boundary);
        CountDownLatch waiting = new CountDownLatch(1);
        List<DqlRecoveryBarrier.Outcome> outcomes = new ArrayList<>();
        Thread waiter = new Thread(() -> {
            waiting.countDown();
            try {
                outcomes.add(coordinator.await("event-1", 2000L));
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.start();

        assertTrue(waiting.await(1, TimeUnit.SECONDS));
        assertTrue(boundary.barrierEnqueued.await(1, TimeUnit.SECONDS));
        coordinator.complete("event-1", DqlRecoveryBarrier.Outcome.FAILED);
        waiter.join(1000L);

        assertEquals(List.of(DqlRecoveryBarrier.Outcome.FAILED), outcomes);
        assertEquals(0, coordinator.activeBarrierCount());
    }

    @Test
    void registeredCaptureFailureReleasesTheMatchingBarrier() throws Exception {
        RecordingBoundary boundary = new RecordingBoundary(false);
        DqlRecoveryBarrierCoordinator coordinator = new DqlRecoveryBarrierCoordinator(boundary);
        coordinator.register("event-1");
        List<DqlRecoveryBarrier.Outcome> outcomes = new ArrayList<>();
        Thread waiter = new Thread(() -> {
            try {
                outcomes.add(coordinator.await("event-1", 2000L));
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.start();

        assertTrue(boundary.barrierEnqueued.await(1, TimeUnit.SECONDS));
        assertTrue(DqlRecoveryFailureRegistry.fail("event-1", new RuntimeException("target failed")));
        waiter.join(1000L);

        assertEquals(List.of(DqlRecoveryBarrier.Outcome.FAILED), outcomes);
        assertEquals(0, coordinator.activeBarrierCount());
    }

    @Test
    void registeredCaptureFailurePreservesOriginalMessageAndStack() throws Exception {
        RecordingBoundary boundary = new RecordingBoundary(false);
        DqlRecoveryBarrierCoordinator coordinator = new DqlRecoveryBarrierCoordinator(boundary);
        coordinator.register("event-with-error");
        RuntimeException failure = new RuntimeException("Duplicate entry '2' for key 'idx_unique_order_no'");

        CountDownLatch waiting = new CountDownLatch(1);
        AtomicReference<DqlRecoveryBarrier.Result> result = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            waiting.countDown();
            try {
                result.set(coordinator.awaitResult("event-with-error", 2000L));
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.start();
        assertTrue(waiting.await(1, TimeUnit.SECONDS));
        assertTrue(boundary.barrierEnqueued.await(1, TimeUnit.SECONDS));
        assertTrue(DqlRecoveryFailureRegistry.fail("event-with-error", failure));
        waiter.join(1000L);

        assertEquals(DqlRecoveryBarrier.Outcome.FAILED, result.get().outcome());
        assertEquals(failure.getMessage(), result.get().message());
        assertTrue(result.get().errorDetails().contains(failure.getClass().getName()));
    }

    @Test
    void timeoutIsTerminalAndDoesNotLeaveBarrierStateBehind() throws Exception {
        RecordingBoundary boundary = new RecordingBoundary(false);
        DqlRecoveryBarrierCoordinator coordinator = new DqlRecoveryBarrierCoordinator(boundary);

        assertEquals(DqlRecoveryBarrier.Outcome.TIMEOUT, coordinator.await("event-1", 1L));
        assertEquals(0, coordinator.activeBarrierCount());
    }

    @Test
    void jobInitializationFailureIsReportedInsteadOfBeingMisclassifiedAsTimeout() throws Exception {
        RecordingBoundary boundary = new RecordingBoundary(false);
        RuntimeException failure = new RuntimeException("Init node failed, connection is null");
        DqlRecoveryBarrierCoordinator coordinator = new DqlRecoveryBarrierCoordinator(
                boundary, null, () -> failure);

        DqlRecoveryBarrier.Result result = coordinator.awaitResult("event-init-failure", 1L);

        assertEquals(DqlRecoveryBarrier.Outcome.FAILED, result.outcome());
        assertEquals(failure.getMessage(), result.message());
        assertTrue(result.errorDetails().contains(failure.getClass().getName()));
        assertEquals(0, coordinator.activeBarrierCount());
    }

    private static class RecordingBoundary implements DqlReplaySourceNode {
        private final List<String> calls = new ArrayList<>();
        private final CountDownLatch barrierEnqueued = new CountDownLatch(1);
        private final boolean completeImmediately;

        private RecordingBoundary(boolean completeImmediately) {
            this.completeImmediately = completeImmediately;
        }

        private void recordData() {
            calls.add("data");
        }

        @Override
        public void enqueue(TapdataDqlRecoveryEvent event) {
            calls.add("data");
        }

        @Override
        public void enqueueBarrier(TapdataCountDownLatchEvent event) {
            calls.add("barrier");
            barrierEnqueued.countDown();
            if (completeImmediately) {
                event.getCountDownLatch().countDown();
            }
        }
    }
}

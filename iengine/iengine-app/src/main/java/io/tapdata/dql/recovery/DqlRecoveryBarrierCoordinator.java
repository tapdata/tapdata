package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataCountDownLatchEvent;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Converts the source-queue order into a per-event target completion barrier.
 * The DML event is injected by the recovery coordinator first; this class is
 * called immediately afterwards and appends exactly one count-down event.
 */
public final class DqlRecoveryBarrierCoordinator implements DqlRecoveryBarrier {
    private final DqlReplaySourceNode sourceBoundary;
    private final Map<String, PendingBarrier> pendingBarriers = new ConcurrentHashMap<>();

    public DqlRecoveryBarrierCoordinator(DqlReplaySourceNode sourceBoundary) {
        this.sourceBoundary = Objects.requireNonNull(sourceBoundary, "sourceBoundary must not be null");
    }

    @Override
    public Outcome await(String eventId, long timeoutMillis) throws InterruptedException {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("recovery eventId must not be blank");
        }
        if (timeoutMillis <= 0L) {
            throw new IllegalArgumentException("recovery barrier timeout must be greater than zero");
        }
        TapdataCountDownLatchEvent barrierEvent = TapdataCountDownLatchEvent.create(1);
        PendingBarrier pending = new PendingBarrier(barrierEvent);
        if (pendingBarriers.putIfAbsent(eventId, pending) != null) {
            throw new IllegalStateException("recovery barrier already exists for event " + eventId);
        }
        try {
            sourceBoundary.enqueueBarrier(barrierEvent);
            if (!barrierEvent.getCountDownLatch().await(timeoutMillis, TimeUnit.MILLISECONDS)) {
                return Outcome.TIMEOUT;
            }
            return pending.outcome();
        } finally {
            pendingBarriers.remove(eventId, pending);
        }
    }

    /** Called by a processor/target failure path for the matching recovery event. */
    public void complete(String eventId, Outcome outcome) {
        if (eventId == null || outcome == null) {
            return;
        }
        PendingBarrier pending = pendingBarriers.get(eventId);
        if (pending != null && pending.complete(outcome)) {
            pending.event().getCountDownLatch().countDown();
        }
    }

    public void succeed(String eventId) {
        complete(eventId, Outcome.SUCCESS);
    }

    public void fail(String eventId) {
        complete(eventId, Outcome.FAILED);
    }

    public int activeBarrierCount() {
        return pendingBarriers.size();
    }

    private static final class PendingBarrier {
        private final TapdataCountDownLatchEvent event;
        private final AtomicBoolean completed = new AtomicBoolean();
        private volatile Outcome outcome = Outcome.SUCCESS;

        private PendingBarrier(TapdataCountDownLatchEvent event) {
            this.event = event;
        }

        private boolean complete(Outcome outcome) {
            if (!completed.compareAndSet(false, true)) {
                return false;
            }
            this.outcome = outcome;
            return true;
        }

        private TapdataCountDownLatchEvent event() {
            return event;
        }

        private Outcome outcome() {
            return outcome;
        }
    }
}

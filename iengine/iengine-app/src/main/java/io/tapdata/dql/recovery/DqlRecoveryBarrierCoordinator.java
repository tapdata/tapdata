package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataCountDownLatchEvent;
import com.hazelcast.core.HazelcastInstance;
import io.tapdata.exception.ExceptionUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Converts the source-queue order into a per-event target completion barrier.
 * The DML event is injected by the recovery coordinator first; this class is
 * called immediately afterwards and appends exactly one count-down event.
 */
public final class DqlRecoveryBarrierCoordinator implements DqlRecoveryBarrier {
    private final DqlReplaySourceNode sourceBoundary;
    private final HazelcastInstance hazelcastInstance;
    private final Supplier<Throwable> jobFailureSupplier;
    private final Map<String, PendingBarrier> pendingBarriers = new ConcurrentHashMap<>();

    public DqlRecoveryBarrierCoordinator(DqlReplaySourceNode sourceBoundary) {
        this(sourceBoundary, null);
    }

    public DqlRecoveryBarrierCoordinator(DqlReplaySourceNode sourceBoundary,
                                         HazelcastInstance hazelcastInstance) {
        this(sourceBoundary, hazelcastInstance, () -> null);
    }

    public DqlRecoveryBarrierCoordinator(DqlReplaySourceNode sourceBoundary,
                                         HazelcastInstance hazelcastInstance,
                                         Supplier<Throwable> jobFailureSupplier) {
        this.sourceBoundary = Objects.requireNonNull(sourceBoundary, "sourceBoundary must not be null");
        this.hazelcastInstance = hazelcastInstance;
        this.jobFailureSupplier = Objects.requireNonNull(
                jobFailureSupplier, "jobFailureSupplier must not be null");
    }

    @Override
    public void register(String eventId) {
        validateEventId(eventId);
        PendingBarrier pending = new PendingBarrier();
        if (pendingBarriers.putIfAbsent(eventId, pending) != null) {
            throw new IllegalStateException("recovery barrier already exists for event " + eventId);
        }
        try {
            DqlRecoveryFailureRegistry.register(eventId,
                    failure -> complete(eventId, Outcome.FAILED, failure));
        } catch (RuntimeException exception) {
            pendingBarriers.remove(eventId, pending);
            throw exception;
        }
    }

    @Override
    public Outcome await(String eventId, long timeoutMillis) throws InterruptedException {
        return awaitResult(eventId, timeoutMillis).outcome();
    }

    @Override
    public Result awaitResult(String eventId, long timeoutMillis) throws InterruptedException {
        validateEventId(eventId);
        if (timeoutMillis <= 0L) {
            throw new IllegalArgumentException("recovery barrier timeout must be greater than zero");
        }
        PendingBarrier pending = pendingBarriers.computeIfAbsent(eventId,
                ignored -> new PendingBarrier());
        try {
            sourceBoundary.enqueueBarrier(pending.event());
            if (hazelcastInstance == null) {
                if (!pending.event().getCountDownLatch().await(timeoutMillis, TimeUnit.MILLISECONDS)) {
                    return pending.timeoutResult();
                }
                return pending.result();
            }
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            while (true) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0L) {
                    return pending.timeoutResult();
                }
                long waitMillis = Math.max(1L, Math.min(
                        TimeUnit.NANOSECONDS.toMillis(remainingNanos), 100L));
                if (pending.event().getCountDownLatch().await(waitMillis, TimeUnit.MILLISECONDS)) {
                    return pending.result();
                }
                if (DqlRecoveryBarrierSignalStore.isSignaled(
                        hazelcastInstance, pending.event().getDqlRecoveryBarrierId())) {
                    pending.complete(Outcome.SUCCESS);
                    return pending.result();
                }
            }
        } finally {
            pendingBarriers.remove(eventId, pending);
            DqlRecoveryFailureRegistry.unregister(eventId);
            DqlRecoveryBarrierSignalStore.remove(hazelcastInstance, pending.event().getDqlRecoveryBarrierId());
        }
    }

    /** Called by a processor/target failure path for the matching recovery event. */
    public void complete(String eventId, Outcome outcome) {
        complete(eventId, outcome, null);
    }

    /** Called with the original connector/processor failure when available. */
    public void complete(String eventId, Outcome outcome, Throwable failure) {
        if (eventId == null || outcome == null) {
            return;
        }
        PendingBarrier pending = pendingBarriers.get(eventId);
        if (pending != null && pending.complete(outcome, failure)) {
            pending.event().getCountDownLatch().countDown();
        }
    }

    public void succeed(String eventId) {
        complete(eventId, Outcome.SUCCESS);
    }

    public void fail(String eventId) {
        complete(eventId, Outcome.FAILED);
    }

    public void fail(String eventId, Throwable failure) {
        complete(eventId, Outcome.FAILED, failure);
    }

    @Override
	public void cancel(String eventId) {
		if (eventId == null) {
			return;
		}
		PendingBarrier pending = pendingBarriers.remove(eventId);
		DqlRecoveryFailureRegistry.unregister(eventId);
		if (pending != null) {
			DqlRecoveryBarrierSignalStore.remove(
					hazelcastInstance, pending.event().getDqlRecoveryBarrierId());
		}
	}

    public int activeBarrierCount() {
        return pendingBarriers.size();
    }

    private void validateEventId(String eventId) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("recovery eventId must not be blank");
        }
    }

    private final class PendingBarrier {
        private final TapdataCountDownLatchEvent event;
        private final AtomicBoolean completed = new AtomicBoolean();
        private volatile Outcome outcome = Outcome.SUCCESS;
        private volatile String message;
        private volatile String errorDetails;

        private PendingBarrier() {
            this.event = TapdataCountDownLatchEvent.create(1);
            if (hazelcastInstance != null) {
                this.event.setDqlRecoveryBarrierId(UUID.randomUUID().toString());
            }
        }

        private boolean complete(Outcome outcome) {
            return complete(outcome, null);
        }

        private boolean complete(Outcome outcome, Throwable failure) {
            if (!completed.compareAndSet(false, true)) {
                return false;
            }
            this.outcome = outcome;
            if (failure != null) {
                this.message = failureMessage(failure);
                this.errorDetails = ExceptionUtil.getStackString(failure);
            }
            return true;
        }

        private TapdataCountDownLatchEvent event() {
            return event;
        }

        private Outcome outcome() {
            return outcome;
        }

        private Result result() {
            return new Result(outcome, message, errorDetails);
        }

        private Result result(Outcome fallbackOutcome) {
            if (completed.compareAndSet(false, true)) {
                outcome = fallbackOutcome;
            }
            return result();
        }

        private Result timeoutResult() {
            Throwable jobFailure = jobFailure();
            if (jobFailure != null) {
                complete(Outcome.FAILED, jobFailure);
                return result();
            }
            return result(Outcome.TIMEOUT);
        }

        private Throwable jobFailure() {
            try {
                return jobFailureSupplier.get();
            } catch (RuntimeException ignored) {
                return null;
            }
        }
    }

    private static String failureMessage(Throwable failure) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        String message = null;
        Throwable current = failure;
        while (current != null && visited.add(current)) {
            if (StringUtils.isNotBlank(current.getMessage())) {
                message = current.getMessage();
            }
            current = current.getCause();
        }
        return StringUtils.isBlank(message)
                ? failure.getClass().getSimpleName()
                : message;
    }
}

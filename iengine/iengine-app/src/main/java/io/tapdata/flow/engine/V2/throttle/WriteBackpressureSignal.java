package io.tapdata.flow.engine.V2.throttle;

import com.tapdata.tm.commons.throttle.WaitReason;
import io.tapdata.observable.metric.throttle.MonotonicClock;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Propagates write-side slowness (OUTPUT RATE_WAIT / IO_WAIT) to dependent upstream source nodes
 * so affected sources pause/throttle new reads while allowing independent branches to proceed
 * and in-flight buffers to drain via 80%/60% hysteresis.
 */
public class WriteBackpressureSignal {

    public static final long DEFAULT_WRITE_IO_WAIT_TTL_NANOS = TimeUnit.SECONDS.toNanos(10L);

    private record TargetWaitEntry(WaitReason reason, long markedAtNanos) {
    }

    private final InFlightBudget budget;
    private final MonotonicClock clock;
    private final long writeIoWaitTtlNanos;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition resumeCondition = lock.newCondition();

    /** TargetNodeId -> reason and mark timestamp for write pause. */
    private final Map<String, TargetWaitEntry> waitingTargets = new ConcurrentHashMap<>();
    /** Optional DAG edge dependency: targetNodeId -> sourceNodeIds that feed it. Empty map means global scope. */
    private final Map<String, Set<String>> targetToSourceDeps = new ConcurrentHashMap<>();

    public WriteBackpressureSignal(InFlightBudget budget) {
        this(budget, MonotonicClock.SYSTEM, DEFAULT_WRITE_IO_WAIT_TTL_NANOS);
    }

    public WriteBackpressureSignal(InFlightBudget budget, MonotonicClock clock) {
        this(budget, clock, DEFAULT_WRITE_IO_WAIT_TTL_NANOS);
    }

    public WriteBackpressureSignal(InFlightBudget budget, MonotonicClock clock, long writeIoWaitTtlNanos) {
        this.budget = budget;
        this.clock = clock == null ? MonotonicClock.SYSTEM : clock;
        this.writeIoWaitTtlNanos = Math.max(0L, writeIoWaitTtlNanos);
        if (budget != null) {
            budget.addDrainListener(this::signalDrainProgress);
        }
    }

    public void registerDependency(String sourceNodeId, String targetNodeId) {
        if (sourceNodeId == null || targetNodeId == null) {
            return;
        }
        targetToSourceDeps.computeIfAbsent(targetNodeId, k -> ConcurrentHashMap.newKeySet()).add(sourceNodeId);
    }

    public void markTargetWaiting(String targetNodeId, WaitReason reason) {
        if (targetNodeId == null || reason == null || reason == WaitReason.NONE) {
            clearTargetWaiting(targetNodeId);
            return;
        }
        waitingTargets.put(targetNodeId, new TargetWaitEntry(reason, clock.nanoTime()));
        lock.lock();
        try {
            resumeCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void clearTargetWaiting(String targetNodeId) {
        if (targetNodeId == null) {
            return;
        }
        if (waitingTargets.remove(targetNodeId) != null) {
            signalDrainProgress();
        }
    }

    public void signalDrainProgress() {
        lock.lock();
        try {
            resumeCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void evictExpiredWriteIoWaits() {
        if (writeIoWaitTtlNanos <= 0L || waitingTargets.isEmpty()) {
            return;
        }
        long nowNanos = clock.nanoTime();
        boolean removedAny = false;
        for (Map.Entry<String, TargetWaitEntry> entry : waitingTargets.entrySet()) {
            TargetWaitEntry waitEntry = entry.getValue();
            if (waitEntry != null
                    && waitEntry.reason() == WaitReason.WRITE_IO_WAIT
                    && (nowNanos - waitEntry.markedAtNanos()) >= writeIoWaitTtlNanos) {
                if (waitingTargets.remove(entry.getKey(), waitEntry)) {
                    removedAny = true;
                }
            }
        }
        if (removedAny) {
            signalDrainProgress();
        }
    }

    /**
     * Checks whether the given source node should pause new upstream pulls due to either
     * high-watermark buffer saturation (80%/60% hysteresis) or an actively stalled dependent target.
     */
    public boolean shouldPauseSourcePull(String sourceNodeId) {
        if (budget != null && budget.isHighWatermarkPaused()) {
            return true;
        }
        return isDependentTargetWaiting(sourceNodeId);
    }

    /**
     * Returns whether a downstream target dependent on {@code sourceNodeId} is currently waiting on RATE_WAIT/IO_WAIT.
     */
    public boolean isDependentTargetWaiting(String sourceNodeId) {
        evictExpiredWriteIoWaits();
        if (waitingTargets.isEmpty()) {
            return false;
        }
        if (targetToSourceDeps.isEmpty() || sourceNodeId == null) {
            return true;
        }
        for (String waitingTarget : waitingTargets.keySet()) {
            Set<String> deps = targetToSourceDeps.get(waitingTarget);
            if (deps == null || deps.isEmpty() || deps.contains(sourceNodeId)) {
                return true;
            }
        }
        return false;
    }

    public WaitReason dominantUpstreamReason(String sourceNodeId) {
        if (budget != null && budget.isHighWatermarkPaused()) {
            return WaitReason.BACKPRESSURE_WAIT;
        }
        evictExpiredWriteIoWaits();
        if (waitingTargets.isEmpty()) {
            return WaitReason.NONE;
        }
        WaitReason dominant = WaitReason.NONE;
        for (Map.Entry<String, TargetWaitEntry> e : waitingTargets.entrySet()) {
            TargetWaitEntry entry = e.getValue();
            if (entry == null) {
                continue;
            }
            Set<String> deps = targetToSourceDeps.get(e.getKey());
            if (targetToSourceDeps.isEmpty() || sourceNodeId == null || deps == null || deps.isEmpty() || deps.contains(sourceNodeId)) {
                if (entry.reason() == WaitReason.WRITE_IO_WAIT) {
                    return WaitReason.WRITE_IO_WAIT;
                }
                dominant = entry.reason();
            }
        }
        return dominant;
    }

    public boolean awaitDrainStep(long timeoutMillis) {
        if (timeoutMillis <= 0L) {
            return false;
        }
        lock.lock();
        try {
            return resumeCondition.await(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            lock.unlock();
        }
    }

    public Set<String> getWaitingTargetIds() {
        evictExpiredWriteIoWaits();
        return Collections.unmodifiableSet(new HashSet<>(waitingTargets.keySet()));
    }
}

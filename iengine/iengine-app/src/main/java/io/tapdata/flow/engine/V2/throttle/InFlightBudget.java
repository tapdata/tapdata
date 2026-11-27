package io.tapdata.flow.engine.V2.throttle;

import io.tapdata.observable.metric.throttle.MonotonicClock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-task-runEpoch in-flight capacity ledger enforcing conservative memory, record, and batch bounds
 * alongside the Agent-wide {@link AgentMemoryBudgetPool}. Supports 80%/60% hysteresis watermarks,
 * atomic two-level reservation rollback, prefetch commitments, and epoch-wide invalidation reclamation.
 */
public class InFlightBudget {

    public static final double DEFAULT_HIGH_WATERMARK = 0.80d;
    public static final double DEFAULT_LOW_WATERMARK = 0.60d;
    public static final long DEFAULT_STALE_LEASE_GRACE_NANOS = TimeUnit.SECONDS.toNanos(30L);

    private final TaskRunKey runKey;
    private final AgentMemoryBudgetPool agentPool;
    private final MonotonicClock clock;
    private final long staleLeaseGraceNanos;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition capacityChanged = lock.newCondition();

    private final long maxBytes;
    private final long maxRecords;
    private final long maxBatches;
    private final long drainReserveBytes;
    private final double highWatermarkRatio;
    private final double lowWatermarkRatio;

    private long usedBytes;
    private long committedPrefetchBytes;
    private long committedPrefetchRecords;
    private long committedPrefetchBatches;
    private long usedRecords;
    private long usedBatches;

    private boolean highWatermarkPaused;
    private boolean overflowed;
    private boolean invalidated;

    private final Map<String, CapacityLease> activeLeases = new ConcurrentHashMap<>();
    private final List<Runnable> drainListeners = new ArrayList<>();

    public InFlightBudget(
            TaskRunKey runKey,
            AgentMemoryBudgetPool agentPool,
            long maxBytes,
            long maxRecords,
            long maxBatches,
            long drainReserveBytes) {
        this(runKey, agentPool, maxBytes, maxRecords, maxBatches, drainReserveBytes,
                DEFAULT_HIGH_WATERMARK, DEFAULT_LOW_WATERMARK);
    }

    public InFlightBudget(
            TaskRunKey runKey,
            AgentMemoryBudgetPool agentPool,
            long maxBytes,
            long maxRecords,
            long maxBatches,
            long drainReserveBytes,
            double highWatermarkRatio,
            double lowWatermarkRatio) {
        this(runKey, agentPool, maxBytes, maxRecords, maxBatches, drainReserveBytes,
                highWatermarkRatio, lowWatermarkRatio, MonotonicClock.SYSTEM, DEFAULT_STALE_LEASE_GRACE_NANOS);
    }

    public InFlightBudget(
            TaskRunKey runKey,
            AgentMemoryBudgetPool agentPool,
            long maxBytes,
            long maxRecords,
            long maxBatches,
            long drainReserveBytes,
            double highWatermarkRatio,
            double lowWatermarkRatio,
            MonotonicClock clock,
            long staleLeaseGraceNanos) {
        if (runKey == null || agentPool == null) {
            throw new IllegalArgumentException("runKey and agentPool must not be null");
        }
        if (maxBytes <= 0L || maxRecords <= 0L || maxBatches <= 0L || drainReserveBytes < 0L) {
            throw new IllegalArgumentException("InFlightBudget capacities must be positive");
        }
        if (lowWatermarkRatio < 0.0d || lowWatermarkRatio >= highWatermarkRatio || highWatermarkRatio > 1.0d) {
            throw new IllegalArgumentException("Invalid watermark ratios");
        }
        this.runKey = runKey;
        this.agentPool = agentPool;
        this.clock = clock == null ? MonotonicClock.SYSTEM : clock;
        this.staleLeaseGraceNanos = Math.max(0L, staleLeaseGraceNanos);
        this.maxBytes = maxBytes;
        this.maxRecords = maxRecords;
        this.maxBatches = maxBatches;
        this.drainReserveBytes = Math.min(drainReserveBytes, maxBytes / 2L);
        this.highWatermarkRatio = highWatermarkRatio;
        this.lowWatermarkRatio = lowWatermarkRatio;
    }

    public boolean canEverFit(long estimatedBytes, long records, long batches) {
        return safeAdd(estimatedBytes, drainReserveBytes) <= maxBytes
                && records <= maxRecords
                && batches <= maxBatches
                && agentPool.canPotentiallyFit(estimatedBytes, records, batches);
    }

    public long maxSingleLeaseBytes() {
        long taskCap = Math.max(1L, maxBytes - drainReserveBytes);
        long agentCap = agentPool.maxSingleReservationBytes();
        return Math.max(1L, Math.min(taskCap, agentCap));
    }

    public long maxSingleLeaseRecords() {
        return Math.max(1L, Math.min(maxRecords, agentPool.maxSingleReservationRecords()));
    }

    /**
     * Atomically reserves capacity in both the task budget and the Agent pool.
     * Lock ordering is strictly task-lock -> agent-pool-lock (acyclic).
     * If Agent pool reservation fails, task local usage is immediately rolled back before returning null.
     */
    public CapacityLease tryAcquireLease(
            long estimatedBytes,
            long records,
            long batches,
            String ownerTag,
            boolean registerAgentWaiterOnFail) {
        return tryAcquireLease(estimatedBytes, records, batches, ownerTag, null, 0L, registerAgentWaiterOnFail);
    }

    public CapacityLease tryAcquireLease(
            long estimatedBytes,
            long records,
            long batches,
            String ownerTag,
            String sourceNodeId,
            long sourceBatchSeq,
            boolean registerAgentWaiterOnFail) {
        return tryAcquireLease(estimatedBytes, records, batches, ownerTag, sourceNodeId, sourceBatchSeq, null,
                registerAgentWaiterOnFail);
    }

    public CapacityLease tryAcquireLease(
            long estimatedBytes,
            long records,
            long batches,
            String ownerTag,
            String sourceNodeId,
            long sourceBatchSeq,
            Set<String> expectedTargets,
            boolean registerAgentWaiterOnFail) {
        if (estimatedBytes < 0L || records < 0L || batches < 0L) {
            throw new IllegalArgumentException("Lease amounts must be non-negative");
        }
        lock.lock();
        try {
            if (invalidated || overflowed) {
                return null;
            }
            if (!canFitTaskLocked(estimatedBytes, records, batches)) {
                return null;
            }
            if (!agentPool.tryReserve(runKey.getTaskId(), estimatedBytes, records, batches, false, registerAgentWaiterOnFail)) {
                return null;
            }
            usedBytes = safeAdd(usedBytes, estimatedBytes);
            usedRecords = safeAdd(usedRecords, records);
            usedBatches = safeAdd(usedBatches, batches);
            evaluateWatermarksLocked();

            CapacityLease lease = new CapacityLease(
                    runKey, this, estimatedBytes, records, batches, ownerTag, sourceNodeId, sourceBatchSeq, expectedTargets);
            activeLeases.put(lease.getLeaseId(), lease);
            return lease;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reserves space ahead of issuing an async/paged connector fetch.
     */
    public PrefetchToken tryReservePrefetch(long estimatedBytes, long estimatedRecords, long estimatedBatches, boolean registerWaiter) {
        lock.lock();
        try {
            if (invalidated || overflowed) {
                return null;
            }
            if (!canFitTaskLocked(estimatedBytes, estimatedRecords, estimatedBatches)) {
                return null;
            }
            if (!agentPool.tryReserve(runKey.getTaskId(), estimatedBytes, estimatedRecords, estimatedBatches, true, registerWaiter)) {
                return null;
            }
            committedPrefetchBytes = safeAdd(committedPrefetchBytes, estimatedBytes);
            committedPrefetchRecords = safeAdd(committedPrefetchRecords, estimatedRecords);
            committedPrefetchBatches = safeAdd(committedPrefetchBatches, estimatedBatches);
            usedRecords = safeAdd(usedRecords, estimatedRecords);
            usedBatches = safeAdd(usedBatches, estimatedBatches);
            evaluateWatermarksLocked();
            return new PrefetchToken(estimatedBytes, estimatedRecords, estimatedBatches);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Settles a prefetch reservation with actual materialized metrics. If actual materialization unexpectedly
     * exceeds hard bounds, returns null, frees only the original prefetch commitment, and marks overflowed.
     */
    public CapacityLease settlePrefetchToLease(
            PrefetchToken token,
            long actualBytes,
            long actualRecords,
            long actualBatches,
            String ownerTag) {
        if (token == null || !token.consume()) {
            return null;
        }
        lock.lock();
        try {
            if (invalidated) {
                agentPool.release(token.bytes, token.records, token.batches, true);
                return null;
            }
            long deltaBytes = actualBytes - token.bytes;
            long deltaRecords = actualRecords - token.records;
            long deltaBatches = actualBatches - token.batches;

            long nextUsedBytes = usedBytes + actualBytes;
            long nextPrefetch = Math.max(0L, committedPrefetchBytes - token.bytes);
            long nextRecords = usedRecords + deltaRecords;
            long nextBatches = usedBatches + deltaBatches;

            boolean taskOverflow = (deltaBytes > 0L || deltaRecords > 0L || deltaBatches > 0L)
                    && (safeAdd(safeAdd(nextUsedBytes, nextPrefetch), drainReserveBytes) > maxBytes
                    || nextRecords > maxRecords
                    || nextBatches > maxBatches);
            if (taskOverflow || !agentPool.settlePrefetch(
                    runKey.getTaskId(),
                    token.bytes, token.records, token.batches,
                    actualBytes, actualRecords, actualBatches)) {
                // Unexpected materialization overflow: release prefetch slot and block further admissions.
                committedPrefetchBytes = Math.max(0L, committedPrefetchBytes - token.bytes);
                committedPrefetchRecords = Math.max(0L, committedPrefetchRecords - token.records);
                committedPrefetchBatches = Math.max(0L, committedPrefetchBatches - token.batches);
                usedRecords = Math.max(0L, usedRecords - token.records);
                usedBatches = Math.max(0L, usedBatches - token.batches);
                agentPool.release(token.bytes, token.records, token.batches, true);
                overflowed = true;
                evaluateWatermarksLocked();
                capacityChanged.signalAll();
                return null;
            }
            committedPrefetchBytes = nextPrefetch;
            committedPrefetchRecords = Math.max(0L, committedPrefetchRecords - token.records);
            committedPrefetchBatches = Math.max(0L, committedPrefetchBatches - token.batches);
            usedBytes = nextUsedBytes;
            usedRecords = Math.max(0L, nextRecords);
            usedBatches = Math.max(0L, nextBatches);
            evaluateWatermarksLocked();

            CapacityLease lease = new CapacityLease(runKey, this, actualBytes, actualRecords, actualBatches, ownerTag);
            activeLeases.put(lease.getLeaseId(), lease);
            return lease;
        } finally {
            lock.unlock();
        }
    }

    public void releaseUnsettledPrefetch(PrefetchToken token) {
        if (token == null || !token.consume()) {
            return;
        }
        lock.lock();
        try {
            committedPrefetchBytes = Math.max(0L, committedPrefetchBytes - token.bytes);
            committedPrefetchRecords = Math.max(0L, committedPrefetchRecords - token.records);
            committedPrefetchBatches = Math.max(0L, committedPrefetchBatches - token.batches);
            usedRecords = Math.max(0L, usedRecords - token.records);
            usedBatches = Math.max(0L, usedBatches - token.batches);
            agentPool.release(token.bytes, token.records, token.batches, true);
            evaluateWatermarksLocked();
            capacityChanged.signalAll();
        } finally {
            lock.unlock();
        }
        notifyDrainListeners();
    }

    /**
     * Reserves incremental memory for real copies / fan-out expansion without disturbing source leases.
     */
    public CapacityLease tryReserveIncrementalCopy(long extraBytes, long extraRecords, long extraBatches, String ownerTag) {
        return tryAcquireLease(extraBytes, extraRecords, extraBatches, ownerTag, false);
    }

    boolean ackLeaseEvents(CapacityLease lease, String targetNodeId, long completedEvents) {
        if (lease == null || completedEvents <= 0L) {
            return false;
        }
        return settleLeaseMutation(lease, () -> lease.ackEventsLocked(targetNodeId, completedEvents));
    }

    boolean releaseLease(CapacityLease lease, long expectedHolderVersion) {
        if (lease == null) {
            return false;
        }
        return settleLeaseMutation(lease, () -> lease.releaseLocked(expectedHolderVersion));
    }

    boolean releaseLeaseBySharedRef(CapacityLease lease, long pinnedHolderVersion) {
        if (lease == null) {
            return false;
        }
        return settleLeaseMutation(lease, () -> lease.releaseBySharedRefLocked(pinnedHolderVersion));
    }

    private boolean settleLeaseMutation(CapacityLease lease, java.util.function.Supplier<long[]> leaseUpdater) {
        boolean notify = false;
        lock.lock();
        try {
            if (!activeLeases.containsKey(lease.getLeaseId())) {
                return false;
            }
            long[] delta = leaseUpdater.get();
            if (delta == null) {
                return false;
            }
            long bytes = Math.max(0L, delta[0]);
            long records = Math.max(0L, delta[1]);
            long batches = Math.max(0L, delta[2]);
            boolean fullRelease = delta.length > 3 && delta[3] != 0L;
            if (fullRelease) {
                activeLeases.remove(lease.getLeaseId());
            }
            if (bytes > 0L || records > 0L || batches > 0L || fullRelease) {
                usedBytes = Math.max(0L, usedBytes - bytes);
                usedRecords = Math.max(0L, usedRecords - records);
                usedBatches = Math.max(0L, usedBatches - batches);
                agentPool.release(bytes, records, batches, false);
                evaluateWatermarksLocked();
                capacityChanged.signalAll();
                notify = true;
            }
        } finally {
            lock.unlock();
        }
        if (notify) {
            notifyDrainListeners();
        }
        return true;
    }

    public long getStaleLeaseGraceNanos() {
        return staleLeaseGraceNanos;
    }

    /**
     * Reclaims residual leases from earlier source batches only after they remain older than
     * {@code latestObservedSeq} across the configured grace window, protecting concurrent retries/fan-out.
     */
    public int flushOlderLeasesForSource(String sourceNodeId, long latestObservedSeq) {
        if (sourceNodeId == null || latestObservedSeq <= 1L) {
            return 0;
        }
        return flushLeasesForSourceUpTo(sourceNodeId, latestObservedSeq - 1L);
    }

    /**
     * Reclaims residual leases for {@code sourceNodeId} with {@code sourceBatchSeq <= maxSeqInclusive}
     * once they stay unresolved past {@link #staleLeaseGraceNanos}, preventing permanent stalls on filtered
     * or silent fan-out branches.
     */
    public int flushStaleLeasesUpTo(String sourceNodeId, long maxSeqInclusive) {
        if (sourceNodeId == null || maxSeqInclusive <= 0L) {
            return 0;
        }
        return flushLeasesForSourceUpTo(sourceNodeId, maxSeqInclusive);
    }

    private int flushLeasesForSourceUpTo(String sourceNodeId, long maxSeqInclusive) {
        int reclaimedCount = 0;
        long nowNanos = clock.nanoTime();
        lock.lock();
        try {
            for (CapacityLease lease : new ArrayList<>(activeLeases.values())) {
                if (sourceNodeId.equals(lease.getSourceNodeId())
                        && lease.getSourceBatchSeq() > 0L
                        && lease.getSourceBatchSeq() <= maxSeqInclusive) {
                    long observedAt = lease.getOrMarkObservedBehindNanos(nowNanos);
                    if ((nowNanos - observedAt) < staleLeaseGraceNanos) {
                        continue;
                    }
                    long[] rem = lease.forceReclaimRemaining();
                    if (rem != null) {
                        activeLeases.remove(lease.getLeaseId());
                        usedBytes = Math.max(0L, usedBytes - rem[0]);
                        usedRecords = Math.max(0L, usedRecords - rem[1]);
                        usedBatches = Math.max(0L, usedBatches - rem[2]);
                        agentPool.release(rem[0], rem[1], rem[2], false);
                        reclaimedCount++;
                    }
                }
            }
            if (reclaimedCount > 0) {
                evaluateWatermarksLocked();
                capacityChanged.signalAll();
            }
        } finally {
            lock.unlock();
        }
        if (reclaimedCount > 0) {
            notifyDrainListeners();
        }
        return reclaimedCount;
    }

    public CapacityLease getLease(String leaseId) {
        return leaseId == null ? null : activeLeases.get(leaseId);
    }

    public boolean releaseLeaseById(String leaseId) {
        CapacityLease lease = getLease(leaseId);
        return lease != null && lease.release(lease.getHolderVersion());
    }

    /**
     * Reclaims every active lease held under this runEpoch and prevents any future lease admissions.
     */
    public void invalidateAndReclaimAll() {
        lock.lock();
        try {
            invalidated = true;
            for (CapacityLease lease : new ArrayList<>(activeLeases.values())) {
                long[] rem = lease.forceReclaimRemaining();
                if (rem != null) {
                    activeLeases.remove(lease.getLeaseId());
                    usedBytes = Math.max(0L, usedBytes - rem[0]);
                    usedRecords = Math.max(0L, usedRecords - rem[1]);
                    usedBatches = Math.max(0L, usedBatches - rem[2]);
                    agentPool.release(rem[0], rem[1], rem[2], false);
                }
            }
            if (committedPrefetchBytes > 0L || committedPrefetchRecords > 0L || committedPrefetchBatches > 0L) {
                agentPool.release(committedPrefetchBytes, committedPrefetchRecords, committedPrefetchBatches, true);
                committedPrefetchBytes = 0L;
                committedPrefetchRecords = 0L;
                committedPrefetchBatches = 0L;
            }
            agentPool.cancelWaiter(runKey.getTaskId());
            usedBytes = 0L;
            usedRecords = 0L;
            usedBatches = 0L;
            highWatermarkPaused = false;
            capacityChanged.signalAll();
        } finally {
            lock.unlock();
        }
        notifyDrainListeners();
    }

    public boolean isHighWatermarkPaused() {
        lock.lock();
        try {
            return highWatermarkPaused;
        } finally {
            lock.unlock();
        }
    }

    public double currentWatermarkRatio() {
        lock.lock();
        try {
            return maxRatioLocked();
        } finally {
            lock.unlock();
        }
    }

    public boolean isOverflowed() {
        lock.lock();
        try {
            return overflowed;
        } finally {
            lock.unlock();
        }
    }

    public boolean isInvalidated() {
        lock.lock();
        try {
            return invalidated;
        } finally {
            lock.unlock();
        }
    }

    public Snapshot snapshot() {
        lock.lock();
        try {
            return new Snapshot(
                    usedBytes,
                    committedPrefetchBytes,
                    usedRecords,
                    usedBatches,
                    maxBytes,
                    maxRecords,
                    maxBatches,
                    activeLeases.size(),
                    highWatermarkPaused,
                    overflowed,
                    invalidated,
                    maxRatioLocked()
            );
        } finally {
            lock.unlock();
        }
    }

    public void addDrainListener(Runnable listener) {
        if (listener == null) {
            return;
        }
        synchronized (drainListeners) {
            drainListeners.add(listener);
        }
    }

    public boolean awaitCapacityOrDrain(long waitMillis) throws InterruptedException {
        if (waitMillis <= 0L) {
            return false;
        }
        lock.lock();
        try {
            if (invalidated) {
                return true;
            }
            return capacityChanged.await(waitMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    public void cancelWaiting() {
        agentPool.cancelWaiter(runKey.getTaskId());
        lock.lock();
        try {
            capacityChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void notifyDrainListeners() {
        List<Runnable> copy;
        synchronized (drainListeners) {
            if (drainListeners.isEmpty()) {
                return;
            }
            copy = new ArrayList<>(drainListeners);
        }
        for (Runnable listener : copy) {
            listener.run();
        }
    }

    private boolean canFitTaskLocked(long bytes, long records, long batches) {
        long nextBytes = safeAdd(safeAdd(usedBytes, committedPrefetchBytes), safeAdd(bytes, drainReserveBytes));
        long nextRecords = safeAdd(usedRecords, records);
        long nextBatches = safeAdd(usedBatches, batches);
        return nextBytes <= maxBytes && nextRecords <= maxRecords && nextBatches <= maxBatches;
    }

    private void evaluateWatermarksLocked() {
        double bytesRatio = (double) safeAdd(usedBytes, committedPrefetchBytes) / Math.max(1L, maxBytes);
        double recRatio = (double) usedRecords / Math.max(1L, maxRecords);
        double batchRatio = (double) usedBatches / Math.max(1L, maxBatches);
        double peak = Math.max(bytesRatio, Math.max(recRatio, batchRatio));

        if (!highWatermarkPaused) {
            if (peak >= highWatermarkRatio) {
                highWatermarkPaused = true;
            }
        } else {
            if (bytesRatio <= lowWatermarkRatio && recRatio <= lowWatermarkRatio && batchRatio <= lowWatermarkRatio) {
                highWatermarkPaused = false;
            }
        }
    }

    private double maxRatioLocked() {
        double bytesRatio = (double) safeAdd(usedBytes, committedPrefetchBytes) / Math.max(1L, maxBytes);
        double recRatio = (double) usedRecords / Math.max(1L, maxRecords);
        double batchRatio = (double) usedBatches / Math.max(1L, maxBatches);
        return Math.min(1.0d, Math.max(bytesRatio, Math.max(recRatio, batchRatio)));
    }

    private static long safeAdd(long a, long b) {
        try {
            return Math.addExact(a, b);
        } catch (ArithmeticException ex) {
            return Long.MAX_VALUE;
        }
    }

    public static final class PrefetchToken {
        private final long bytes;
        private final long records;
        private final long batches;
        private boolean consumed;

        private PrefetchToken(long bytes, long records, long batches) {
            this.bytes = bytes;
            this.records = records;
            this.batches = batches;
        }

        private synchronized boolean consume() {
            if (consumed) {
                return false;
            }
            consumed = true;
            return true;
        }
    }

    public record Snapshot(
            long usedBytes,
            long committedPrefetchBytes,
            long usedRecords,
            long usedBatches,
            long maxBytes,
            long maxRecords,
            long maxBatches,
            int activeLeaseCount,
            boolean highWatermarkPaused,
            boolean overflowed,
            boolean invalidated,
            double maxRatio
    ) {
    }
}

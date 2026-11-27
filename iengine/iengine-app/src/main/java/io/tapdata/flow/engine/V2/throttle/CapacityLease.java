package io.tapdata.flow.engine.V2.throttle;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Capacity lease binding reserved bytes/records/batches to a specific task runEpoch, owner, and holder version.
 * Supports single-owner handoff (with holderVersion fencing), shared fan-out reference counting,
 * multi-target fan-out proportional ack tracking, single-lock-order budget settlement, and idempotent release.
 */
public final class CapacityLease {

    private final String leaseId;
    private final TaskRunKey runKey;
    private final InFlightBudget budget;
    private final long initialBytes;
    private final long initialRecords;
    private final long initialBatches;
    private final String sourceNodeId;
    private final long sourceBatchSeq;
    private final Set<String> expectedTargets;
    private final Map<String, Long> ackedByTarget;

    private long committedFanOutEvents;
    private long observedBehindAtNanos;
    private long remainingBytes;
    private long remainingRecords;
    private long remainingBatches;
    private String ownerTag;
    private long holderVersion;
    private int activeRefCount;
    private boolean released;

    CapacityLease(
            TaskRunKey runKey,
            InFlightBudget budget,
            long reservedBytes,
            long reservedRecords,
            long reservedBatches,
            String ownerTag) {
        this(runKey, budget, reservedBytes, reservedRecords, reservedBatches, ownerTag, null, 0L, null);
    }

    CapacityLease(
            TaskRunKey runKey,
            InFlightBudget budget,
            long reservedBytes,
            long reservedRecords,
            long reservedBatches,
            String ownerTag,
            String sourceNodeId,
            long sourceBatchSeq) {
        this(runKey, budget, reservedBytes, reservedRecords, reservedBatches, ownerTag, sourceNodeId, sourceBatchSeq, null);
    }

    CapacityLease(
            TaskRunKey runKey,
            InFlightBudget budget,
            long reservedBytes,
            long reservedRecords,
            long reservedBatches,
            String ownerTag,
            String sourceNodeId,
            long sourceBatchSeq,
            Set<String> expectedTargets) {
        this.leaseId = UUID.randomUUID().toString();
        this.runKey = runKey;
        this.budget = budget;
        this.initialBytes = Math.max(0L, reservedBytes);
        this.initialRecords = Math.max(0L, reservedRecords);
        this.initialBatches = Math.max(0L, reservedBatches);
        this.remainingBytes = this.initialBytes;
        this.remainingRecords = this.initialRecords;
        this.remainingBatches = this.initialBatches;
        this.sourceNodeId = sourceNodeId;
        this.sourceBatchSeq = sourceBatchSeq;
        if (expectedTargets != null && expectedTargets.size() > 1) {
            this.expectedTargets = Collections.unmodifiableSet(new HashSet<>(expectedTargets));
            this.ackedByTarget = new HashMap<>(expectedTargets.size());
        } else {
            this.expectedTargets = Collections.emptySet();
            this.ackedByTarget = null;
        }
        this.ownerTag = ownerTag == null ? "source" : ownerTag;
        this.holderVersion = 1L;
        this.activeRefCount = 1;
        this.released = false;
    }

    public String getLeaseId() {
        return leaseId;
    }

    public TaskRunKey getRunKey() {
        return runKey;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
    }

    public long getSourceBatchSeq() {
        return sourceBatchSeq;
    }

    public long getReservedBytes() {
        return initialBytes;
    }

    public long getReservedRecords() {
        return initialRecords;
    }

    public long getReservedBatches() {
        return initialBatches;
    }

    public synchronized long getRemainingBytes() {
        return remainingBytes;
    }

    public synchronized long getRemainingRecords() {
        return remainingRecords;
    }

    public synchronized long getRemainingBatches() {
        return remainingBatches;
    }

    public synchronized String getOwnerTag() {
        return ownerTag;
    }

    public synchronized long getHolderVersion() {
        return holderVersion;
    }

    public synchronized int getActiveRefCount() {
        return activeRefCount;
    }

    public synchronized boolean isReleased() {
        return released;
    }

    /**
     * Atomically transfers this lease across threads/queues without reserving duplicate budget.
     *
     * @return new holderVersion on success, or -1 if already released or expectedHolderVersion is stale.
     */
    public synchronized long transferOwnership(long expectedHolderVersion, String newOwnerTag) {
        if (released || holderVersion != expectedHolderVersion) {
            return -1L;
        }
        this.ownerTag = newOwnerTag == null ? this.ownerTag : newOwnerTag;
        this.holderVersion++;
        return this.holderVersion;
    }

    /**
     * Acquires a shared reference handle for zero-copy fan-out branches.
     * Underlying budget is returned only once when the last shared reference is released.
     */
    public synchronized SharedRef retainSharedReference(long expectedHolderVersion, String branchOwner) {
        if (released || holderVersion != expectedHolderVersion) {
            return null;
        }
        activeRefCount++;
        return new SharedRef(this, expectedHolderVersion, branchOwner);
    }

    /**
     * Releases proportional records and bytes when part of a source batch completes at downstream targets.
     * Executes under {@link InFlightBudget}'s single lock to eliminate cross-lock races and handles
     * multi-target fan-out watermarks when multiple downstream targets consume the same source lease.
     */
    public boolean ackProcessedEvents(long completedEvents) {
        return ackProcessedEvents(null, completedEvents);
    }

    public boolean ackProcessedEvents(String targetNodeId, long completedEvents) {
        if (completedEvents <= 0L) {
            return false;
        }
        return budget.ackLeaseEvents(this, targetNodeId, completedEvents);
    }

    /**
     * Releases the primary lease (or one shared ref count) only if {@code expectedHolderVersion} still matches.
     * Old-owner late callbacks with a stale holderVersion are safely rejected without disturbing current reservations.
     */
    public boolean release(long expectedHolderVersion) {
        return budget.releaseLease(this, expectedHolderVersion);
    }

    synchronized long[] ackEventsLocked(String targetNodeId, long completedEvents) {
        if (released || activeRefCount > 1 || completedEvents <= 0L) {
            return null;
        }
        long effectiveDelta = completedEvents;
        if (!expectedTargets.isEmpty()) {
            if (targetNodeId == null || !expectedTargets.contains(targetNodeId)) {
                return null;
            }
            long prevForTarget = ackedByTarget.getOrDefault(targetNodeId, 0L);
            long nextForTarget = Math.min(initialRecords, prevForTarget + completedEvents);
            ackedByTarget.put(targetNodeId, nextForTarget);

            long minAcrossTargets = initialRecords;
            for (String target : expectedTargets) {
                long acked = ackedByTarget.getOrDefault(target, 0L);
                if (acked < minAcrossTargets) {
                    minAcrossTargets = acked;
                }
            }
            effectiveDelta = Math.max(0L, minAcrossTargets - committedFanOutEvents);
            committedFanOutEvents += effectiveDelta;
            if (effectiveDelta == 0L) {
                return new long[]{0L, 0L, 0L, 0L};
            }
        }

        long releaseBytes;
        long releaseRecords;
        long releaseBatches;
        long fullRelease;
        if (remainingRecords <= effectiveDelta) {
            releaseBytes = remainingBytes;
            releaseRecords = remainingRecords;
            releaseBatches = remainingBatches;
            remainingBytes = 0L;
            remainingRecords = 0L;
            remainingBatches = 0L;
            activeRefCount = 0;
            released = true;
            fullRelease = 1L;
        } else {
            releaseRecords = effectiveDelta;
            releaseBytes = (remainingBytes * releaseRecords) / Math.max(1L, remainingRecords);
            releaseBatches = 0L;
            remainingRecords -= releaseRecords;
            remainingBytes -= releaseBytes;
            fullRelease = 0L;
        }
        return new long[]{releaseBytes, releaseRecords, releaseBatches, fullRelease};
    }

    synchronized long[] releaseLocked(long expectedHolderVersion) {
        if (released || holderVersion != expectedHolderVersion) {
            return null;
        }
        activeRefCount--;
        if (activeRefCount <= 0) {
            long[] rem = new long[]{remainingBytes, remainingRecords, remainingBatches, 1L};
            remainingBytes = 0L;
            remainingRecords = 0L;
            remainingBatches = 0L;
            released = true;
            return rem;
        }
        return new long[]{0L, 0L, 0L, 0L};
    }

    synchronized long getOrMarkObservedBehindNanos(long nowNanos) {
        if (observedBehindAtNanos == 0L) {
            observedBehindAtNanos = nowNanos;
        }
        return observedBehindAtNanos;
    }

    /**
     * Force-reclaims remaining budget held by this lease upon invalidation, stop, or sequence sweep.
     * Returns an array {bytes, records, batches} or null if already released.
     */
    synchronized long[] forceReclaimRemaining() {
        if (released) {
            return null;
        }
        long[] rem = new long[]{remainingBytes, remainingRecords, remainingBatches};
        remainingBytes = 0L;
        remainingRecords = 0L;
        remainingBatches = 0L;
        released = true;
        activeRefCount = 0;
        holderVersion++;
        return rem;
    }

    /**
     * Idempotent shared reference handle for multi-branch downstream sharing of one underlying lease.
     */
    public static final class SharedRef {
        private final CapacityLease parent;
        private final long pinnedHolderVersion;
        private final String branchOwner;
        private boolean refReleased;

        private SharedRef(CapacityLease parent, long pinnedHolderVersion, String branchOwner) {
            this.parent = parent;
            this.pinnedHolderVersion = pinnedHolderVersion;
            this.branchOwner = branchOwner;
        }

        public String getBranchOwner() {
            return branchOwner;
        }

        public synchronized boolean isRefReleased() {
            return refReleased;
        }

        public boolean release() {
            synchronized (this) {
                if (refReleased) {
                    return false;
                }
                refReleased = true;
            }
            return parent.releaseBySharedRef(pinnedHolderVersion);
        }
    }

    private boolean releaseBySharedRef(long pinnedHolderVersion) {
        return budget.releaseLeaseBySharedRef(this, pinnedHolderVersion);
    }

    synchronized long[] releaseBySharedRefLocked(long pinnedHolderVersion) {
        if (released || holderVersion < pinnedHolderVersion) {
            return null;
        }
        activeRefCount--;
        if (activeRefCount <= 0) {
            long[] rem = new long[]{remainingBytes, remainingRecords, remainingBatches, 1L};
            remainingBytes = 0L;
            remainingRecords = 0L;
            remainingBatches = 0L;
            released = true;
            return rem;
        }
        return new long[]{0L, 0L, 0L, 0L};
    }
}

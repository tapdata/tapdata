package io.tapdata.flow.engine.V2.throttle;

import io.tapdata.observable.metric.throttle.MonotonicClock;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Agent-wide JVM memory, record, and batch budget pool shared by all active throttled/monitored tasks.
 * Ensures aggregate reserved in-flight capacity plus committed prefetch stays strictly within heap safety headroom.
 */
public class AgentMemoryBudgetPool {

    private static final long MIN_DEFAULT_POOL_BYTES = 64L * 1024L * 1024L;
    private static final long MAX_DEFAULT_POOL_BYTES = 512L * 1024L * 1024L;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition releasedCondition = lock.newCondition();
    private final MonotonicClock clock;

    private volatile long maxMemoryBytes;
    private volatile long maxRecords;
    private volatile long maxBatches;
    private volatile long drainReserveBytes;

    private long occupiedBytes;
    private long committedPrefetchBytes;
    private long occupiedRecords;
    private long occupiedBatches;

    /** Round-robin waiting queue: at most one entry per taskId. */
    private final Deque<String> waitingTasks = new ArrayDeque<>();
    private final Set<String> waitingTaskSet = new HashSet<>();

    public AgentMemoryBudgetPool() {
        this(defaultPoolBytes(), 2_000_000L, 100_000L, 4L * 1024L * 1024L, MonotonicClock.SYSTEM);
    }

    public AgentMemoryBudgetPool(
            long maxMemoryBytes,
            long maxRecords,
            long maxBatches,
            long drainReserveBytes,
            MonotonicClock clock) {
        if (maxMemoryBytes <= 0L || maxRecords <= 0L || maxBatches <= 0L || drainReserveBytes < 0L) {
            throw new IllegalArgumentException("Invalid agent memory pool bounds");
        }
        this.maxMemoryBytes = maxMemoryBytes;
        this.maxRecords = maxRecords;
        this.maxBatches = maxBatches;
        this.drainReserveBytes = Math.min(drainReserveBytes, maxMemoryBytes / 4L);
        this.clock = clock == null ? MonotonicClock.SYSTEM : clock;
    }

    private static long defaultPoolBytes() {
        long jvmMax = Runtime.getRuntime().maxMemory();
        if (jvmMax <= 0L || jvmMax == Long.MAX_VALUE) {
            return MAX_DEFAULT_POOL_BYTES;
        }
        long candidate = jvmMax / 4L;
        return Math.max(MIN_DEFAULT_POOL_BYTES, Math.min(MAX_DEFAULT_POOL_BYTES, candidate));
    }

    /**
     * Try reserving additional capacity from the Agent pool. If false, nothing is charged and optionally
     * registers the calling task as the single waiting entry in the fair round-robin queue.
     */
    public boolean tryReserve(String taskId, long bytes, long records, long batches, boolean asPrefetch, boolean registerWaiter) {
        if (bytes < 0L || records < 0L || batches < 0L) {
            throw new IllegalArgumentException("Reservation values must be non-negative");
        }
        lock.lock();
        try {
            if (!isTurnEligible(taskId)) {
                if (registerWaiter && taskId != null) {
                    enqueueWaiter(taskId);
                }
                return false;
            }
            if (!canFitLocked(bytes, records, batches)) {
                if (registerWaiter && taskId != null) {
                    enqueueWaiter(taskId);
                    if (waitingTasks.size() > 1 && taskId.equals(waitingTasks.peekFirst())) {
                        waitingTasks.pollFirst();
                        waitingTasks.offerLast(taskId);
                        releasedCondition.signalAll();
                    }
                }
                return false;
            }
            if (asPrefetch) {
                committedPrefetchBytes = safeAdd(committedPrefetchBytes, bytes);
            } else {
                occupiedBytes = safeAdd(occupiedBytes, bytes);
            }
            occupiedRecords = safeAdd(occupiedRecords, records);
            occupiedBatches = safeAdd(occupiedBatches, batches);
            if (taskId != null && waitingTaskSet.remove(taskId)) {
                waitingTasks.remove(taskId);
                rotateIfNecessary();
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Converts part/all of an existing prefetch reservation into occupied lease memory.
     */
    public boolean settlePrefetch(
            String taskId,
            long prefetchBytes,
            long prefetchRecords,
            long prefetchBatches,
            long actualBytes,
            long actualRecords,
            long actualBatches) {
        lock.lock();
        try {
            long deltaBytes = actualBytes - prefetchBytes;
            long deltaRecords = actualRecords - prefetchRecords;
            long deltaBatches = actualBatches - prefetchBatches;

            long nextOccupied = occupiedBytes + actualBytes;
            long nextPrefetch = Math.max(0L, committedPrefetchBytes - prefetchBytes);
            long nextRecords = occupiedRecords + deltaRecords;
            long nextBatches = occupiedBatches + deltaBatches;

            if (deltaBytes > 0L || deltaRecords > 0L || deltaBatches > 0L) {
                if (nextOccupied + nextPrefetch + drainReserveBytes > maxMemoryBytes
                        || nextRecords > maxRecords
                        || nextBatches > maxBatches) {
                    return false;
                }
            }
            committedPrefetchBytes = nextPrefetch;
            occupiedBytes = Math.max(0L, nextOccupied);
            occupiedRecords = Math.max(0L, nextRecords);
            occupiedBatches = Math.max(0L, nextBatches);
            if (taskId != null) {
                cancelWaiterLocked(taskId);
            }
            releasedCondition.signalAll();
            return true;
        } finally {
            lock.unlock();
        }
    }

    public void release(long bytes, long records, long batches, boolean wasPrefetch) {
        if (bytes <= 0L && records <= 0L && batches <= 0L) {
            return;
        }
        lock.lock();
        try {
            if (wasPrefetch) {
                committedPrefetchBytes = Math.max(0L, committedPrefetchBytes - Math.max(0L, bytes));
            } else {
                occupiedBytes = Math.max(0L, occupiedBytes - Math.max(0L, bytes));
            }
            occupiedRecords = Math.max(0L, occupiedRecords - Math.max(0L, records));
            occupiedBatches = Math.max(0L, occupiedBatches - Math.max(0L, batches));
            releasedCondition.signalAll();
            releasedCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void cancelWaiter(String taskId) {
        if (taskId == null) {
            return;
        }
        lock.lock();
        try {
            cancelWaiterLocked(taskId);
            releasedCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public boolean awaitCapacityChange(long waitMillis) throws InterruptedException {
        if (waitMillis <= 0L) {
            return false;
        }
        lock.lock();
        try {
            return releasedCondition.await(waitMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    public void skipTurnAndYield(String taskId) {
        if (taskId == null) {
            return;
        }
        lock.lock();
        try {
            if (waitingTaskSet.contains(taskId) && waitingTasks.size() > 1 && taskId.equals(waitingTasks.peekFirst())) {
                waitingTasks.pollFirst();
                waitingTasks.offerLast(taskId);
                releasedCondition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean canPotentiallyFit(long bytes, long records, long batches) {
        lock.lock();
        try {
            return bytes + drainReserveBytes <= maxMemoryBytes
                    && records <= maxRecords
                    && batches <= maxBatches;
        } finally {
            lock.unlock();
        }
    }

    public long maxSingleReservationBytes() {
        lock.lock();
        try {
            return Math.max(1L, maxMemoryBytes - drainReserveBytes);
        } finally {
            lock.unlock();
        }
    }

    public long maxSingleReservationRecords() {
        lock.lock();
        try {
            return Math.max(1L, maxRecords);
        } finally {
            lock.unlock();
        }
    }

    public Snapshot snapshot() {
        lock.lock();
        try {
            return new Snapshot(
                    maxMemoryBytes,
                    drainReserveBytes,
                    occupiedBytes,
                    committedPrefetchBytes,
                    occupiedRecords,
                    occupiedBatches,
                    maxRecords,
                    maxBatches,
                    waitingTasks.size()
            );
        } finally {
            lock.unlock();
        }
    }

    public void reconfigure(long maxMemoryBytes, long maxRecords, long maxBatches, long drainReserveBytes) {
        if (maxMemoryBytes <= 0L || maxRecords <= 0L || maxBatches <= 0L || drainReserveBytes < 0L) {
            throw new IllegalArgumentException("Invalid agent memory pool bounds");
        }
        lock.lock();
        try {
            this.maxMemoryBytes = maxMemoryBytes;
            this.maxRecords = maxRecords;
            this.maxBatches = maxBatches;
            this.drainReserveBytes = drainReserveBytes;
            releasedCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private boolean isTurnEligible(String taskId) {
        if (taskId == null || waitingTasks.isEmpty()) {
            return true;
        }
        if (!waitingTaskSet.contains(taskId)) {
            // Do not allow bypassing when other tasks are queued waiting for freed capacity.
            return false;
        }
        return taskId.equals(waitingTasks.peekFirst());
    }

    private void enqueueWaiter(String taskId) {
        if (waitingTaskSet.add(taskId)) {
            waitingTasks.offerLast(taskId);
        }
    }

    private void cancelWaiterLocked(String taskId) {
        if (waitingTaskSet.remove(taskId)) {
            waitingTasks.remove(taskId);
        }
    }

    private void rotateIfNecessary() {
        if (waitingTasks.size() > 1) {
            releasedCondition.signalAll();
        }
    }

    private boolean canFitLocked(long bytes, long records, long batches) {
        long nextBytes = safeAdd(safeAdd(occupiedBytes, committedPrefetchBytes), safeAdd(bytes, drainReserveBytes));
        long nextRecords = safeAdd(occupiedRecords, records);
        long nextBatches = safeAdd(occupiedBatches, batches);
        return nextBytes <= maxMemoryBytes && nextRecords <= maxRecords && nextBatches <= maxBatches;
    }

    private static long safeAdd(long a, long b) {
        try {
            return Math.addExact(a, b);
        } catch (ArithmeticException ex) {
            return Long.MAX_VALUE;
        }
    }

    public record Snapshot(
            long maxMemoryBytes,
            long drainReserveBytes,
            long occupiedBytes,
            long committedPrefetchBytes,
            long occupiedRecords,
            long occupiedBatches,
            long maxRecords,
            long maxBatches,
            int waitingTaskCount
    ) {
        public long totalReservedBytes() {
            return occupiedBytes + committedPrefetchBytes;
        }
    }
}

package io.tapdata.flow.engine.V2.throttle;

import com.tapdata.tm.commons.throttle.TaskThrottleApplyReceipt;
import com.tapdata.tm.commons.throttle.TaskThrottlePolicyDto;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import io.tapdata.observable.metric.throttle.MonotonicClock;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * Dual-dimension (BYTES & QPS) integer token bucket controller with fixed 4-lock ordering
 * (FULL+INPUT -> FULL+OUTPUT -> CDC+INPUT -> CDC+OUTPUT), bounded atomic overdraft (<= 5s),
 * 1-second fair priority reservation without partial holding, and atomic hot-update debt preservation.
 */
public class TaskThrottleController {

    private static final long ONE_SECOND_NANOS = 1_000_000_000L;
    private static final BigInteger ONE_SECOND_BI = BigInteger.valueOf(ONE_SECOND_NANOS);
    private static final BigInteger MAX_LONG_BI = BigInteger.valueOf(Long.MAX_VALUE);
    public static final long DEFAULT_FAIR_PRIORITY_WAIT_NANOS = ONE_SECOND_NANOS;

    private final TaskRunKey runKey;
    private final MonotonicClock clock;
    private final long maxAtomicSeconds;
    private final long fairPriorityWaitNanos;
    private final AtomicLong candidateSeq = new AtomicLong(1L);

    /** Four ordered slot locks: order = phase.ordinal() * 2 + direction.ordinal(). */
    private final SlotState[] slots = new SlotState[4];
    private final ReentrantLock policyPublishLock = new ReentrantLock();
    private final ReentrantLock waiterSignalLock = new ReentrantLock();
    private final Condition waiterSignal = waiterSignalLock.newCondition();

    private volatile PolicySnapshot activeSnapshot;
    private volatile boolean stopped;

    public TaskThrottleController(TaskRunKey runKey, MonotonicClock clock) {
        this(runKey, clock, ThrottleConstants.DEFAULT_MAX_ATOMIC_SECONDS, DEFAULT_FAIR_PRIORITY_WAIT_NANOS);
    }

    public TaskThrottleController(
            TaskRunKey runKey,
            MonotonicClock clock,
            long maxAtomicSeconds,
            long fairPriorityWaitNanos) {
        if (runKey == null) {
            throw new IllegalArgumentException("runKey must not be null");
        }
        this.runKey = runKey;
        this.clock = clock == null ? MonotonicClock.SYSTEM : clock;
        this.maxAtomicSeconds = Math.max(1L, maxAtomicSeconds);
        this.fairPriorityWaitNanos = Math.max(0L, fairPriorityWaitNanos);

        long nowMono = this.clock.nanoTime();
        for (ThrottlePhase phase : ThrottlePhase.values()) {
            for (ThrottleDirection dir : ThrottleDirection.values()) {
                int idx = slotIndex(phase, dir);
                slots[idx] = new SlotState(idx, phase, dir, nowMono);
            }
        }
        this.activeSnapshot = PolicySnapshot.disabled();
    }

    public static int slotIndex(ThrottlePhase phase, ThrottleDirection direction) {
        return phase.ordinal() * 2 + direction.ordinal();
    }

    public PolicySnapshot currentSnapshot() {
        return activeSnapshot;
    }

    public long getAppliedVersion() {
        return activeSnapshot.appliedVersion();
    }

    public boolean isPhaseEnabled(ThrottlePhase phase) {
        if (phase == null) {
            return false;
        }
        PhaseRule rule = activeSnapshot.phases().get(phase);
        return rule != null && rule.enabled();
    }

    public long maxSplittableEvents(ThrottlePhase phase) {
        PhaseRule rule = activeSnapshot.phases().get(phase);
        if (rule == null || !rule.enabled() || !rule.qpsEnabled()) {
            return Long.MAX_VALUE;
        }
        return computeNormalBurst(ThrottleDimension.QPS, rule.eventsPerSec());
    }

    public long maxSplittableBytes(ThrottlePhase phase) {
        PhaseRule rule = activeSnapshot.phases().get(phase);
        if (rule == null || !rule.enabled() || !rule.bytesEnabled()) {
            return Long.MAX_VALUE;
        }
        return computeNormalBurst(ThrottleDimension.BYTES, rule.bytesPerSec());
    }

    /**
     * Atomically validates and applies a policy snapshot across all 4 ordered slot locks,
     * settling refilled tokens up to nowMono, preserving negative debt, clipping excess positive tokens,
     * and starting newly enabled dimensions from an empty bucket.
     */
    public TaskThrottleApplyReceipt applyPolicy(
            TaskThrottlePolicyDto dto,
            String agentId,
            List<Long> skippedVersions) {
        long nowUtc = clock.currentTimeMillis();
        long reqVersion = dto == null ? 0L : dto.getDesiredVersion();

        policyPublishLock.lock();
        try {
            if (stopped) {
                return buildReceipt(agentId, reqVersion, nowUtc, "REJECTED", "TASK_STOPPED", skippedVersions);
            }
            if (dto == null) {
                return applyValidatedSnapshot(PolicySnapshot.disabled(), agentId, skippedVersions, nowUtc);
            }
            if (dto.getDesiredVersion() < activeSnapshot.appliedVersion()) {
                return buildReceipt(agentId, dto.getDesiredVersion(), nowUtc,
                        "IGNORED_STALE", ThrottleConstants.ERR_VERSION_CONFLICT, skippedVersions);
            }
            String validationErr = validatePolicy(dto);
            if (validationErr != null) {
                return buildReceipt(agentId, dto.getDesiredVersion(), nowUtc, "REJECTED", validationErr, skippedVersions);
            }
            PolicySnapshot nextSnapshot = toSnapshot(dto, nowUtc);
            return applyValidatedSnapshot(nextSnapshot, agentId, skippedVersions, nowUtc);
        } finally {
            policyPublishLock.unlock();
        }
    }

    private TaskThrottleApplyReceipt applyValidatedSnapshot(
            PolicySnapshot nextSnapshot,
            String agentId,
            List<Long> skippedVersions,
            long nowUtc) {
        long nowMono = clock.nanoTime();
        // Acquire all 4 slot locks in strict order 0 -> 1 -> 2 -> 3 and release in reverse order.
        for (int i = 0; i < slots.length; i++) {
            slots[i].lock.lock();
        }
        try {
            for (SlotState slot : slots) {
                PhaseRule rule = nextSnapshot.phases().get(slot.phase);
                boolean phaseOn = rule != null && rule.enabled();
                slot.bytesBucket.reconfigure(phaseOn && rule.bytesEnabled(), phaseOn ? rule.bytesPerSec() : 0L,
                        ThrottleDimension.BYTES, nowMono);
                slot.qpsBucket.reconfigure(phaseOn && rule.qpsEnabled(), phaseOn ? rule.eventsPerSec() : 0L,
                        ThrottleDimension.QPS, nowMono);
            }
            this.activeSnapshot = nextSnapshot;
        } finally {
            for (int i = slots.length - 1; i >= 0; i--) {
                slots[i].lock.unlock();
            }
        }
        wakeAllWaiters();
        return buildReceipt(agentId, nextSnapshot.appliedVersion(), nowUtc, "APPLIED", null, skippedVersions);
    }

    public WaitTicket newWaitTicket() {
        return new WaitTicket(candidateSeq.getAndIncrement(), clock.nanoTime());
    }

    /**
     * Cancels any fair reservation held by the given wait ticket and wakes waiters.
     */
    public void cancelWaitTicket(WaitTicket ticket) {
        if (ticket == null) {
            return;
        }
        ticket.markCancelled();
        for (int i = 0; i < slots.length; i++) {
            slots[i].lock.lock();
        }
        try {
            for (SlotState slot : slots) {
                slot.bytesBucket.releaseReservationIfOwner(ticket.candidateId);
                slot.qpsBucket.releaseReservationIfOwner(ticket.candidateId);
            }
        } finally {
            for (int i = slots.length - 1; i >= 0; i--) {
                slots[i].lock.unlock();
            }
        }
        wakeAllWaiters();
    }

    public void shutdown() {
        stopped = true;
        for (int i = 0; i < slots.length; i++) {
            slots[i].lock.lock();
        }
        try {
            for (SlotState slot : slots) {
                slot.bytesBucket.clearReservation();
                slot.qpsBucket.clearReservation();
            }
        } finally {
            for (int i = slots.length - 1; i >= 0; i--) {
                slots[i].lock.unlock();
            }
        }
        wakeAllWaiters();
    }

    public AcquireResult tryAcquire(
            ThrottlePhase phase,
            ThrottleDirection direction,
            long bytesCost,
            long eventsCost,
            boolean splittable,
            boolean bytesCostKnown,
            WaitTicket ticket) {
        return tryAcquireAll(
                Collections.singletonList(new SlotDemand(phase, direction, bytesCost, eventsCost, splittable, bytesCostKnown)),
                ticket);
    }

    /**
     * Acquires tokens across one or multiple (phase, direction) slots in fixed lock order.
     * Inside critical section: refills elapsed tokens, checks atomic <= 5s boundary, verifies
     * all required buckets (including fair priority reservations >= 1s), and either debits all
     * buckets together or debits none and computes exact ceiling ready nanos.
     */
    public AcquireResult tryAcquireAll(List<SlotDemand> demands, WaitTicket ticket) {
        if (stopped) {
            return AcquireResult.stopped(activeSnapshot.appliedVersion());
        }
        if (ticket != null && ticket.isCancelled()) {
            return AcquireResult.cancelled(activeSnapshot.appliedVersion());
        }
        if (demands == null || demands.isEmpty()) {
            return AcquireResult.admitted(activeSnapshot.appliedVersion());
        }

        // Deduplicate and strictly sort slot indices according to 4-lock ordering.
        TreeSet<Integer> orderedIndices = new TreeSet<>();
        Map<Integer, SlotDemand> demandBySlot = new java.util.TreeMap<>();
        for (SlotDemand d : demands) {
            if (d == null || d.phase == null || d.direction == null) {
                continue;
            }
            int idx = slotIndex(d.phase, d.direction);
            orderedIndices.add(idx);
            demandBySlot.merge(idx, d, SlotDemand::combine);
        }
        if (orderedIndices.isEmpty()) {
            return AcquireResult.admitted(activeSnapshot.appliedVersion());
        }

        List<Integer> lockOrder = new ArrayList<>(orderedIndices);
        long nowMono = clock.nanoTime();
        boolean releasedFairReservation = false;

        for (int idx : lockOrder) {
            slots[idx].lock.lock();
        }
        try {
            if (stopped) {
                return AcquireResult.stopped(activeSnapshot.appliedVersion());
            }
            if (ticket != null && ticket.isCancelled()) {
                releasedFairReservation = clearReservationLocked(lockOrder, ticket.candidateId);
                return AcquireResult.cancelled(activeSnapshot.appliedVersion());
            }

            long snapshotVersion = activeSnapshot.appliedVersion();
            EnumSet<ThrottleDimension> blockedDims = EnumSet.noneOf(ThrottleDimension.class);
            boolean anyEnabled = false;
            boolean sampleUnknownBlocking = false;
            long maxWaitNanos = 0L;

            List<BucketRequirement> requirements = new ArrayList<>(lockOrder.size() * 2);

            for (int idx : lockOrder) {
                SlotState slot = slots[idx];
                SlotDemand demand = demandBySlot.get(idx);

                slot.bytesBucket.refill(nowMono);
                slot.qpsBucket.refill(nowMono);

                if (slot.bytesBucket.enabled) {
                    anyEnabled = true;
                    if (!demand.bytesCostKnown) {
                        sampleUnknownBlocking = true;
                        blockedDims.add(ThrottleDimension.BYTES);
                    } else if (demand.bytesCost > 0L) {
                        long maxAtomic = maxAtomicCost(slot.bytesBucket.rate);
                        if (!demand.allowAtomicOverdraft && demand.bytesCost > maxAtomic) {
                            if (ticket != null) {
                                releasedFairReservation |= clearReservationLocked(lockOrder, ticket.candidateId);
                            }
                            return AcquireResult.exceedsAtomicLimit(snapshotVersion, ThrottleDimension.BYTES, demand.bytesCost, maxAtomic);
                        }
                        requirements.add(new BucketRequirement(slot.bytesBucket, ThrottleDimension.BYTES, demand.bytesCost));
                    }
                }

                if (slot.qpsBucket.enabled) {
                    anyEnabled = true;
                    if (demand.eventsCost > 0L) {
                        long maxAtomic = maxAtomicCost(slot.qpsBucket.rate);
                        if (!demand.allowAtomicOverdraft && demand.eventsCost > maxAtomic) {
                            if (ticket != null) {
                                releasedFairReservation |= clearReservationLocked(lockOrder, ticket.candidateId);
                            }
                            return AcquireResult.exceedsAtomicLimit(snapshotVersion, ThrottleDimension.QPS, demand.eventsCost, maxAtomic);
                        }
                        requirements.add(new BucketRequirement(slot.qpsBucket, ThrottleDimension.QPS, demand.eventsCost));
                    }
                }
            }

            if (!anyEnabled) {
                return AcquireResult.admitted(snapshotVersion);
            }
            if (sampleUnknownBlocking) {
                return AcquireResult.waitForUnknownSample(snapshotVersion, blockedDims);
            }
            if (requirements.isEmpty()) {
                return AcquireResult.admitted(snapshotVersion);
            }

            long cid = ticket == null ? 0L : ticket.candidateId;
            boolean reservedByOther = false;
            boolean needsReservationCheck = false;

            for (BucketRequirement req : requirements) {
                if (req.bucket.isReservedByOther(cid)) {
                    reservedByOther = true;
                    blockedDims.add(req.dimension);
                    maxWaitNanos = Math.max(maxWaitNanos, 5_000_000L);
                    continue;
                }
                long requiredBal = req.requiredReadyBalance();
                if (req.bucket.balance < requiredBal) {
                    blockedDims.add(req.dimension);
                    long wait = req.bucket.computeWaitNanos(requiredBal);
                    maxWaitNanos = Math.max(maxWaitNanos, wait);
                    needsReservationCheck = true;
                }
            }

            if (!reservedByOther && blockedDims.isEmpty()) {
                // Atomic debit across all required buckets + clear candidate fair reservation.
                for (BucketRequirement req : requirements) {
                    req.bucket.balance = saturatingSubtract(req.bucket.balance, req.cost);
                    if (req.bucket.balance < req.bucket.normalBurst) {
                        // Remainder preserved while refilling below burst
                    } else {
                        req.bucket.remainderNanos = 0L;
                    }
                    if (cid > 0L && req.bucket.releaseReservationIfOwner(cid)) {
                        releasedFairReservation = true;
                    }
                }
                return AcquireResult.admitted(snapshotVersion);
            }

            // Fair 1-second reservation for starved / oversized candidates without partial locking.
            if (ticket != null && ticket.candidateId > 0L) {
                ticket.markEligibleIfUnset(nowMono);
                if (needsReservationCheck && !reservedByOther && ticket.waitedNanos(nowMono) >= fairPriorityWaitNanos) {
                    boolean canReserveGroup = true;
                    for (BucketRequirement req : requirements) {
                        if (req.bucket.isReservedByOther(cid)) {
                            canReserveGroup = false;
                            break;
                        }
                    }
                    if (canReserveGroup) {
                        for (BucketRequirement req : requirements) {
                            req.bucket.reservedByCandidateId = cid;
                        }
                    }
                }
            }

            return AcquireResult.rateWait(snapshotVersion, Math.max(1L, maxWaitNanos), blockedDims);
        } finally {
            for (int i = lockOrder.size() - 1; i >= 0; i--) {
                slots[lockOrder.get(i)].lock.unlock();
            }
            if (releasedFairReservation) {
                wakeAllWaiters();
            }
        }
    }

    /**
     * Waits outside all token locks until admitted, cancelled, stopped, or atomic limit exceeded.
     */
    public AcquireResult acquireBlocking(
            List<SlotDemand> demands,
            BooleanSupplier cancelled,
            WaitStateObserver waitObserver) {
        WaitTicket ticket = newWaitTicket();
        try {
            while (true) {
                if (stopped) {
                    return AcquireResult.stopped(activeSnapshot.appliedVersion());
                }
                if (cancelled != null && cancelled.getAsBoolean()) {
                    cancelWaitTicket(ticket);
                    return AcquireResult.cancelled(activeSnapshot.appliedVersion());
                }

                AcquireResult result = tryAcquireAll(demands, ticket);
                if (result.outcome() == Outcome.ADMITTED) {
                    if (waitObserver != null) {
                        waitObserver.onAdmitted(result);
                    }
                    return result;
                }
                if (result.outcome() == Outcome.EXCEEDS_ATOMIC_LIMIT
                        || result.outcome() == Outcome.CANCELLED
                        || result.outcome() == Outcome.STOPPED) {
                    return result;
                }

                if (waitObserver != null) {
                    waitObserver.onWaiting(result);
                }

                long parkNanos = result.outcome() == Outcome.SAMPLE_UNKNOWN_WAIT
                        ? TimeUnit.MILLISECONDS.toNanos(100L)
                        : Math.min(TimeUnit.MILLISECONDS.toNanos(500L), Math.max(1_000_000L, result.waitNanos()));
                awaitSignal(parkNanos);
            }
        } finally {
            if (!stopped) {
                // Ensure no stray reservation leaks on unexpected exception/exit
                clearTicketSilent(ticket);
            }
        }
    }

    private void clearTicketSilent(WaitTicket ticket) {
        if (ticket == null) {
            return;
        }
        for (int i = 0; i < slots.length; i++) {
            slots[i].lock.lock();
        }
        try {
            for (SlotState slot : slots) {
                slot.bytesBucket.releaseReservationIfOwner(ticket.candidateId);
                slot.qpsBucket.releaseReservationIfOwner(ticket.candidateId);
            }
        } finally {
            for (int i = slots.length - 1; i >= 0; i--) {
                slots[i].lock.unlock();
            }
        }
    }

    private void awaitSignal(long nanos) {
        waiterSignalLock.lock();
        try {
            waiterSignal.awaitNanos(nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            waiterSignalLock.unlock();
        }
    }

    private void wakeAllWaiters() {
        waiterSignalLock.lock();
        try {
            waiterSignal.signalAll();
        } finally {
            waiterSignalLock.unlock();
        }
    }

    private boolean clearReservationLocked(List<Integer> lockOrder, long cid) {
        boolean releasedReservationIfChanged = false;
        if (cid <= 0L) {
            return false;
        }
        for (int idx : lockOrder) {
            releasedReservationIfChanged |= slots[idx].bytesBucket.releaseReservationIfOwner(cid);
            releasedReservationIfChanged |= slots[idx].qpsBucket.releaseReservationIfOwner(cid);
        }
        return releasedReservationIfChanged;
    }

    public long computeNormalBurst(ThrottleDimension dim, long rate) {
        if (rate <= 0L) {
            return 0L;
        }
        if (dim == ThrottleDimension.BYTES) {
            return rate;
        }
        return Math.max(1L, rate / 10L);
    }

    public long maxAtomicCost(long rate) {
        if (rate <= 0L) {
            return 0L;
        }
        try {
            return Math.multiplyExact(rate, maxAtomicSeconds);
        } catch (ArithmeticException ex) {
            return Long.MAX_VALUE;
        }
    }

    private String validatePolicy(TaskThrottlePolicyDto dto) {
        if (dto.getDesiredVersion() < 0L) {
            return ThrottleConstants.ERR_VERSION_CONFLICT;
        }
        if (dto.getPhases() == null || dto.getPhases().isEmpty()) {
            return null;
        }
        for (Map.Entry<ThrottlePhase, ThrottlePhaseLimit> entry : dto.getPhases().entrySet()) {
            ThrottlePhaseLimit limit = entry.getValue();
            if (limit == null || !limit.isEnabled()) {
                continue;
            }
            Long bytes = limit.getBytesPerSec();
            Long qps = limit.getEventsPerSec();
            if (bytes == null && qps == null) {
                return ThrottleConstants.ERR_INVALID_RATE;
            }
            if ((bytes != null && bytes <= 0L) || (qps != null && qps <= 0L)) {
                return ThrottleConstants.ERR_INVALID_RATE;
            }
        }
        return null;
    }

    private PolicySnapshot toSnapshot(TaskThrottlePolicyDto dto, long appliedAt) {
        EnumMap<ThrottlePhase, PhaseRule> map = new EnumMap<>(ThrottlePhase.class);
        for (ThrottlePhase phase : ThrottlePhase.values()) {
            ThrottlePhaseLimit limit = dto.getPhases() == null ? null : dto.getPhases().get(phase);
            if (limit != null && limit.isEnabled()) {
                long b = limit.getBytesPerSec() == null ? 0L : limit.getBytesPerSec();
                long q = limit.getEventsPerSec() == null ? 0L : limit.getEventsPerSec();
                map.put(phase, new PhaseRule(true, b, q));
            } else {
                map.put(phase, PhaseRule.DISABLED);
            }
        }
        return new PolicySnapshot(dto.getDesiredVersion(), appliedAt, Collections.unmodifiableMap(map));
    }

    private TaskThrottleApplyReceipt buildReceipt(
            String agentId,
            long policyVersion,
            long appliedAt,
            String outcome,
            String reason,
            List<Long> skippedVersions) {
        return TaskThrottleApplyReceipt.builder()
                .taskKey(runKey.toTaskKey())
                .agentId(agentId)
                .runEpoch(runKey.getRunEpoch())
                .policyVersion(policyVersion)
                .appliedAt(appliedAt)
                .outcome(outcome)
                .reason(reason)
                .skippedVersions(skippedVersions == null ? Collections.emptyList() : List.copyOf(skippedVersions))
                .build();
    }

    private static long saturatingAdd(long a, long b) {
        if (b <= 0L) {
            return a;
        }
        if (a > Long.MAX_VALUE - b) {
            return Long.MAX_VALUE;
        }
        return a + b;
    }

    private final class SlotState {
        final int orderIndex;
        final ThrottlePhase phase;
        final ThrottleDirection direction;
        final ReentrantLock lock = new ReentrantLock();
        final BucketState bytesBucket;
        final BucketState qpsBucket;

        SlotState(int orderIndex, ThrottlePhase phase, ThrottleDirection direction, long nowMono) {
            this.orderIndex = orderIndex;
            this.phase = phase;
            this.direction = direction;
            this.bytesBucket = new BucketState(ThrottleDimension.BYTES, nowMono);
            this.qpsBucket = new BucketState(ThrottleDimension.QPS, nowMono);
        }
    }

    private final class BucketState {
        final ThrottleDimension dimension;
        boolean enabled;
        long rate;
        long normalBurst;
        long balance;
        long remainderNanos;
        long lastRefillMonoNanos;
        long reservedByCandidateId;

        BucketState(ThrottleDimension dimension, long nowMono) {
            this.dimension = dimension;
            this.lastRefillMonoNanos = nowMono;
        }

        void refill(long nowMono) {
            if (!enabled) {
                lastRefillMonoNanos = nowMono;
                return;
            }
            if (nowMono <= lastRefillMonoNanos) {
                return;
            }
            long elapsed = nowMono - lastRefillMonoNanos;
            lastRefillMonoNanos = nowMono;
            if (balance >= normalBurst) {
                remainderNanos = 0L;
                return;
            }
            long added;
            long nextRemainder;
            if (elapsed <= (Long.MAX_VALUE - remainderNanos) / Math.max(1L, rate)) {
                long total = elapsed * rate + remainderNanos;
                added = total / ONE_SECOND_NANOS;
                nextRemainder = total % ONE_SECOND_NANOS;
            } else {
                BigInteger product = BigInteger.valueOf(elapsed)
                        .multiply(BigInteger.valueOf(rate))
                        .add(BigInteger.valueOf(remainderNanos));
                BigInteger[] divRem = product.divideAndRemainder(ONE_SECOND_BI);
                added = divRem[0].compareTo(MAX_LONG_BI) >= 0 ? Long.MAX_VALUE : divRem[0].longValue();
                nextRemainder = divRem[1].longValue();
            }
            long nextBalance = saturatingAdd(balance, added);
            if (nextBalance >= normalBurst) {
                balance = normalBurst;
                remainderNanos = 0L;
            } else {
                balance = nextBalance;
                remainderNanos = nextRemainder;
            }
        }

        void reconfigure(boolean nextEnabled, long nextRate, ThrottleDimension dim, long nowMono) {
            if (this.enabled) {
                refill(nowMono);
            } else {
                this.lastRefillMonoNanos = nowMono;
            }
            if (!nextEnabled || nextRate <= 0L) {
                this.enabled = false;
                this.rate = 0L;
                this.normalBurst = 0L;
                this.balance = 0L;
                this.remainderNanos = 0L;
                this.reservedByCandidateId = 0L;
                return;
            }
            long nextBurst = computeNormalBurst(dim, nextRate);
            if (this.enabled) {
                // Keep negative debt intact; clip positive surplus to new burst limit.
                if (this.balance >= nextBurst) {
                    this.balance = nextBurst;
                    this.remainderNanos = 0L;
                }
            } else {
                // Newly enabled dimension starts from an empty bucket.
                this.balance = 0L;
                this.remainderNanos = 0L;
                this.lastRefillMonoNanos = nowMono;
            }
            this.enabled = true;
            this.rate = nextRate;
            this.normalBurst = nextBurst;
            this.reservedByCandidateId = 0L;
        }

        boolean isReservedByOther(long cid) {
            return reservedByCandidateId != 0L && reservedByCandidateId != cid;
        }

        boolean releaseReservationIfOwner(long cid) {
            if (reservedByCandidateId == cid) {
                reservedByCandidateId = 0L;
                return true;
            }
            return false;
        }

        void clearReservation() {
            reservedByCandidateId = 0L;
        }

        long computeWaitNanos(long targetBalance) {
            long deficit = targetBalance - balance;
            if (deficit <= 0L || rate <= 0L) {
                return 0L;
            }
            if (deficit <= (Long.MAX_VALUE / ONE_SECOND_NANOS) - 1L) {
                long numerator = deficit * ONE_SECOND_NANOS - remainderNanos;
                if (numerator <= 0L) {
                    return 0L;
                }
                return (numerator + rate - 1L) / rate;
            }
            BigInteger num = BigInteger.valueOf(deficit)
                    .multiply(ONE_SECOND_BI)
                    .subtract(BigInteger.valueOf(remainderNanos));
            if (num.signum() <= 0) {
                return 0L;
            }
            BigInteger r = BigInteger.valueOf(rate);
            BigInteger[] divRem = num.divideAndRemainder(r);
            BigInteger ceil = divRem[1].signum() > 0 ? divRem[0].add(BigInteger.ONE) : divRem[0];
            return ceil.compareTo(MAX_LONG_BI) >= 0 ? Long.MAX_VALUE : ceil.longValue();
        }
    }

    private record BucketRequirement(BucketState bucket, ThrottleDimension dimension, long cost) {
        long requiredReadyBalance() {
            return cost > bucket.normalBurst ? bucket.normalBurst : cost;
        }
    }

    public static final class WaitTicket {
        private final long candidateId;
        private volatile long firstEligibleMonoNanos = Long.MIN_VALUE;
        private final AtomicBoolean cancelled = new AtomicBoolean(false);

        private WaitTicket(long candidateId, long createdMonoNanos) {
            this.candidateId = candidateId;
        }

        public long getCandidateId() {
            return candidateId;
        }

        void markEligibleIfUnset(long nowMono) {
            if (firstEligibleMonoNanos == Long.MIN_VALUE) {
                firstEligibleMonoNanos = nowMono;
            }
        }

        long waitedNanos(long nowMono) {
            long start = firstEligibleMonoNanos;
            return start == Long.MIN_VALUE ? 0L : Math.max(0L, nowMono - start);
        }

        void markCancelled() {
            cancelled.set(true);
        }

        public boolean isCancelled() {
            return cancelled.get();
        }
    }

    public interface WaitStateObserver {
        void onWaiting(AcquireResult waitResult);

        default void onAdmitted(AcquireResult admittedResult) {
        }
    }

    private static long saturatingSubtract(long a, long b) {
        try {
            return Math.subtractExact(a, b);
        } catch (ArithmeticException ex) {
            return Long.MIN_VALUE / 2L;
        }
    }

    public record SlotDemand(
            ThrottlePhase phase,
            ThrottleDirection direction,
            long bytesCost,
            long eventsCost,
            boolean splittable,
            boolean bytesCostKnown,
            boolean allowAtomicOverdraft) {

        public SlotDemand(
                ThrottlePhase phase,
                ThrottleDirection direction,
                long bytesCost,
                long eventsCost,
                boolean splittable,
                boolean bytesCostKnown) {
            this(phase, direction, bytesCost, eventsCost, splittable, bytesCostKnown, false);
        }

        static SlotDemand combine(SlotDemand a, SlotDemand b) {
            return new SlotDemand(
                    a.phase,
                    a.direction,
                    a.bytesCost + b.bytesCost,
                    a.eventsCost + b.eventsCost,
                    a.splittable && b.splittable,
                    a.bytesCostKnown && b.bytesCostKnown,
                    a.allowAtomicOverdraft || b.allowAtomicOverdraft
            );
        }
    }

    public enum Outcome {
        ADMITTED,
        RATE_WAIT,
        SAMPLE_UNKNOWN_WAIT,
        EXCEEDS_ATOMIC_LIMIT,
        CANCELLED,
        STOPPED
    }

    public record AcquireResult(
            Outcome outcome,
            long appliedVersion,
            long waitNanos,
            Set<ThrottleDimension> waitingDimensions,
            ThrottleDimension exceededDimension,
            long requestedCost,
            long limitCost) {

        public static AcquireResult admitted(long appliedVersion) {
            return new AcquireResult(Outcome.ADMITTED, appliedVersion, 0L, EnumSet.noneOf(ThrottleDimension.class), null, 0L, 0L);
        }

        public static AcquireResult rateWait(long appliedVersion, long waitNanos, Set<ThrottleDimension> dims) {
            return new AcquireResult(Outcome.RATE_WAIT, appliedVersion, waitNanos, Collections.unmodifiableSet(EnumSet.copyOf(dims)), null, 0L, 0L);
        }

        public static AcquireResult waitForUnknownSample(long appliedVersion, Set<ThrottleDimension> dims) {
            return new AcquireResult(Outcome.SAMPLE_UNKNOWN_WAIT, appliedVersion, TimeUnit.MILLISECONDS.toNanos(100L),
                    Collections.unmodifiableSet(EnumSet.copyOf(dims)), null, 0L, 0L);
        }

        public static AcquireResult exceedsAtomicLimit(long appliedVersion, ThrottleDimension dim, long cost, long limit) {
            return new AcquireResult(Outcome.EXCEEDS_ATOMIC_LIMIT, appliedVersion, 0L, EnumSet.noneOf(ThrottleDimension.class), dim, cost, limit);
        }

        public static AcquireResult cancelled(long appliedVersion) {
            return new AcquireResult(Outcome.CANCELLED, appliedVersion, 0L, EnumSet.noneOf(ThrottleDimension.class), null, 0L, 0L);
        }

        public static AcquireResult stopped(long appliedVersion) {
            return new AcquireResult(Outcome.STOPPED, appliedVersion, 0L, EnumSet.noneOf(ThrottleDimension.class), null, 0L, 0L);
        }
    }

    public record PhaseRule(boolean enabled, long bytesPerSec, long eventsPerSec) {
        public static final PhaseRule DISABLED = new PhaseRule(false, 0L, 0L);

        public boolean bytesEnabled() {
            return enabled && bytesPerSec > 0L;
        }

        public boolean qpsEnabled() {
            return enabled && eventsPerSec > 0L;
        }
    }

    public record PolicySnapshot(long appliedVersion, long appliedAt, Map<ThrottlePhase, PhaseRule> phases) {
        public static PolicySnapshot disabled() {
            EnumMap<ThrottlePhase, PhaseRule> map = new EnumMap<>(ThrottlePhase.class);
            for (ThrottlePhase phase : ThrottlePhase.values()) {
                map.put(phase, PhaseRule.DISABLED);
            }
            return new PolicySnapshot(0L, 0L, Collections.unmodifiableMap(map));
        }
    }
}

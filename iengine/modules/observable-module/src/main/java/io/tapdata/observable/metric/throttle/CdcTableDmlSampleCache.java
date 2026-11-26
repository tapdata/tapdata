package io.tapdata.observable.metric.throttle;

import com.tapdata.tm.commons.throttle.SampleQuality;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import io.tapdata.entity.event.dml.TapRecordEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-(taskId, runEpoch, direction) cache storing up to 4 event-weighted canonical byte samples per CDC (table, DML) group.
 * Supports staggered refresh, 2x interval expiration, 5-300s hot interval updates without cache flush, 5s mutation probing,
 * and hard sampling budget protection.
 */
public final class CdcTableDmlSampleCache {

    public static final class CacheConfig {
        private final int refreshIntervalSeconds;
        private final int maxSamplesPerGroup;
        private final int normalSamplesPerBatch;
        private final int coldStartSamplesPerBatch;
        private final int maxGroups;
        private final int maxWorkUnitsPerBatch;
        private final long mutationProbeIntervalNanos;
        private final long sampleConfigVersion;
        private final long highActiveBatchThreshold;

        private CacheConfig(Builder builder) {
            validateInterval(builder.refreshIntervalSeconds);
            this.refreshIntervalSeconds = builder.refreshIntervalSeconds;
            this.maxSamplesPerGroup = builder.maxSamplesPerGroup;
            this.normalSamplesPerBatch = builder.normalSamplesPerBatch;
            this.coldStartSamplesPerBatch = builder.coldStartSamplesPerBatch;
            this.maxGroups = builder.maxGroups;
            this.maxWorkUnitsPerBatch = builder.maxWorkUnitsPerBatch;
            this.mutationProbeIntervalNanos = builder.mutationProbeIntervalNanos;
            this.sampleConfigVersion = builder.sampleConfigVersion;
            this.highActiveBatchThreshold = builder.highActiveBatchThreshold;
        }

        public static CacheConfig defaults() {
            return new Builder().build();
        }

        public static Builder builder() {
            return new Builder();
        }

        public int getRefreshIntervalSeconds() {
            return refreshIntervalSeconds;
        }

        public int getMaxSamplesPerGroup() {
            return maxSamplesPerGroup;
        }

        public int getNormalSamplesPerBatch() {
            return normalSamplesPerBatch;
        }

        public int getColdStartSamplesPerBatch() {
            return coldStartSamplesPerBatch;
        }

        public int getMaxGroups() {
            return maxGroups;
        }

        public int getMaxWorkUnitsPerBatch() {
            return maxWorkUnitsPerBatch;
        }

        public long getMutationProbeIntervalNanos() {
            return mutationProbeIntervalNanos;
        }

        public long getSampleConfigVersion() {
            return sampleConfigVersion;
        }

        public long getHighActiveBatchThreshold() {
            return highActiveBatchThreshold;
        }

        public static final class Builder {
            private int refreshIntervalSeconds = ThrottleConstants.DEFAULT_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS;
            private int maxSamplesPerGroup = ThrottleConstants.MAX_SAMPLES_PER_GROUP;
            private int normalSamplesPerBatch = 1;
            private int coldStartSamplesPerBatch = 4;
            private int maxGroups = 10_000;
            private int maxWorkUnitsPerBatch = EventV1CodecSupport.DEFAULT_MAX_WORK_UNITS;
            private long mutationProbeIntervalNanos = TimeUnit.SECONDS.toNanos(ThrottleConstants.MUTATION_PROBE_INTERVAL_SECONDS);
            private long sampleConfigVersion = 1L;
            private long highActiveBatchThreshold = 10L;

            public Builder refreshIntervalSeconds(int refreshIntervalSeconds) {
                this.refreshIntervalSeconds = refreshIntervalSeconds;
                return this;
            }

            public Builder maxSamplesPerGroup(int maxSamplesPerGroup) {
                this.maxSamplesPerGroup = maxSamplesPerGroup;
                return this;
            }

            public Builder normalSamplesPerBatch(int normalSamplesPerBatch) {
                this.normalSamplesPerBatch = normalSamplesPerBatch;
                return this;
            }

            public Builder coldStartSamplesPerBatch(int coldStartSamplesPerBatch) {
                this.coldStartSamplesPerBatch = coldStartSamplesPerBatch;
                return this;
            }

            public Builder maxGroups(int maxGroups) {
                this.maxGroups = maxGroups;
                return this;
            }

            public Builder maxWorkUnitsPerBatch(int maxWorkUnitsPerBatch) {
                this.maxWorkUnitsPerBatch = maxWorkUnitsPerBatch;
                return this;
            }

            public Builder mutationProbeIntervalNanos(long mutationProbeIntervalNanos) {
                this.mutationProbeIntervalNanos = mutationProbeIntervalNanos;
                return this;
            }

            public Builder sampleConfigVersion(long sampleConfigVersion) {
                this.sampleConfigVersion = sampleConfigVersion;
                return this;
            }

            public Builder highActiveBatchThreshold(long highActiveBatchThreshold) {
                this.highActiveBatchThreshold = highActiveBatchThreshold;
                return this;
            }

            public CacheConfig build() {
                if (maxSamplesPerGroup <= 0 || normalSamplesPerBatch <= 0 || coldStartSamplesPerBatch <= 0
                        || maxGroups <= 0 || maxWorkUnitsPerBatch <= 0 || mutationProbeIntervalNanos <= 0) {
                    throw new IllegalArgumentException("Sample cache limits must be positive");
                }
                return new CacheConfig(this);
            }
        }
    }

    private record SampleEntry(long bytes, long eventWeight, long sampledAtNanos) {
    }

    private record ConfigState(int refreshIntervalSeconds, long refreshIntervalNanos, long sampleConfigVersion) {
    }

    private static final class GroupSnapshot {
        final List<SampleEntry> samples;
        final long lastSampledAtNanos;
        final long lastAccessedAtNanos;
        final long lastProbeAtNanos;
        final SampleEntry pendingMutationSample;
        final int pendingMutationDirection;
        final boolean lowTrafficCovered;

        private GroupSnapshot(
                List<SampleEntry> samples,
                long lastSampledAtNanos,
                long lastAccessedAtNanos,
                long lastProbeAtNanos,
                SampleEntry pendingMutationSample,
                int pendingMutationDirection,
                boolean lowTrafficCovered) {
            this.samples = samples;
            this.lastSampledAtNanos = lastSampledAtNanos;
            this.lastAccessedAtNanos = lastAccessedAtNanos;
            this.lastProbeAtNanos = lastProbeAtNanos;
            this.pendingMutationSample = pendingMutationSample;
            this.pendingMutationDirection = pendingMutationDirection;
            this.lowTrafficCovered = lowTrafficCovered;
        }

        static GroupSnapshot empty(long nowNanos) {
            return new GroupSnapshot(List.of(), 0L, nowNanos, 0L, null, 0, false);
        }
    }

    private static final class GroupSlot {
        final CdcSampleGroupKey key;
        final AtomicReference<GroupSnapshot> snapshot;
        final AtomicBoolean refreshing = new AtomicBoolean(false);
        final AtomicInteger cursor = new AtomicInteger(0);
        final AtomicLong staggerSeed;

        GroupSlot(CdcSampleGroupKey key, long nowNanos) {
            this.key = key;
            this.snapshot = new AtomicReference<>(GroupSnapshot.empty(nowNanos));
            this.staggerSeed = new AtomicLong(key.hashCode());
        }
    }

    /**
     * Per-group batch slice produced by the caller during single-pass batch scanning.
     */
    public static final class GroupBatchEvents {
        private final CdcSampleGroupKey key;
        private final List<TapRecordEvent> events = new ArrayList<>();

        public GroupBatchEvents(CdcSampleGroupKey key) {
            this.key = key;
        }

        public void add(TapRecordEvent event) {
            events.add(event);
        }

        public CdcSampleGroupKey getKey() {
            return key;
        }

        public List<TapRecordEvent> getEvents() {
            return events;
        }

        public int size() {
            return events.size();
        }
    }

    private record CandidateEvaluation(
            GroupSlot slot,
            GroupBatchEvents groupEvents,
            int priority, // 0 = MISSING, 1 = ACCUMULATING (1 sample), 2 = STALE/MUTATION/DUE
            boolean mutationProbeOnly
    ) {
    }

    private final String taskId;
    private final long runEpoch;
    private final ThrottleDirection direction;
    private final MonotonicClock clock;
    private final EventCostMeter meter;
    private final CacheConfig staticConfig;
    private final AtomicReference<ConfigState> configState;
    private final ConcurrentHashMap<CdcSampleGroupKey, GroupSlot> groups = new ConcurrentHashMap<>();
    private final AtomicInteger roundRobinCursor = new AtomicInteger(0);

    public CdcTableDmlSampleCache(
            String taskId,
            long runEpoch,
            ThrottleDirection direction,
            MonotonicClock clock,
            EventCostMeter meter,
            CacheConfig config) {
        this.taskId = taskId != null ? taskId : "";
        this.runEpoch = runEpoch;
        if (direction == null || clock == null || meter == null || config == null) {
            throw new IllegalArgumentException("CdcTableDmlSampleCache arguments must not be null");
        }
        this.direction = direction;
        this.clock = clock;
        this.meter = meter;
        this.staticConfig = config;
        this.configState = new AtomicReference<>(new ConfigState(
                config.getRefreshIntervalSeconds(),
                TimeUnit.SECONDS.toNanos(config.getRefreshIntervalSeconds()),
                config.getSampleConfigVersion()));
    }

    public static void validateInterval(int intervalSeconds) {
        if (intervalSeconds < ThrottleConstants.MIN_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS
                || intervalSeconds > ThrottleConstants.MAX_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS) {
            throw new IllegalArgumentException(
                    ThrottleConstants.ERR_INVALID_RATE + ": cdcSampleRefreshIntervalSeconds must be in [5, 300], got " + intervalSeconds);
        }
    }

    /**
     * Atomically applies a new CDC sampling interval (5-300s) without discarding existing cached samples.
     */
    public void updateRefreshInterval(int intervalSeconds, long sampleConfigVersion) {
        validateInterval(intervalSeconds);
        this.configState.set(new ConfigState(
                intervalSeconds,
                TimeUnit.SECONDS.toNanos(intervalSeconds),
                sampleConfigVersion));
    }

    public int getRefreshIntervalSeconds() {
        return configState.get().refreshIntervalSeconds();
    }

    public long getSampleConfigVersion() {
        return configState.get().sampleConfigVersion();
    }

    public int getGroupCount() {
        return groups.size();
    }

    /**
     * Marks groups observed during low-traffic metering as having completed a full measurement window
     * when at least one valid sample exists within the interval.
     */
    public void markLowTrafficWindowCovered(long nowNanos) {
        ConfigState cfg = this.configState.get();
        for (GroupSlot slot : groups.values()) {
            slot.snapshot.updateAndGet(prev -> {
                List<SampleEntry> active = pruneExpired(prev.samples, nowNanos, cfg.refreshIntervalNanos());
                if (active.size() == 1 && (nowNanos - active.get(0).sampledAtNanos()) >= cfg.refreshIntervalNanos() / 2) {
                    return new GroupSnapshot(active, prev.lastSampledAtNanos, prev.lastAccessedAtNanos,
                            prev.lastProbeAtNanos, prev.pendingMutationSample, prev.pendingMutationDirection, true);
                }
                return prev;
            });
        }
    }

    public BatchMeteringResult estimateBatch(
            List<GroupBatchEvents> batchGroups,
            long insertCount,
            long updateCount,
            long deleteCount,
            long controlCount,
            boolean rateWaiting) {
        ConfigState cfg = this.configState.get();
        long nowNanos = clock.nanoTime();
        long dmlCount = insertCount + updateCount + deleteCount;

        if (batchGroups == null || batchGroups.isEmpty() || dmlCount == 0L) {
            return BatchMeteringResult.builder()
                    .taskId(taskId)
                    .runEpoch(runEpoch)
                    .phase(ThrottlePhase.CDC)
                    .direction(direction)
                    .dmlCount(0L)
                    .insertCount(0L)
                    .updateCount(0L)
                    .deleteCount(0L)
                    .controlCount(controlCount)
                    .estimatedBytes(0L)
                    .sampleQuality(SampleQuality.VALID)
                    .sampleCount(0L)
                    .budgetHit(false)
                    .meterVersion(ThrottleConstants.METER_VERSION)
                    .sampleConfigVersion(cfg.sampleConfigVersion())
                    .cdcSampleRefreshIntervalSeconds(cfg.refreshIntervalSeconds())
                    .missingGroups(List.of())
                    .build();
        }

        evictExpiredGroupsIfFull(nowNanos, cfg.refreshIntervalNanos());

        boolean cacheLimitHit = false;
        List<GroupSlot> slots = new ArrayList<>(batchGroups.size());
        for (GroupBatchEvents g : batchGroups) {
            GroupSlot slot = groups.get(g.getKey());
            if (slot == null) {
                if (groups.size() >= staticConfig.getMaxGroups()) {
                    cacheLimitHit = true;
                    slots.add(null);
                    continue;
                }
                GroupSlot candidate = new GroupSlot(g.getKey(), nowNanos);
                GroupSlot existing = groups.putIfAbsent(g.getKey(), candidate);
                slot = existing != null ? existing : candidate;
            }
            slots.add(slot);
        }

        List<CandidateEvaluation> candidates = new ArrayList<>();
        boolean hasColdStartCandidate = false;
        for (int i = 0; i < batchGroups.size(); i++) {
            GroupSlot slot = slots.get(i);
            GroupBatchEvents g = batchGroups.get(i);
            if (slot == null || g.getEvents().isEmpty()) {
                continue;
            }
            GroupSnapshot snap = slot.snapshot.get();
            List<SampleEntry> valid = pruneExpired(snap.samples, nowNanos, cfg.refreshIntervalNanos());
            if (valid.isEmpty()) {
                hasColdStartCandidate = true;
                candidates.add(new CandidateEvaluation(slot, g, 0, false));
            } else if (valid.size() == 1 && !snap.lowTrafficCovered) {
                hasColdStartCandidate = true;
                candidates.add(new CandidateEvaluation(slot, g, 1, false));
            } else {
                long newestAge = nowNanos - valid.get(valid.size() - 1).sampledAtNanos();
                boolean regularDue = newestAge >= cfg.refreshIntervalNanos();
                boolean mutationEligible = !regularDue
                        && isMutationProbeEligible(snap, valid, g.size(), nowNanos, rateWaiting);
                if (regularDue || mutationEligible) {
                    candidates.add(new CandidateEvaluation(slot, g, 2, mutationEligible && !regularDue));
                }
            }
        }

        int maxSamplesThisBatch = hasColdStartCandidate
                ? staticConfig.getColdStartSamplesPerBatch()
                : staticConfig.getNormalSamplesPerBatch();

        long sampledInBatch = 0L;
        boolean workBudgetHit = cacheLimitHit;
        StringBuilder diagnostics = new StringBuilder();
        if (cacheLimitHit) {
            diagnostics.append("Group cache limit reached (maxGroups=").append(staticConfig.getMaxGroups()).append("); ");
        }

        if (!candidates.isEmpty()) {
            // Sort by priority (MISSING first, then ACCUMULATING, then DUE/MUTATION) and rotate peers evenly.
            // Within priority 2 (regular due / mutation), stagger tie-break deterministically by monotonic offset + round-robin cursor.
            int offset = candidates.size() > 1 ? Math.floorMod(roundRobinCursor.getAndIncrement(), candidates.size()) : 0;
            List<CandidateEvaluation> ordered = new ArrayList<>(candidates.size());
            for (int i = 0; i < candidates.size(); i++) {
                ordered.add(candidates.get((i + offset) % candidates.size()));
            }
            ordered.sort((a, b) -> {
                int pCmp = Integer.compare(a.priority(), b.priority());
                if (pCmp != 0) {
                    return pCmp;
                }
                if (a.priority() == 2) {
                    return Long.compare(
                            a.slot().snapshot.get().lastSampledAtNanos + staggerOffsetNanos(a.slot(), cfg.refreshIntervalNanos()),
                            b.slot().snapshot.get().lastSampledAtNanos + staggerOffsetNanos(b.slot(), cfg.refreshIntervalNanos()));
                }
                return 0;
            });

            int remainingWorkUnits = staticConfig.getMaxWorkUnitsPerBatch();
            for (CandidateEvaluation candidate : ordered) {
                if (sampledInBatch >= maxSamplesThisBatch) {
                    if (candidate.priority() == 0) {
                        workBudgetHit = true;
                        diagnostics.append("Cold-start group deferred due to batch sample cap; ");
                    }
                    break;
                }
                if (remainingWorkUnits <= 0) {
                    workBudgetHit = true;
                    diagnostics.append("Work-unit budget exhausted before sampling ").append(candidate.slot().key).append("; ");
                    break;
                }
                if (!candidate.slot().refreshing.compareAndSet(false, true)) {
                    continue;
                }
                try {
                    List<TapRecordEvent> groupEvents = candidate.groupEvents().getEvents();
                    int idx = Math.floorMod(candidate.slot().cursor.getAndIncrement(), groupEvents.size());
                    TapRecordEvent targetEvent = groupEvents.get(idx);
                    EventV1View view = EventV1View.fromTapRecordEvent(targetEvent, candidate.slot().key.getNormalizedTable());
                    long bytes = meter.measure(view, remainingWorkUnits);
                    // Approximate work units consumed based on payload fields/size guardrail
                    int estUnits = Math.max(1, (int) Math.min(remainingWorkUnits, 1L + bytes / 4096L));
                    remainingWorkUnits -= estUnits;
                    SampleEntry measured = new SampleEntry(bytes, groupEvents.size(), nowNanos);
                    applyMeasuredSample(candidate.slot(), measured, cfg, nowNanos, candidate.mutationProbeOnly());
                    sampledInBatch++;
                } catch (MeterBudgetExceededException ex) {
                    workBudgetHit = true;
                    diagnostics.append("Sampling budget exceeded for ").append(candidate.slot().key).append(": ").append(ex.getMessage()).append("; ");
                    break;
                } catch (RuntimeException ex) {
                    diagnostics.append("Sampling failed for ").append(candidate.slot().key).append(": ").append(ex.getMessage()).append("; ");
                } finally {
                    candidate.slot().refreshing.set(false);
                }
            }
        }

        // Aggregate batch estimatedBytes and worst SampleQuality
        long totalEstimatedBytes = 0L;
        boolean missingAny = false;
        SampleQuality worstQuality = SampleQuality.VALID;
        List<CdcSampleGroupKey> missingGroups = new ArrayList<>();

        for (int i = 0; i < batchGroups.size(); i++) {
            GroupBatchEvents g = batchGroups.get(i);
            if (g.size() == 0) {
                continue;
            }
            GroupSlot slot = slots.get(i);
            if (slot == null) {
                missingAny = true;
                missingGroups.add(g.getKey());
                worstQuality = combineQuality(worstQuality, SampleQuality.BUDGET_LIMITED);
                continue;
            }
            GroupSnapshot snap = slot.snapshot.updateAndGet(prev -> new GroupSnapshot(
                    pruneExpired(prev.samples, nowNanos, cfg.refreshIntervalNanos()),
                    prev.lastSampledAtNanos,
                    nowNanos,
                    prev.lastProbeAtNanos,
                    prev.pendingMutationSample,
                    prev.pendingMutationDirection,
                    prev.lowTrafficCovered));

            if (snap.samples.isEmpty()) {
                missingAny = true;
                missingGroups.add(g.getKey());
                worstQuality = combineQuality(worstQuality, workBudgetHit ? SampleQuality.BUDGET_LIMITED : SampleQuality.MISSING);
                continue;
            }

            double weightedMean = computeWeightedMean(snap.samples);
            long groupEstimated = (long) Math.ceil(g.size() * weightedMean);
            if (Long.MAX_VALUE - totalEstimatedBytes < groupEstimated) {
                totalEstimatedBytes = Long.MAX_VALUE;
            } else {
                totalEstimatedBytes += groupEstimated;
            }

            SampleEntry newest = snap.samples.get(snap.samples.size() - 1);
            long newestAge = nowNanos - newest.sampledAtNanos();
            SampleQuality groupQuality;
            if (snap.samples.size() == 1 && !snap.lowTrafficCovered) {
                groupQuality = SampleQuality.ACCUMULATING;
            } else if (newestAge > cfg.refreshIntervalNanos()) {
                groupQuality = SampleQuality.STALE_REUSED;
            } else {
                groupQuality = SampleQuality.VALID;
            }
            worstQuality = combineQuality(worstQuality, groupQuality);
        }

        if (workBudgetHit && worstQuality == SampleQuality.VALID && missingAny) {
            worstQuality = SampleQuality.BUDGET_LIMITED;
        }

        return BatchMeteringResult.builder()
                .taskId(taskId)
                .runEpoch(runEpoch)
                .phase(ThrottlePhase.CDC)
                .direction(direction)
                .dmlCount(dmlCount)
                .insertCount(insertCount)
                .updateCount(updateCount)
                .deleteCount(deleteCount)
                .controlCount(controlCount)
                .estimatedBytes(missingAny ? null : totalEstimatedBytes)
                .sampleQuality(worstQuality)
                .sampleCount(sampledInBatch)
                .budgetHit(workBudgetHit)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .sampleConfigVersion(cfg.sampleConfigVersion())
                .cdcSampleRefreshIntervalSeconds(cfg.refreshIntervalSeconds())
                .missingGroups(missingGroups.isEmpty() ? List.of() : Collections.unmodifiableList(missingGroups))
                .sampleDiagnostics(diagnostics.length() == 0 ? null : diagnostics.toString())
                .build();
    }

    private boolean isMutationProbeEligible(
            GroupSnapshot snap,
            List<SampleEntry> valid,
            int groupBatchSize,
            long nowNanos,
            boolean rateWaiting) {
        if (valid.size() < 2) {
            return false;
        }
        boolean highActive = groupBatchSize >= staticConfig.getHighActiveBatchThreshold();
        if (!rateWaiting && !highActive) {
            return false;
        }
        long lastActivity = Math.max(snap.lastSampledAtNanos, snap.lastProbeAtNanos);
        return (nowNanos - lastActivity) >= staticConfig.getMutationProbeIntervalNanos();
    }

    private void applyMeasuredSample(
            GroupSlot slot,
            SampleEntry measured,
            ConfigState cfg,
            long nowNanos,
            boolean probeUpdate) {
        slot.snapshot.updateAndGet(prev -> {
            List<SampleEntry> active = pruneExpired(prev.samples, nowNanos, cfg.refreshIntervalNanos());
            if (active.size() >= 2) {
                double oldMean = computeWeightedMean(active);
                int shiftDir = 0;
                if (oldMean > 0.0d) {
                    if (measured.bytes() > 2.0d * oldMean) {
                        shiftDir = 1;
                    } else if (measured.bytes() < 0.5d * oldMean) {
                        shiftDir = -1;
                    }
                } else if (measured.bytes() > 0L) {
                    shiftDir = 1;
                }

                if (shiftDir != 0) {
                    if (prev.pendingMutationDirection == shiftDir && prev.pendingMutationSample != null) {
                        // Two consecutive confirming mutation samples: immediately rebuild group baseline.
                        List<SampleEntry> rebuilt = List.of(prev.pendingMutationSample, measured);
                        return new GroupSnapshot(rebuilt, nowNanos, nowNanos, nowNanos, null, 0, true);
                    }
                    // Keep old samples on first spike; store pending probe candidate until confirmed.
                    return new GroupSnapshot(active, prev.lastSampledAtNanos, nowNanos, nowNanos, measured, shiftDir, prev.lowTrafficCovered);
                }
            }

            List<SampleEntry> updated = new ArrayList<>(active);
            updated.add(measured);
            while (updated.size() > staticConfig.getMaxSamplesPerGroup()) {
                updated.remove(0);
            }
            return new GroupSnapshot(
                    Collections.unmodifiableList(updated),
                    nowNanos,
                    nowNanos,
                    nowNanos,
                    null,
                    0,
                    prev.lowTrafficCovered || updated.size() >= 2);
        });
    }

    private static List<SampleEntry> pruneExpired(List<SampleEntry> samples, long nowNanos, long refreshIntervalNanos) {
        if (samples.isEmpty()) {
            return List.of();
        }
        long maxAgeNanos = saturatingMultiply(refreshIntervalNanos, 2L);
        List<SampleEntry> valid = null;
        for (int i = 0; i < samples.size(); i++) {
            SampleEntry entry = samples.get(i);
            boolean expired = (nowNanos - entry.sampledAtNanos()) > maxAgeNanos;
            if (!expired && valid != null) {
                valid.add(entry);
            } else if (expired && valid == null) {
                valid = new ArrayList<>(samples.size());
                for (int j = 0; j < i; j++) {
                    valid.add(samples.get(j));
                }
            }
        }
        return valid == null ? samples : Collections.unmodifiableList(valid);
    }

    private void evictExpiredGroupsIfFull(long nowNanos, long refreshIntervalNanos) {
        if (groups.size() < staticConfig.getMaxGroups()) {
            return;
        }
        long idleCutoffNanos = saturatingMultiply(refreshIntervalNanos, 2L);
        for (Map.Entry<CdcSampleGroupKey, GroupSlot> entry : groups.entrySet()) {
            GroupSlot slot = entry.getValue();
            if (slot.refreshing.get()) {
                continue;
            }
            GroupSnapshot snap = slot.snapshot.get();
            if ((nowNanos - snap.lastAccessedAtNanos) > idleCutoffNanos) {
                groups.remove(entry.getKey(), slot);
            }
        }
    }

    private static double computeWeightedMean(List<SampleEntry> samples) {
        double weightedSum = 0.0d;
        double weightTotal = 0.0d;
        for (SampleEntry entry : samples) {
            double w = Math.max(1L, entry.eventWeight());
            weightedSum += ((double) entry.bytes()) * w;
            weightTotal += w;
        }
        return weightTotal > 0.0d ? weightedSum / weightTotal : 0.0d;
    }

    private static long staggerOffsetNanos(GroupSlot slot, long refreshIntervalNanos) {
        long window = Math.max(1L, refreshIntervalNanos / 4L);
        return Math.floorMod(slot.staggerSeed.get(), window);
    }

    private static long saturatingMultiply(long a, long b) {
        if (a <= 0L || b <= 0L) {
            return 0L;
        }
        if (a > Long.MAX_VALUE / b) {
            return Long.MAX_VALUE;
        }
        return a * b;
    }

    static SampleQuality combineQuality(SampleQuality a, SampleQuality b) {
        if (a == null) return b;
        if (b == null) return a;
        return qualitySeverity(a) >= qualitySeverity(b) ? a : b;
    }

    private static int qualitySeverity(SampleQuality quality) {
        return switch (quality) {
            case BUDGET_LIMITED -> 5;
            case MISSING -> 4;
            case STALE_REUSED -> 3;
            case ACCUMULATING -> 2;
            case VALID -> 1;
        };
    }
}

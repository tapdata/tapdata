package com.tapdata.tm.throttle.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.AgentThrottleDeltaResponse;
import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.SampleQuality;
import com.tapdata.tm.commons.throttle.TaskThrottleApplyReceipt;
import com.tapdata.tm.commons.throttle.TaskThrottlePolicyDto;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDimension;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import com.tapdata.tm.commons.throttle.TimeQuality;
import com.tapdata.tm.commons.throttle.WaitReason;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.chart.ChartViewService;
import com.tapdata.tm.throttle.dto.TaskThrottleLiveStatusView;
import com.tapdata.tm.throttle.dto.TaskThrottleRankingResponse;
import com.tapdata.tm.throttle.entity.TaskThrottleChangeLogEntity;
import com.tapdata.tm.throttle.entity.TaskThrottlePolicyEntity;
import com.tapdata.tm.throttle.entity.TaskThrottleTelemetryEntity;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Handles incremental Agent policy delta pulling, idempotent receipt ingestion,
 * 5s telemetry persistence, real-time wait status evaluation (5s renew / 15s unknown expiry),
 * and >= 90%-coverage dashboard data-rate rankings using union-deduplicated time intervals.
 */
@Service
@Slf4j
public class TaskThrottleTelemetryAndRankingService {

    public static final double MIN_RANKING_COVERAGE_RATIO = 0.90d;
    private static final long LIVE_UNKNOWN_EXPIRE_MILLIS = TimeUnit.SECONDS.toMillis(ThrottleConstants.REALTIME_WAIT_UNKNOWN_SECONDS);
    private static final long HISTORICAL_BACKFILL_CUTOFF_MILLIS = TimeUnit.SECONDS.toMillis(30L);

    private static final Set<String> RANKABLE_TASK_KINDS = Set.of("sync", "migrate", "logCollector");

    private final MongoOperations mongo;
    private final TaskThrottleSampleConfigService sampleConfigService;
    private final Clock clock;

    @Autowired(required = false)
    private ChartViewService chartViewService;

    private final Map<String, LiveTaskTracker> liveTrackers = new ConcurrentHashMap<>();

    @Autowired
    public TaskThrottleTelemetryAndRankingService(
            MongoOperations mongo,
            TaskThrottleSampleConfigService sampleConfigService) {
        this(mongo, sampleConfigService, Clock.systemUTC());
    }

    public TaskThrottleTelemetryAndRankingService(
            MongoOperations mongo,
            TaskThrottleSampleConfigService sampleConfigService,
            Clock clock) {
        this.mongo = mongo;
        this.sampleConfigService = sampleConfigService;
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    public void setChartViewService(ChartViewService chartViewService) {
        this.chartViewService = chartViewService;
    }

    /**
     * Pulls incremental task policies, authorized runEpochs, fencing directives for displaced runs,
     * and the latest CDC sampling interval configuration for the polling Agent.
     */
    public AgentThrottleDeltaResponse pullDelta(String agentId, long cursor) {
        long now = clock.millis();
        TaskThrottleSampleConfigService.ConfigSnapshot sampleCfg = sampleConfigService.getCurrentConfig();
        List<AgentThrottleDeltaResponse.TaskDirective> directives = new ArrayList<>();
        long maxSeq = Math.max(0L, cursor);

        if (mongo != null && agentId != null && !agentId.isBlank()) {
            Criteria activeCriteria = Criteria.where("currentAgentId").is(agentId);
            if (cursor > 0L) {
                activeCriteria = activeCriteria.and("deltaSeq").gt(cursor);
            }
            List<TaskThrottlePolicyEntity> active = mongo.find(Query.query(activeCriteria), TaskThrottlePolicyEntity.class);
            if (active != null) {
                for (TaskThrottlePolicyEntity entity : active) {
                    maxSeq = Math.max(maxSeq, entity.getDeltaSeq());
                    directives.add(AgentThrottleDeltaResponse.TaskDirective.builder()
                            .tenantId(entity.getTenantId())
                            .taskKind(entity.getTaskKind())
                            .taskId(entity.getTaskId())
                            .authorizedRunEpoch(entity.getCurrentRunEpoch())
                            .protectedRun(entity.isProtectedRunActive())
                            .fenced(false)
                            .policy(TaskThrottleExecutionGuardService.toPolicyDto(entity))
                            .build());
                }
            }

            Criteria displacedCriteria = Criteria.where("previousAgentId").is(agentId).and("currentAgentId").ne(agentId);
            if (cursor > 0L) {
                displacedCriteria = displacedCriteria.and("deltaSeq").gt(cursor);
            }
            List<TaskThrottlePolicyEntity> displaced = mongo.find(Query.query(displacedCriteria), TaskThrottlePolicyEntity.class);
            if (displaced != null) {
                for (TaskThrottlePolicyEntity entity : displaced) {
                    maxSeq = Math.max(maxSeq, entity.getDeltaSeq());
                    directives.add(AgentThrottleDeltaResponse.TaskDirective.builder()
                            .tenantId(entity.getTenantId())
                            .taskKind(entity.getTaskKind())
                            .taskId(entity.getTaskId())
                            .authorizedRunEpoch(entity.getCurrentRunEpoch())
                            .protectedRun(entity.isProtectedRunActive())
                            .fenced(true)
                            .policy(null)
                            .build());
                }
            }
        }

        return AgentThrottleDeltaResponse.builder()
                .schemaVersion(1)
                .cursor(maxSeq)
                .fullSync(cursor <= 0L)
                .serverTimeUtc(now)
                .sampleConfigVersion(sampleCfg.configVersion())
                .cdcSampleRefreshIntervalSeconds(sampleCfg.intervalSeconds())
                .directives(directives)
                .build();
    }

    /**
     * Ingests policy apply receipts idempotently by (taskKey, runEpoch, policyVersion).
     * Only monotonic receipts for the currently authorized runEpoch update applied summary fields;
     * failed receipts retain the last successfully applied summary and surface reason.
     */
    public void ingestReceipts(String agentId, List<TaskThrottleApplyReceipt> receipts) {
        if (mongo == null || receipts == null || receipts.isEmpty()) {
            return;
        }
        long now = clock.millis();
        for (TaskThrottleApplyReceipt receipt : receipts) {
            if (receipt == null || receipt.getTaskKey() == null) {
                continue;
            }
            String taskId = extractTaskIdFromKey(receipt.getTaskKey());
            String dedupKey = "RECEIPT:" + receipt.getTaskKey() + ":" + receipt.getRunEpoch() + ":" + receipt.getPolicyVersion();

            try {
                mongo.insert(TaskThrottleChangeLogEntity.builder()
                        .id(UUID.randomUUID().toString())
                        .dedupKey(dedupKey)
                        .taskKey(receipt.getTaskKey())
                        .taskId(taskId)
                        .eventType(TaskThrottleChangeLogEntity.EVENT_APPLY_RECEIPT)
                        .runEpoch(receipt.getRunEpoch())
                        .policyVersion(receipt.getPolicyVersion())
                        .outcome(receipt.getOutcome())
                        .reason(receipt.getReason())
                        .skippedVersions(receipt.getSkippedVersions())
                        .agentId(receipt.getAgentId() != null ? receipt.getAgentId() : agentId)
                        .occurredAt(receipt.getAppliedAt() > 0L ? receipt.getAppliedAt() : now)
                        .build());
            } catch (DuplicateKeyException ignored) {
                // Already recorded: receipt redelivery still evaluates monotonic summary safely below
            }

            TaskThrottlePolicyEntity policy = mongo.findOne(
                    Query.query(Criteria.where("_id").is(receipt.getTaskKey())),
                    TaskThrottlePolicyEntity.class);
            if (policy == null) {
                continue;
            }
            // Only current authorized runEpoch and monotonic policy versions can advance running summary.
            if (receipt.getRunEpoch() != policy.getCurrentRunEpoch()
                    || receipt.getPolicyVersion() < policy.getAppliedVersion()) {
                continue;
            }
            // Also avoid stale older version overwriting a higher already-applied version.
            boolean appliedSuccess = "APPLIED".equalsIgnoreCase(receipt.getOutcome());
            if (appliedSuccess) {
                Map<ThrottlePhase, ThrottlePhaseLimit> snapshotPhases = resolvePhasesForVersion(policy, receipt.getPolicyVersion());
                Query casQ = Query.query(Criteria.where("_id").is(policy.getId())
                        .and("currentRunEpoch").is(policy.getCurrentRunEpoch())
                        .and("appliedVersion").lte(receipt.getPolicyVersion()));
                Update u = new Update()
                        .set("appliedVersion", receipt.getPolicyVersion())
                        .set("appliedAt", receipt.getAppliedAt() > 0L ? receipt.getAppliedAt() : now)
                        .set("applyStatus", TaskThrottlePolicyEntity.APPLY_APPLIED)
                        .set("lastApplyReason", null);
                if (snapshotPhases != null) {
                    u.set("appliedPhases", snapshotPhases);
                }
                mongo.updateFirst(casQ, u, TaskThrottlePolicyEntity.class);
            } else {
                // Keep last successful appliedVersion/appliedPhases intact!
                Query failQ = Query.query(Criteria.where("_id").is(policy.getId())
                        .and("currentRunEpoch").is(policy.getCurrentRunEpoch()));
                Update u = new Update()
                        .set("applyStatus", TaskThrottlePolicyEntity.APPLY_FAILED)
                        .set("lastApplyReason", receipt.getReason());
                mongo.updateFirst(failQ, u, TaskThrottlePolicyEntity.class);
            }
        }
    }

    /**
     * Persists telemetry windows deduplicated by (taskId, runEpoch, sampleSeq, phase, direction)
     * and refreshes real-time throttling status only for current epoch & strictly newer non-backfill sequences.
     */
    public void ingestTelemetry(String agentId, Map<String, List<TaskThrottleTelemetrySample>> samplesByTask) {
        if (samplesByTask == null || samplesByTask.isEmpty()) {
            return;
        }
        long nowUtc = clock.millis();
        for (Map.Entry<String, List<TaskThrottleTelemetrySample>> entry : samplesByTask.entrySet()) {
            String taskId = entry.getKey();
            List<TaskThrottleTelemetrySample> list = entry.getValue();
            if (taskId == null || list == null || list.isEmpty()) {
                continue;
            }

            TaskThrottlePolicyEntity policy = mongo == null ? null : mongo.findOne(
                    Query.query(Criteria.where("taskId").is(taskId)), TaskThrottlePolicyEntity.class);

            for (TaskThrottleTelemetrySample sample : list) {
                if (sample == null || sample.getPhase() == null || sample.getDirection() == null) {
                    continue;
                }
                if (mongo != null) {
                    String sampleDocId = TaskThrottleTelemetryEntity.buildSampleId(
                            taskId, sample.getRunEpoch(), sample.getSampleSeq(), sample.getPhase(), sample.getDirection());
                    TaskThrottleTelemetryEntity doc = TaskThrottleTelemetryEntity.builder()
                            .id(sampleDocId)
                            .tenantId(policy != null ? policy.getTenantId() : null)
                            .taskKind(policy != null ? policy.getTaskKind() : null)
                            .taskId(taskId)
                            .agentId(agentId)
                            .receivedAtUtc(nowUtc)
                            .runEpoch(sample.getRunEpoch())
                            .sampleSeq(sample.getSampleSeq())
                            .meterVersion(sample.getMeterVersion())
                            .sampleConfigVersion(sample.getSampleConfigVersion())
                            .cdcSampleRefreshIntervalSeconds(sample.getCdcSampleRefreshIntervalSeconds())
                            .phase(sample.getPhase())
                            .direction(sample.getDirection())
                            .windowStartUtc(sample.getWindowStartUtc())
                            .windowEndUtc(sample.getWindowEndUtc())
                            .durationNanos(sample.getDurationNanos())
                            .timeQuality(sample.getTimeQuality())
                            .appliedVersion(sample.getAppliedVersion())
                            .admittedEvents(sample.getAdmittedEvents())
                            .estimatedAdmittedBytes(sample.getEstimatedAdmittedBytes())
                            .sampleCount(sample.getSampleCount())
                            .sampleQuality(sample.getSampleQuality())
                            .cancelledAfterDebitEvents(sample.getCancelledAfterDebitEvents())
                            .cancelledEstimatedBytes(sample.getCancelledEstimatedBytes())
                            .waitReason(sample.getWaitReason())
                            .waitingDimensions(sample.getWaitingDimensions())
                            .bufferWatermark(sample.getBufferWatermark())
                            .logPositionStatus(sample.getLogPositionStatus())
                            .build();
                    try {
                        mongo.insert(doc);
                    } catch (DuplicateKeyException ignored) {
                        // Duplicate upload still doesn't renew older live status
                    }
                }

                // Update live status ONLY when epoch >= current authorized epoch AND not stale/backfill sample.
                long authorizedEpoch = policy == null ? 0L : policy.getCurrentRunEpoch();
                if (authorizedEpoch > 0L && sample.getRunEpoch() < authorizedEpoch) {
                    continue;
                }
                boolean isHistoricalBackfill = sample.getWindowEndUtc() > 0L
                        && (nowUtc - sample.getWindowEndUtc() > HISTORICAL_BACKFILL_CUTOFF_MILLIS);
                if (isHistoricalBackfill) {
                    continue;
                }
                LiveTaskTracker tracker = liveTrackers.computeIfAbsent(taskId, LiveTaskTracker::new);
                tracker.updateFromSample(sample, nowUtc);
            }
        }
    }

    public TaskThrottleLiveStatusView evaluateLiveStatus(String taskId) {
        if (taskId == null) {
            return null;
        }
        LiveTaskTracker tracker = liveTrackers.get(taskId);
        if (tracker == null) {
            return TaskThrottleLiveStatusView.builder()
                    .taskId(taskId)
                    .throttlingActive(false)
                    .statusUnknown(false)
                    .policyConfigured(false)
                    .configuredPhases(Set.of())
                    .runEpoch(0L)
                    .activeWaits(List.of())
                    .activeDimensionsByPhase(Map.of())
                    .build();
        }
        return tracker.toView(clock.millis());
    }

    public Map<String, TaskThrottleLiveStatusView> evaluateLiveStatuses(List<String> taskIds) {
        Map<String, TaskThrottleLiveStatusView> map = new LinkedHashMap<>();
        if (taskIds != null) {
            for (String taskId : taskIds) {
                if (taskId != null && !taskId.isBlank()) {
                    map.put(taskId, evaluateLiveStatus(taskId));
                }
            }
        }
        if (mongo != null && !map.isEmpty()) {
            Map<String, Set<ThrottlePhase>> configuredByTaskId = loadConfiguredPhases(map.keySet());
            for (Map.Entry<String, TaskThrottleLiveStatusView> entry : map.entrySet()) {
                Set<ThrottlePhase> phases = configuredByTaskId.getOrDefault(entry.getKey(), Set.of());
                TaskThrottleLiveStatusView view = entry.getValue();
                view.setConfiguredPhases(phases);
                view.setPolicyConfigured(!phases.isEmpty());
            }
        }
        return map;
    }

    private Map<String, Set<ThrottlePhase>> loadConfiguredPhases(Set<String> taskIds) {
        Map<String, Set<ThrottlePhase>> result = new HashMap<>();
        if (mongo == null || taskIds == null || taskIds.isEmpty()) {
            return result;
        }
        List<TaskThrottlePolicyEntity> policies = mongo.find(
                Query.query(Criteria.where("taskId").in(taskIds)),
                TaskThrottlePolicyEntity.class);
        if (policies != null) {
            for (TaskThrottlePolicyEntity policy : policies) {
                if (policy == null || policy.getTaskId() == null) {
                    continue;
                }
                EnumSet<ThrottlePhase> enabled = EnumSet.noneOf(ThrottlePhase.class);
                if (policy.getPhases() != null) {
                    for (Map.Entry<ThrottlePhase, ThrottlePhaseLimit> phaseEntry : policy.getPhases().entrySet()) {
                        if (phaseEntry.getKey() != null
                                && phaseEntry.getValue() != null
                                && phaseEntry.getValue().isEnabled()) {
                            enabled.add(phaseEntry.getKey());
                        }
                    }
                }
                if (!enabled.isEmpty()) {
                    result.computeIfAbsent(policy.getTaskId(), k -> EnumSet.noneOf(ThrottlePhase.class))
                            .addAll(enabled);
                }
            }
        }
        return result;
    }

    public void clearLiveStatus(String taskId) {
        if (taskId != null) {
            liveTrackers.remove(taskId);
        }
    }

    /**
     * Computes Dashboard Estimated Data Rate ranking (MB/s) using only event-v1 samples,
     * merging overlapping FULL/CDC intervals on the same direction by union, and enforcing >= 90% window coverage.
     */
    public TaskThrottleRankingResponse queryEstimatedDataRateRanking(
            UserDetail user,
            ThrottleDirection direction,
            long windowSeconds,
            int topN) {
        ThrottleDirection resolvedDir = direction == null ? ThrottleDirection.OUTPUT : direction;
        long resolvedWinSec = windowSeconds == 3600L ? 3600L : 300L;
        int resolvedTop = Math.max(1, Math.min(50, topN <= 0 ? 5 : topN));

        long windowEndUtc = clock.millis();
        long windowStartUtc = windowEndUtc - TimeUnit.SECONDS.toMillis(resolvedWinSec);

        List<TaskDto> authorizedTasks = (chartViewService != null && user != null)
                ? chartViewService.getViewTaskDtoByUser(user)
                : List.of();
        Map<String, TaskDto> taskById = new LinkedHashMap<>();
        if (authorizedTasks != null) {
            for (TaskDto t : authorizedTasks) {
                if (t == null || t.getId() == null) {
                    continue;
                }
                String kind = TaskThrottlePolicyService.normalizeSupportedKindOrNull(t.getSyncType());
                if (kind != null && RANKABLE_TASK_KINDS.contains(kind)) {
                    taskById.put(t.getId().toHexString(), t);
                }
            }
        }

        List<TaskThrottleRankingResponse.RankingItem> qualified = new ArrayList<>();
        List<TaskThrottleRankingResponse.ExcludedItem> excluded = new ArrayList<>();

        if (mongo != null && !taskById.isEmpty()) {
            Query query = Query.query(Criteria.where("taskId").in(taskById.keySet())
                    .and("meterVersion").is(ThrottleConstants.METER_VERSION)
                    .and("direction").is(resolvedDir)
                    .and("windowEndUtc").gte(windowStartUtc)
                    .and("windowStartUtc").lte(windowEndUtc));
            List<TaskThrottleTelemetryEntity> allSamples = mongo.find(query, TaskThrottleTelemetryEntity.class);
            Map<String, List<TaskThrottleTelemetryEntity>> byTask = new HashMap<>();
            if (allSamples != null) {
                for (TaskThrottleTelemetryEntity s : allSamples) {
                    byTask.computeIfAbsent(s.getTaskId(), k -> new ArrayList<>()).add(s);
                }
            }

            Map<String, Boolean> throttleEnabledMap = loadThrottleEnabledFlags(taskById.keySet());

            for (Map.Entry<String, TaskDto> entry : taskById.entrySet()) {
                String taskId = entry.getKey();
                TaskDto task = entry.getValue();
                List<TaskThrottleTelemetryEntity> taskSamples = byTask.getOrDefault(taskId, List.of());

                List<long[]> validIntervals = new ArrayList<>();
                long totalValidBytes = 0L;
                boolean sawUnaligned = false;
                boolean sawMissingBytes = false;

                for (TaskThrottleTelemetryEntity s : taskSamples) {
                    if (s.getTimeQuality() == TimeQuality.UNALIGNED) {
                        sawUnaligned = true;
                        continue;
                    }
                    if (s.getSampleQuality() != SampleQuality.VALID || s.getEstimatedAdmittedBytes() == null) {
                        sawMissingBytes = true;
                        continue;
                    }
                    long clippedStart = Math.max(windowStartUtc, s.getWindowStartUtc());
                    long clippedEnd = Math.min(windowEndUtc, s.getWindowEndUtc());
                    if (clippedEnd > clippedStart) {
                        validIntervals.add(new long[]{clippedStart, clippedEnd});
                        totalValidBytes += Math.max(0L, s.getEstimatedAdmittedBytes());
                    }
                }

                double coveredSec = mergeIntervalUnionMillis(validIntervals) / 1000.0d;
                double coverageRatio = Math.min(1.0d, coveredSec / resolvedWinSec);

                if (coveredSec <= 0.0d || coverageRatio < MIN_RANKING_COVERAGE_RATIO) {
                    String reason = sawUnaligned
                            ? "CLOCK_UNALIGNED"
                            : (sawMissingBytes ? "MISSING_BYTES_OR_SAMPLE" : "COVERAGE_BELOW_90_PERCENT");
                    excluded.add(TaskThrottleRankingResponse.ExcludedItem.builder()
                            .taskId(taskId)
                            .taskName(task.getName())
                            .taskKind(TaskThrottlePolicyService.normalizeSupportedKindOrNull(task.getSyncType()))
                            .coveredSeconds(coveredSec)
                            .coverageRatio(coverageRatio)
                            .reason(reason)
                            .build());
                    continue;
                }

                double bytesPerSec = totalValidBytes / coveredSec;
                double mbPerSec = bytesPerSec / ThrottleConstants.BYTES_PER_MB;
                qualified.add(TaskThrottleRankingResponse.RankingItem.builder()
                        .taskId(taskId)
                        .taskName(task.getName())
                        .taskKind(TaskThrottlePolicyService.normalizeSupportedKindOrNull(task.getSyncType()))
                        .estimatedBytesPerSec(bytesPerSec)
                        .estimatedMbPerSec(mbPerSec)
                        .totalEstimatedBytes(totalValidBytes)
                        .coveredSeconds(coveredSec)
                        .coverageRatio(coverageRatio)
                        .throttleEnabled(throttleEnabledMap.getOrDefault(taskId, false))
                        .liveStatus(evaluateLiveStatus(taskId))
                        .build());
            }
        }

        qualified.sort(Comparator.comparingDouble(TaskThrottleRankingResponse.RankingItem::getEstimatedBytesPerSec).reversed());
        List<TaskThrottleRankingResponse.RankingItem> topItems = qualified.size() > resolvedTop
                ? new ArrayList<>(qualified.subList(0, resolvedTop))
                : qualified;

        return TaskThrottleRankingResponse.builder()
                .meterVersion(ThrottleConstants.METER_VERSION)
                .direction(resolvedDir)
                .windowSeconds(resolvedWinSec)
                .minCoverageRatio(MIN_RANKING_COVERAGE_RATIO)
                .items(topItems)
                .excluded(excluded)
                .build();
    }

    private Map<String, Boolean> loadThrottleEnabledFlags(Set<String> taskIds) {
        Map<String, Boolean> map = new HashMap<>();
        if (mongo == null || taskIds.isEmpty()) {
            return map;
        }
        List<TaskThrottlePolicyEntity> policies = mongo.find(
                Query.query(Criteria.where("taskId").in(taskIds)), TaskThrottlePolicyEntity.class);
        if (policies != null) {
            for (TaskThrottlePolicyEntity p : policies) {
                map.put(p.getTaskId(), TaskThrottleExecutionGuardService.hasEnabledPhase(p));
            }
        }
        return map;
    }

    private Map<ThrottlePhase, ThrottlePhaseLimit> resolvePhasesForVersion(TaskThrottlePolicyEntity policy, long version) {
        if (policy.getDesiredVersion() == version) {
            return policy.getPhases();
        }
        if (mongo != null) {
            String changeDedup = "CHANGE:" + policy.getId() + ":" + version;
            TaskThrottleChangeLogEntity change = mongo.findOne(
                    Query.query(Criteria.where("dedupKey").is(changeDedup)),
                    TaskThrottleChangeLogEntity.class);
            if (change != null && change.getAfterPhases() != null) {
                return change.getAfterPhases();
            }
        }
        return policy.getPhases();
    }

    static long mergeIntervalUnionMillis(List<long[]> intervals) {
        if (intervals == null || intervals.isEmpty()) {
            return 0L;
        }
        intervals.sort(Comparator.comparingLong((long[] a) -> a[0]).thenComparingLong(a -> a[1]));
        long total = 0L;
        long curStart = intervals.get(0)[0];
        long curEnd = intervals.get(0)[1];
        for (int i = 1; i < intervals.size(); i++) {
            long[] next = intervals.get(i);
            if (next[0] <= curEnd) {
                curEnd = Math.max(curEnd, next[1]);
            } else {
                total += (curEnd - curStart);
                curStart = next[0];
                curEnd = next[1];
            }
        }
        total += (curEnd - curStart);
        return Math.max(0L, total);
    }

    private static String extractTaskIdFromKey(String taskKey) {
        int idx = taskKey.lastIndexOf(':');
        return (idx >= 0 && idx + 1 < taskKey.length()) ? taskKey.substring(idx + 1) : taskKey;
    }

    private static final class LiveTaskTracker {
        private final String taskId;
        private long latestRunEpoch = 0L;
        private long lastReceivedAtUtc = 0L;
        private final Map<String, SlotWait> slots = new HashMap<>();

        LiveTaskTracker(String taskId) {
            this.taskId = taskId;
        }

        synchronized void updateFromSample(TaskThrottleTelemetrySample sample, long nowUtc) {
            if (sample.getRunEpoch() > latestRunEpoch) {
                latestRunEpoch = sample.getRunEpoch();
                slots.clear();
            } else if (sample.getRunEpoch() < latestRunEpoch) {
                return;
            }
            String slotKey = sample.getPhase() + ":" + sample.getDirection();
            SlotWait prev = slots.get(slotKey);
            if (prev != null && sample.getSampleSeq() <= prev.sampleSeq) {
                return;
            }
            Set<ThrottleDimension> dims = (sample.getWaitingDimensions() == null || sample.getWaitingDimensions().isEmpty())
                    ? EnumSet.noneOf(ThrottleDimension.class)
                    : EnumSet.copyOf(sample.getWaitingDimensions());
            slots.put(slotKey, new SlotWait(
                    sample.getPhase(),
                    sample.getDirection(),
                    sample.getSampleSeq(),
                    sample.getWaitReason() == null ? WaitReason.NONE : sample.getWaitReason(),
                    dims,
                    sample.getAppliedVersion(),
                    sample.getBufferWatermark(),
                    sample.getLogPositionStatus() == null ? LogPositionStatus.NORMAL : sample.getLogPositionStatus(),
                    nowUtc
            ));
            lastReceivedAtUtc = Math.max(lastReceivedAtUtc, nowUtc);
        }

        synchronized TaskThrottleLiveStatusView toView(long nowUtc) {
            boolean unknown = lastReceivedAtUtc > 0L && (nowUtc - lastReceivedAtUtc > LIVE_UNKNOWN_EXPIRE_MILLIS);
            List<TaskThrottleLiveStatusView.ActiveWaitDetail> activeWaits = new ArrayList<>();
            Map<ThrottlePhase, Set<ThrottleDimension>> byPhase = new EnumMap<>(ThrottlePhase.class);
            boolean anyRateWait = false;

            if (!unknown) {
                for (SlotWait sw : slots.values()) {
                    if (sw.waitReason == WaitReason.RATE_WAIT && !sw.waitingDimensions.isEmpty()) {
                        anyRateWait = true;
                        activeWaits.add(TaskThrottleLiveStatusView.ActiveWaitDetail.builder()
                                .phase(sw.phase)
                                .direction(sw.direction)
                                .waitReason(sw.waitReason)
                                .waitingDimensions(EnumSet.copyOf(sw.waitingDimensions))
                                .appliedVersion(sw.appliedVersion)
                                .bufferWatermark(sw.bufferWatermark)
                                .logPositionStatus(sw.logPositionStatus)
                                .build());
                        byPhase.computeIfAbsent(sw.phase, p -> EnumSet.noneOf(ThrottleDimension.class))
                                .addAll(sw.waitingDimensions);
                    }
                }
            }

            return TaskThrottleLiveStatusView.builder()
                    .taskId(taskId)
                    .throttlingActive(anyRateWait)
                    .statusUnknown(unknown)
                    .policyConfigured(false)
                    .configuredPhases(Set.of())
                    .runEpoch(latestRunEpoch)
                    .lastReportedAtUtc(lastReceivedAtUtc > 0L ? lastReceivedAtUtc : null)
                    .activeWaits(activeWaits)
                    .activeDimensionsByPhase(byPhase)
                    .build();
        }
    }

    private record SlotWait(
            ThrottlePhase phase,
            ThrottleDirection direction,
            long sampleSeq,
            WaitReason waitReason,
            Set<ThrottleDimension> waitingDimensions,
            long appliedVersion,
            double bufferWatermark,
            LogPositionStatus logPositionStatus,
            long receivedAtUtc) {
    }
}

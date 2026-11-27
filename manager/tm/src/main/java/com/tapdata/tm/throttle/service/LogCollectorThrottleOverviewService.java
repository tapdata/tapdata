package com.tapdata.tm.throttle.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.LogPositionStatus;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.WaitReason;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskServiceImpl;
import com.tapdata.tm.throttle.dto.LogCollectorThrottleOverviewView;
import com.tapdata.tm.throttle.dto.TaskThrottleLiveStatusView;
import com.tapdata.tm.throttle.dto.TaskThrottlePolicyView;
import com.tapdata.tm.throttle.entity.TaskThrottlePolicyEntity;
import com.tapdata.tm.throttle.entity.TaskThrottleTelemetryEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides permission-aware monitoring aggregation for share-mining (logCollector) tasks,
 * strictly separating source capture, shared log write, and per-consumer replay lags/statuses.
 */
@Service
@Slf4j
public class LogCollectorThrottleOverviewService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MongoOperations mongo;
    private final TaskThrottlePolicyService policyService;
    private final TaskThrottleTelemetryAndRankingService telemetryService;
    private final Clock clock;

    @Autowired(required = false)
    private TaskServiceImpl taskService;

    @Autowired
    public LogCollectorThrottleOverviewService(
            MongoOperations mongo,
            TaskThrottlePolicyService policyService,
            TaskThrottleTelemetryAndRankingService telemetryService) {
        this(mongo, policyService, telemetryService, Clock.systemUTC());
    }

    public LogCollectorThrottleOverviewService(
            MongoOperations mongo,
            TaskThrottlePolicyService policyService,
            TaskThrottleTelemetryAndRankingService telemetryService,
            Clock clock) {
        this.mongo = mongo;
        this.policyService = policyService;
        this.telemetryService = telemetryService;
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    public void setTaskService(TaskServiceImpl taskService) {
        this.taskService = taskService;
    }

    public LogCollectorThrottleOverviewView getOverview(String collectorTaskId, UserDetail user) {
        TaskDto collector = policyService.validateTaskAccess(collectorTaskId, "logCollector", user);
        TaskThrottlePolicyView policyView = policyService.getPolicyView("logCollector", collectorTaskId, user);

        Set<String> connectionIds = new LinkedHashSet<>();
        int storageTimeDays = 3;
        DAG dag = collector.getDag();
        if (dag != null && dag.getSources() != null) {
            for (Node<?> node : dag.getSources()) {
                if (node instanceof LogCollectorNode lcn) {
                    if (lcn.getConnectionIds() != null) {
                        connectionIds.addAll(lcn.getConnectionIds());
                    }
                    if (lcn.getStorageTime() != null && lcn.getStorageTime() > 0) {
                        storageTimeDays = lcn.getStorageTime();
                    }
                }
            }
        }

        TaskThrottleTelemetryEntity inputLatest = findLatestTelemetry(collectorTaskId, ThrottlePhase.CDC, ThrottleDirection.INPUT);
        TaskThrottleTelemetryEntity outputLatest = findLatestTelemetry(collectorTaskId, ThrottlePhase.CDC, ThrottleDirection.OUTPUT);

        int totalCount = 0;
        List<LogCollectorThrottleOverviewView.ConsumerThrottleSummary> visibleConsumers = new ArrayList<>();
        Long oldestReplayTime = null;

        if (taskService != null && !connectionIds.isEmpty()) {
            Criteria consumerCriteria = Criteria.where("is_deleted").is(false)
                    .and("shareCdcEnable").is(true)
                    .and("syncType").in("sync", "migrate")
                    .and("dag.nodes").elemMatch(Criteria.where("catalog").is("data").and("connectionId").in(connectionIds));

            totalCount = (int) Math.max(0L, taskService.count(new Query(consumerCriteria)));
            List<TaskDto> authorized = taskService.findAllDto(new Query(consumerCriteria), user);
            if (authorized != null && !authorized.isEmpty()) {
                Set<String> visibleIds = new HashSet<>();
                for (TaskDto t : authorized) {
                    if (t != null && t.getId() != null) {
                        visibleIds.add(t.getId().toHexString());
                    }
                }
                Map<String, Boolean> enabledMap = loadThrottleEnabledMap(visibleIds);
                long nowMs = clock.millis();

                for (TaskDto consumer : authorized) {
                    if (consumer == null || consumer.getId() == null) {
                        continue;
                    }
                    String cid = consumer.getId().toHexString();
                    TaskThrottleLiveStatusView live = telemetryService.evaluateLiveStatus(cid);
                    TaskThrottleTelemetryEntity outTelemetry = findLatestTelemetry(cid, ThrottlePhase.CDC, ThrottleDirection.OUTPUT);
                    LogCollectorThrottleOverviewView.DirectionSnapshot outSnap = toDirectionSnapshot(outTelemetry);

                    Long eventTime = extractOldestEventTimeMs(consumer.getAttrs());
                    Long delayMs = (eventTime != null && eventTime > 0L) ? Math.max(0L, nowMs - eventTime) : null;
                    if (eventTime != null && (oldestReplayTime == null || eventTime < oldestReplayTime)) {
                        oldestReplayTime = eventTime;
                    }

                    LogPositionStatus posStatus = resolveConsumerPositionStatus(outTelemetry, live);
                    visibleConsumers.add(LogCollectorThrottleOverviewView.ConsumerThrottleSummary.builder()
                            .taskId(cid)
                            .taskName(consumer.getName())
                            .syncType(consumer.getSyncType())
                            .status(consumer.getStatus())
                            .consumerDelayMs(delayMs)
                            .lastEventTime(eventTime)
                            .throttleEnabled(enabledMap.getOrDefault(cid, false))
                            .liveStatus(live)
                            .logPositionStatus(posStatus)
                            .estimatedOutputMbPerSec(outSnap != null ? outSnap.getEstimatedMbPerSec() : null)
                            .build());
                }
            }
        }

        int hiddenCount = Math.max(0, totalCount - visibleConsumers.size());
        return LogCollectorThrottleOverviewView.builder()
                .collectorTaskId(collectorTaskId)
                .collectorTaskName(collector.getName())
                .collectorStatus(collector.getStatus())
                .storageTimeDays(storageTimeDays)
                .policyView(policyView)
                .sourceCapture(toDirectionSnapshot(inputLatest))
                .logWrite(toDirectionSnapshot(outputLatest))
                .oldestConsumerReplayTime(oldestReplayTime)
                .totalConsumerCount(Math.max(totalCount, visibleConsumers.size()))
                .hiddenConsumerCount(hiddenCount)
                .visibleConsumers(visibleConsumers)
                .build();
    }

    private TaskThrottleTelemetryEntity findLatestTelemetry(String taskId, ThrottlePhase phase, ThrottleDirection direction) {
        if (mongo == null || taskId == null) {
            return null;
        }
        Query q = Query.query(Criteria.where("taskId").is(taskId)
                        .and("phase").is(phase)
                        .and("direction").is(direction))
                .with(Sort.by(Sort.Direction.DESC, "windowEndUtc"))
                .limit(1);
        return mongo.findOne(q, TaskThrottleTelemetryEntity.class);
    }

    private Map<String, Boolean> loadThrottleEnabledMap(Set<String> taskIds) {
        if (mongo == null || taskIds.isEmpty()) {
            return Collections.emptyMap();
        }
        List<TaskThrottlePolicyEntity> policies = mongo.find(
                Query.query(Criteria.where("taskId").in(taskIds)), TaskThrottlePolicyEntity.class);
        Map<String, Boolean> map = new HashMap<>();
        if (policies != null) {
            for (TaskThrottlePolicyEntity entity : policies) {
                map.put(entity.getTaskId(), TaskThrottleExecutionGuardService.hasEnabledPhase(entity));
            }
        }
        return map;
    }

    private static LogCollectorThrottleOverviewView.DirectionSnapshot toDirectionSnapshot(TaskThrottleTelemetryEntity entity) {
        if (entity == null) {
            return null;
        }
        double durationSec = entity.getDurationNanos() > 0L
                ? entity.getDurationNanos() / 1_000_000_000.0d
                : (entity.getWindowEndUtc() > entity.getWindowStartUtc()
                ? (entity.getWindowEndUtc() - entity.getWindowStartUtc()) / 1000.0d
                : 0.0d);
        Double qps = durationSec > 0.0d ? (entity.getAdmittedEvents() / durationSec) : null;
        Double mbPerSec = (durationSec > 0.0d && entity.getEstimatedAdmittedBytes() != null)
                ? ((entity.getEstimatedAdmittedBytes() / durationSec) / ThrottleConstants.BYTES_PER_MB)
                : null;
        return LogCollectorThrottleOverviewView.DirectionSnapshot.builder()
                .estimatedMbPerSec(mbPerSec)
                .qps(qps)
                .waitReason(entity.getWaitReason() == null ? WaitReason.NONE : entity.getWaitReason())
                .waitingDimensions(entity.getWaitingDimensions() == null
                        ? Collections.emptySet()
                        : Collections.unmodifiableSet(entity.getWaitingDimensions()))
                .logPositionStatus(entity.getLogPositionStatus() == null ? LogPositionStatus.UNKNOWN : entity.getLogPositionStatus())
                .lastSampledAtUtc(entity.getWindowEndUtc() > 0L ? entity.getWindowEndUtc() : null)
                .build();
    }

    private static LogPositionStatus resolveConsumerPositionStatus(
            TaskThrottleTelemetryEntity telemetry,
            TaskThrottleLiveStatusView live) {
        if (telemetry != null && telemetry.getLogPositionStatus() != null) {
            return telemetry.getLogPositionStatus();
        }
        LogPositionStatus worstLive = null;
        if (live != null && live.getActiveWaits() != null) {
            for (TaskThrottleLiveStatusView.ActiveWaitDetail detail : live.getActiveWaits()) {
                if (detail == null || detail.getLogPositionStatus() == null) {
                    continue;
                }
                if (worstLive == null || severityRank(detail.getLogPositionStatus()) > severityRank(worstLive)) {
                    worstLive = detail.getLogPositionStatus();
                }
            }
        }
        return worstLive != null ? worstLive : LogPositionStatus.UNKNOWN;
    }

    private static int severityRank(LogPositionStatus status) {
        if (status == null) {
            return -1;
        }
        return switch (status) {
            case UNKNOWN -> 0;
            case NORMAL -> 1;
            case WARN_70 -> 2;
            case CRITICAL_90 -> 3;
            case EXPIRED -> 4;
        };
    }

    private static Long extractOldestEventTimeMs(Map<String, Object> attrs) {
        if (attrs == null || attrs.isEmpty()) {
            return null;
        }
        Long oldest = null;
        Object direct = attrs.get("eventTime");
        if (direct instanceof Number n && n.longValue() > 0L) {
            oldest = n.longValue();
        }
        Object syncProgressObj = attrs.get("syncProgress");
        if (syncProgressObj instanceof Map<?, ?> progressMap) {
            for (Object val : progressMap.values()) {
                Long candidate = extractEventTimeFromProgressEntry(val);
                if (candidate != null && candidate > 0L && (oldest == null || candidate < oldest)) {
                    oldest = candidate;
                }
            }
        }
        return oldest;
    }

    private static Long extractEventTimeFromProgressEntry(Object val) {
        if (val == null) {
            return null;
        }
        try {
            if (val instanceof Map<?, ?> m) {
                Object ts = m.get("eventTime");
                return ts instanceof Number n ? n.longValue() : null;
            }
            if (val instanceof String json && json.trim().startsWith("{")) {
                Map<?, ?> parsed = MAPPER.readValue(json, Map.class);
                Object ts = parsed.get("eventTime");
                return ts instanceof Number n ? n.longValue() : null;
            }
        } catch (Exception ignored) {
            // Best-effort extraction from syncProgress metadata
        }
        return null;
    }
}

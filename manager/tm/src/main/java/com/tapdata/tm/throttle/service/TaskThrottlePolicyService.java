package com.tapdata.tm.throttle.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.TaskThrottlePolicyDto;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskServiceImpl;
import com.tapdata.tm.throttle.dto.TaskThrottleLiveStatusView;
import com.tapdata.tm.throttle.dto.TaskThrottlePolicyUpdateRequest;
import com.tapdata.tm.throttle.dto.TaskThrottlePolicyView;
import com.tapdata.tm.throttle.dto.TaskThrottleVersionConflictException;
import com.tapdata.tm.throttle.entity.TaskThrottleChangeLogEntity;
import com.tapdata.tm.throttle.entity.TaskThrottlePolicyEntity;
import com.tapdata.tm.throttle.entity.TaskThrottleStopEvidenceEntity;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Control-plane policy management service:
 * - enforces supported task kinds (sync, migrate, logCollector), path checks, tenant ACLs, and rate bounds,
 * - performs single-document CAS updates with HTTP 409 conflict views,
 * - persists bounded audit outbox entries on the policy document and flushes them idempotently to TaskThrottleChangeLog,
 * - synchronizes copy/import/delete lifecycle hooks.
 */
@Service
@Slf4j
public class TaskThrottlePolicyService {

    public static final long MIN_BYTES_PER_SEC = 1_000L;
    public static final long MAX_BYTES_PER_SEC = 10_000_000_000L;
    public static final long MIN_EVENTS_PER_SEC = 1L;
    public static final long MAX_EVENTS_PER_SEC = 10_000_000L;
    public static final String ERR_AUDIT_OUTBOX_FULL = "AUDIT_OUTBOX_FULL";

    private final MongoOperations mongo;
    private final Clock clock;
    private final AtomicLong deltaSeqClock = new AtomicLong(1L);

    @Autowired(required = false)
    private TaskServiceImpl taskService;
    @Autowired(required = false)
    private TaskThrottleExecutionGuardService executionGuardService;
    @Autowired(required = false)
    private TaskThrottleTelemetryAndRankingService telemetryAndRankingService;

    @Autowired
    public TaskThrottlePolicyService(MongoOperations mongo) {
        this(mongo, Clock.systemUTC());
    }

    public TaskThrottlePolicyService(MongoOperations mongo, Clock clock) {
        this.mongo = mongo;
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    public void setTaskService(TaskServiceImpl taskService) {
        this.taskService = taskService;
    }

    public void setExecutionGuardService(TaskThrottleExecutionGuardService executionGuardService) {
        this.executionGuardService = executionGuardService;
    }

    public void setTelemetryAndRankingService(TaskThrottleTelemetryAndRankingService telemetryAndRankingService) {
        this.telemetryAndRankingService = telemetryAndRankingService;
    }

    public static String normalizeSupportedKindOrNull(String rawKind) {
        if (rawKind == null || rawKind.isBlank()) {
            return null;
        }
        String k = rawKind.trim();
        if ("sync".equalsIgnoreCase(k)) {
            return "sync";
        }
        if ("migrate".equalsIgnoreCase(k)) {
            return "migrate";
        }
        if ("logCollector".equalsIgnoreCase(k) || "log_collector".equalsIgnoreCase(k)) {
            return "logCollector";
        }
        return null;
    }

    public String normalizeAndValidateKind(String rawKind) {
        String normalized = normalizeSupportedKindOrNull(rawKind);
        if (normalized == null) {
            throw new BizException(ThrottleConstants.ERR_UNSUPPORTED_TASK_KIND,
                    "Unsupported task kind for throttling: " + rawKind);
        }
        return normalized;
    }

    public TaskDto validateTaskAccess(String taskId, String normalizedKind, UserDetail user) {
        if (taskId == null || taskId.isBlank()) {
            throw new BizException(ThrottleConstants.ERR_UNSUPPORTED_PATH, "taskId must not be blank");
        }
        if (taskService == null) {
            // Allow isolated policy-unit runs without full TaskService bean; build synthetic TaskDto guard
            TaskDto synthetic = new TaskDto();
            if (ObjectId.isValid(taskId)) {
                synthetic.setId(new ObjectId(taskId));
            }
            synthetic.setSyncType(normalizedKind);
            if (user != null) {
                synthetic.setCustomId(user.getCustomerId());
                synthetic.setUserId(user.getUserId());
            }
            return synthetic;
        }
        ObjectId oid;
        try {
            oid = new ObjectId(taskId.trim());
        } catch (IllegalArgumentException ex) {
            throw new BizException(ThrottleConstants.ERR_UNSUPPORTED_PATH, "Invalid taskId: " + taskId);
        }
        TaskDto taskDto = user != null
                ? taskService.checkExistById(oid, user)
                : taskService.findByTaskId(oid);
        if (taskDto == null) {
            throw new BizException("Task.NotFound", "Task not found or not accessible: " + taskId);
        }
        String actualKind = normalizeSupportedKindOrNull(taskDto.getSyncType());
        if (actualKind == null) {
            throw new BizException(ThrottleConstants.ERR_UNSUPPORTED_TASK_KIND,
                    "Task syncType does not support throttling: " + taskDto.getSyncType());
        }
        if (!actualKind.equals(normalizedKind)) {
            throw new BizException(ThrottleConstants.ERR_UNSUPPORTED_TASK_KIND,
                    "Task kind mismatch: expected=" + actualKind + ", requested=" + normalizedKind);
        }
        Map<String, Object> attrs = taskDto.getAttrs();
        if (attrs != null && (Boolean.TRUE.equals(attrs.get("crossJvm"))
                || Boolean.TRUE.equals(attrs.get("unsupportedThrottlePath")))) {
            throw new BizException(ThrottleConstants.ERR_UNSUPPORTED_PATH,
                    "Current execution path does not support task throttling");
        }
        return taskDto;
    }

    public void validatePolicyPhases(Map<ThrottlePhase, ThrottlePhaseLimit> phases, String taskMode) {
        if (phases == null || phases.isEmpty()) {
            return;
        }
        for (Map.Entry<ThrottlePhase, ThrottlePhaseLimit> entry : phases.entrySet()) {
            ThrottlePhase phase = entry.getKey();
            ThrottlePhaseLimit limit = entry.getValue();
            if (phase == null || limit == null || !limit.isEnabled()) {
                continue;
            }
            if ("initial_sync".equalsIgnoreCase(taskMode) && phase == ThrottlePhase.CDC) {
                throw new BizException(ThrottleConstants.ERR_INVALID_RATE, "CDC throttling cannot be enabled on initial_sync-only task");
            }
            if ("cdc".equalsIgnoreCase(taskMode) && phase == ThrottlePhase.FULL) {
                throw new BizException(ThrottleConstants.ERR_INVALID_RATE, "FULL throttling cannot be enabled on cdc-only task");
            }
            Long bytes = limit.getBytesPerSec();
            Long events = limit.getEventsPerSec();
            if (bytes == null && events == null) {
                throw new BizException(ThrottleConstants.ERR_INVALID_RATE,
                        "Enabled throttle phase " + phase + " requires bytesPerSec or eventsPerSec");
            }
            if (bytes != null && (bytes < MIN_BYTES_PER_SEC || bytes > MAX_BYTES_PER_SEC)) {
                throw new BizException(ThrottleConstants.ERR_INVALID_RATE,
                        "bytesPerSec out of range [" + MIN_BYTES_PER_SEC + ", " + MAX_BYTES_PER_SEC + "]");
            }
            if (events != null && (events < MIN_EVENTS_PER_SEC || events > MAX_EVENTS_PER_SEC)) {
                throw new BizException(ThrottleConstants.ERR_INVALID_RATE,
                        "eventsPerSec out of range [" + MIN_EVENTS_PER_SEC + ", " + MAX_EVENTS_PER_SEC + "]");
            }
        }
    }

    public TaskThrottlePolicyView getPolicyView(String rawKind, String taskId, UserDetail user) {
        String kind = normalizeAndValidateKind(rawKind);
        TaskDto taskDto = validateTaskAccess(taskId, kind, user);
        String tenantId = resolveTenantId(taskDto, user);
        String taskKey = TaskThrottlePolicyEntity.buildTaskKey(tenantId, kind, taskId);

        TaskThrottlePolicyEntity entity = mongo == null ? null : mongo.findOne(
                Query.query(Criteria.where("_id").is(taskKey)), TaskThrottlePolicyEntity.class);
        if (entity == null && mongo != null) {
            entity = mongo.findOne(Query.query(Criteria.where("taskId").is(taskId)), TaskThrottlePolicyEntity.class);
        }
        return toView(entity, tenantId, kind, taskId, taskKey);
    }

    /**
     * Atomically updates the desired throttling policy via CAS on expectedVersion
     * and persists a bounded audit outbox entry in the same Mongo document operation.
     */
    public TaskThrottlePolicyView updatePolicy(
            String rawKind,
            String taskId,
            TaskThrottlePolicyUpdateRequest req,
            UserDetail user) {
        if (req == null) {
            throw new BizException(ThrottleConstants.ERR_INVALID_RATE, "Request body is required");
        }
        String kind = normalizeAndValidateKind(rawKind);
        TaskDto taskDto = validateTaskAccess(taskId, kind, user);
        validatePolicyPhases(req.getPhases(), taskDto.getType());

        String tenantId = resolveTenantId(taskDto, user);
        String taskKey = TaskThrottlePolicyEntity.buildTaskKey(tenantId, kind, taskId);
        String operator = user != null ? (user.getUsername() != null ? user.getUsername() : user.getUserId()) : "system";
        long now = clock.millis();
        Map<ThrottlePhase, ThrottlePhaseLimit> sanitizedPhases = copyPhases(req.getPhases());

        TaskThrottlePolicyEntity existing = mongo.findOne(
                Query.query(Criteria.where("_id").is(taskKey)), TaskThrottlePolicyEntity.class);
        if (existing != null) {
            flushOutboxForTask(existing);
            existing = mongo.findOne(Query.query(Criteria.where("_id").is(taskKey)), TaskThrottlePolicyEntity.class);
            if (existing != null && existing.getPendingAuditOutbox() != null
                    && existing.getPendingAuditOutbox().size() >= TaskThrottlePolicyEntity.MAX_OUTBOX_CAPACITY) {
                throw new BizException(ERR_AUDIT_OUTBOX_FULL,
                        "Throttle audit outbox backlog exceeded safety cap (" + TaskThrottlePolicyEntity.MAX_OUTBOX_CAPACITY + ")");
            }
        }

        boolean enablingAny = hasEnabledPhase(sanitizedPhases);
        String targetAgentId = taskDto.getAgentId() != null ? taskDto.getAgentId()
                : (existing != null ? existing.getCurrentAgentId() : null);
        boolean compatibleAgent = !enablingAny
                || targetAgentId == null
                || executionGuardService == null
                || executionGuardService.isAgentCompatible(targetAgentId, kind);

        String taskStatus = taskDto.getStatus();
        boolean taskRunningOnAgent = targetAgentId != null && !targetAgentId.isBlank()
                && (TaskDto.STATUS_RUNNING.equals(taskStatus) || TaskDto.STATUS_WAIT_RUN.equals(taskStatus));
        boolean activateRunningProtection = taskRunningOnAgent && enablingAny && compatibleAgent;

        String nextApplyStatus = compatibleAgent
                ? TaskThrottlePolicyEntity.APPLY_PENDING
                : TaskThrottlePolicyEntity.APPLY_WAITING_COMPATIBLE_AGENT;
        String nextGateStatus = compatibleAgent
                ? (existing != null && existing.getStopGateStatus() != null
                ? existing.getStopGateStatus() : TaskThrottlePolicyEntity.STOP_GATE_NONE)
                : TaskThrottlePolicyEntity.STOP_GATE_WAITING_COMPATIBLE_AGENT;
        String nextGateReason = compatibleAgent
                ? (existing != null ? existing.getStopGateReason() : null)
                : "Running/assigned Agent does not support throttling; awaiting compatible Agent";

        if (existing == null) {
            if (req.getExpectedVersion() != 0L) {
                TaskThrottlePolicyView currentEmpty = toView(null, tenantId, kind, taskId, taskKey);
                throw new TaskThrottleVersionConflictException(0L, currentEmpty);
            }
            long nextVer = 1L;
            TaskThrottlePolicyEntity.AuditOutboxEntry outbox = TaskThrottlePolicyEntity.AuditOutboxEntry.builder()
                    .entryId(UUID.randomUUID().toString())
                    .dedupKey("CHANGE:" + taskKey + ":" + nextVer)
                    .eventType(TaskThrottleChangeLogEntity.EVENT_POLICY_CHANGE)
                    .beforeDesiredVersion(0L)
                    .afterDesiredVersion(nextVer)
                    .beforePhases(null)
                    .afterPhases(sanitizedPhases)
                    .operator(operator)
                    .occurredAt(now)
                    .detail("Initial policy save")
                    .build();
            long initialRunEpoch = activateRunningProtection ? 1L : 0L;
            TaskThrottlePolicyEntity created = TaskThrottlePolicyEntity.builder()
                    .id(taskKey)
                    .tenantId(tenantId)
                    .taskKind(kind)
                    .taskId(taskId)
                    .schemaVersion(1)
                    .desiredVersion(nextVer)
                    .phases(sanitizedPhases)
                    .updatedBy(operator)
                    .updatedAt(now)
                    .currentRunEpoch(initialRunEpoch)
                    .currentAgentId(targetAgentId)
                    .protectedRunActive(activateRunningProtection)
                    .stopGateStatus(nextGateStatus)
                    .stopGateReason(nextGateReason)
                    .deltaSeq(nextDeltaSeq())
                    .appliedVersion(0L)
                    .appliedPhases(null)
                    .applyStatus(nextApplyStatus)
                    .pendingAuditOutbox(new ArrayList<>(List.of(outbox)))
                    .build();
            try {
                mongo.insert(created);
            } catch (DuplicateKeyException dup) {
                TaskThrottlePolicyEntity latest = mongo.findOne(
                        Query.query(Criteria.where("_id").is(taskKey)), TaskThrottlePolicyEntity.class);
                throw new TaskThrottleVersionConflictException(
                        latest != null ? latest.getDesiredVersion() : 0L,
                        toView(latest, tenantId, kind, taskId, taskKey));
            }
            if (activateRunningProtection) {
                syncRunningTaskThrottleAttrs(taskId, initialRunEpoch, true);
            }
            flushOutboxForTask(created);
            TaskThrottlePolicyEntity refreshed = mongo.findOne(
                    Query.query(Criteria.where("_id").is(taskKey)), TaskThrottlePolicyEntity.class);
            return toView(refreshed != null ? refreshed : created, tenantId, kind, taskId, taskKey);
        }

        long expectedVer = req.getExpectedVersion();
        if (existing.getDesiredVersion() != expectedVer) {
            throw new TaskThrottleVersionConflictException(
                    existing.getDesiredVersion(),
                    toView(existing, tenantId, kind, taskId, taskKey));
        }

        long nextVer = expectedVer + 1L;
        TaskThrottlePolicyEntity.AuditOutboxEntry outbox = TaskThrottlePolicyEntity.AuditOutboxEntry.builder()
                .entryId(UUID.randomUUID().toString())
                .dedupKey("CHANGE:" + taskKey + ":" + nextVer)
                .eventType(TaskThrottleChangeLogEntity.EVENT_POLICY_CHANGE)
                .beforeDesiredVersion(expectedVer)
                .afterDesiredVersion(nextVer)
                .beforePhases(existing.getPhases())
                .afterPhases(sanitizedPhases)
                .operator(operator)
                .occurredAt(now)
                .detail("Policy updated from v" + expectedVer + " to v" + nextVer)
                .build();

        Query casQuery = Query.query(Criteria.where("_id").is(taskKey).and("desiredVersion").is(expectedVer));
        Update casUpdate = new Update()
                .set("desiredVersion", nextVer)
                .set("phases", sanitizedPhases)
                .set("updatedBy", operator)
                .set("updatedAt", now)
                .set("deltaSeq", nextDeltaSeq())
                .set("applyStatus", nextApplyStatus)
                .set("stopGateStatus", nextGateStatus)
                .set("stopGateReason", nextGateReason)
                .push("pendingAuditOutbox", outbox);

        boolean needRunningProtectionSync = activateRunningProtection
                && (existing.getCurrentRunEpoch() <= 0L
                || !existing.isProtectedRunActive()
                || !targetAgentId.equals(existing.getCurrentAgentId()));
        long effectiveEpoch = existing.getCurrentRunEpoch() > 0L ? existing.getCurrentRunEpoch() : 1L;
        if (needRunningProtectionSync) {
            casUpdate.set("currentRunEpoch", effectiveEpoch)
                    .set("currentAgentId", targetAgentId)
                    .set("protectedRunActive", true);
        }

        TaskThrottlePolicyEntity updated = mongo.findAndModify(
                casQuery, casUpdate, FindAndModifyOptions.options().returnNew(true), TaskThrottlePolicyEntity.class);
        if (updated == null) {
            TaskThrottlePolicyEntity latest = mongo.findOne(
                    Query.query(Criteria.where("_id").is(taskKey)), TaskThrottlePolicyEntity.class);
            throw new TaskThrottleVersionConflictException(
                    latest != null ? latest.getDesiredVersion() : expectedVer,
                    toView(latest, tenantId, kind, taskId, taskKey));
        }
        if (needRunningProtectionSync) {
            syncRunningTaskThrottleAttrs(taskId, effectiveEpoch, true);
        }
        flushOutboxForTask(updated);
        TaskThrottlePolicyEntity refreshed = mongo.findOne(
                Query.query(Criteria.where("_id").is(taskKey)), TaskThrottlePolicyEntity.class);
        return toView(refreshed != null ? refreshed : updated, tenantId, kind, taskId, taskKey);
    }

    public List<TaskThrottleChangeLogEntity> listChanges(String rawKind, String taskId, UserDetail user, int page, int size) {
        String kind = normalizeAndValidateKind(rawKind);
        TaskDto taskDto = validateTaskAccess(taskId, kind, user);
        String tenantId = resolveTenantId(taskDto, user);
        String taskKey = TaskThrottlePolicyEntity.buildTaskKey(tenantId, kind, taskId);

        if (mongo == null) {
            return List.of();
        }
        TaskThrottlePolicyEntity entity = mongo.findOne(
                Query.query(Criteria.where("_id").is(taskKey)), TaskThrottlePolicyEntity.class);
        if (entity != null) {
            flushOutboxForTask(entity);
        }

        int safePage = Math.max(0, page);
        int safeSize = Math.max(1, Math.min(200, size <= 0 ? 50 : size));
        Query q = Query.query(Criteria.where("taskId").is(taskId))
                .with(Sort.by(Sort.Direction.DESC, "occurredAt"))
                .skip((long) safePage * safeSize)
                .limit(safeSize);
        List<TaskThrottleChangeLogEntity> list = mongo.find(q, TaskThrottleChangeLogEntity.class);
        return list == null ? List.of() : list;
    }

    /**
     * Copies throttling policy from a cloned source task to the newly created target task starting at desiredVersion=1.
     */
    public void copyPolicy(String sourceTaskId, TaskDto copiedTask, UserDetail user) {
        if (mongo == null || sourceTaskId == null || copiedTask == null || copiedTask.getId() == null) {
            return;
        }
        String kind = normalizeSupportedKindOrNull(copiedTask.getSyncType());
        if (kind == null) {
            return;
        }
        TaskThrottlePolicyEntity sourcePolicy = mongo.findOne(
                Query.query(Criteria.where("taskId").is(sourceTaskId)), TaskThrottlePolicyEntity.class);
        if (sourcePolicy == null || !hasEnabledPhase(sourcePolicy.getPhases())) {
            return;
        }
        TaskThrottlePolicyUpdateRequest req = TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(0L)
                .phases(copyPhases(sourcePolicy.getPhases()))
                .build();
        updatePolicy(kind, copiedTask.getId().toHexString(), req, user);
    }

    /**
     * Imports explicit throttle policy for an imported task; ignores legacy exports missing policy.
     */
    public void importPolicy(TaskDto targetTask, TaskThrottlePolicyDto dto, UserDetail user) {
        if (mongo == null || targetTask == null || targetTask.getId() == null || dto == null || dto.getPhases() == null) {
            return;
        }
        String kind = normalizeSupportedKindOrNull(targetTask.getSyncType());
        if (kind == null) {
            return;
        }
        TaskThrottlePolicyUpdateRequest req = TaskThrottlePolicyUpdateRequest.builder()
                .expectedVersion(0L)
                .phases(dto.getPhases())
                .build();
        updatePolicy(kind, targetTask.getId().toHexString(), req, user);
    }

    /**
     * Cleans up policy and live tracking when a task is deleted, preserving immutable TaskThrottleChangeLog audit rows.
     */
    public void onTaskDeleted(String taskId) {
        if (taskId == null) {
            return;
        }
        if (mongo != null) {
            mongo.remove(Query.query(Criteria.where("taskId").is(taskId)), TaskThrottlePolicyEntity.class);
            mongo.remove(Query.query(Criteria.where("taskId").is(taskId)), TaskThrottleStopEvidenceEntity.class);
        }
        if (telemetryAndRankingService != null) {
            telemetryAndRankingService.clearLiveStatus(taskId);
        }
    }

    public int flushOutboxForTask(TaskThrottlePolicyEntity entity) {
        if (mongo == null || entity == null || entity.getPendingAuditOutbox() == null || entity.getPendingAuditOutbox().isEmpty()) {
            return 0;
        }
        int flushed = 0;
        for (TaskThrottlePolicyEntity.AuditOutboxEntry entry : new ArrayList<>(entity.getPendingAuditOutbox())) {
            try {
                mongo.insert(TaskThrottleChangeLogEntity.builder()
                        .id(UUID.randomUUID().toString())
                        .dedupKey(entry.getDedupKey())
                        .tenantId(entity.getTenantId())
                        .taskKind(entity.getTaskKind())
                        .taskId(entity.getTaskId())
                        .taskKey(entity.getId())
                        .eventType(entry.getEventType())
                        .runEpoch(entity.getCurrentRunEpoch())
                        .policyVersion(entry.getAfterDesiredVersion())
                        .beforePhases(entry.getBeforePhases())
                        .afterPhases(entry.getAfterPhases())
                        .outcome("COMMITTED")
                        .reason(entry.getDetail())
                        .operator(entry.getOperator())
                        .occurredAt(entry.getOccurredAt())
                        .build());
            } catch (DuplicateKeyException ignored) {
                // Already flushed idempotently
            } catch (Exception ex) {
                log.warn("Flush throttle audit outbox failed for taskKey={}, entryId={}: {}",
                        entity.getId(), entry.getEntryId(), ex.getMessage());
                continue;
            }
            mongo.updateFirst(
                    Query.query(Criteria.where("_id").is(entity.getId())),
                    new Update().pull("pendingAuditOutbox", new org.bson.Document("entryId", entry.getEntryId())),
                    TaskThrottlePolicyEntity.class);
            flushed++;
        }
        return flushed;
    }

    private void syncRunningTaskThrottleAttrs(String taskId, long runEpoch, boolean protectedRun) {
        if (taskService == null || taskId == null || !ObjectId.isValid(taskId)) {
            return;
        }
        taskService.update(
                Query.query(Criteria.where("_id").is(new ObjectId(taskId))),
                new Update().set("attrs.throttleRunEpoch", runEpoch).set("attrs.throttleProtected", protectedRun));
    }

    private TaskThrottlePolicyView toView(
            TaskThrottlePolicyEntity entity,
            String tenantId,
            String kind,
            String taskId,
            String taskKey) {
        TaskThrottleLiveStatusView live = telemetryAndRankingService != null
                ? telemetryAndRankingService.evaluateLiveStatus(taskId)
                : null;
        if (live != null && entity != null && entity.getPhases() != null) {
            EnumSet<ThrottlePhase> enabledPhases = EnumSet.noneOf(ThrottlePhase.class);
            for (Map.Entry<ThrottlePhase, ThrottlePhaseLimit> phaseEntry : entity.getPhases().entrySet()) {
                if (phaseEntry.getKey() != null && phaseEntry.getValue() != null && phaseEntry.getValue().isEnabled()) {
                    enabledPhases.add(phaseEntry.getKey());
                }
            }
            live.setConfiguredPhases(enabledPhases);
            live.setPolicyConfigured(!enabledPhases.isEmpty());
        }
        if (entity == null) {
            return TaskThrottlePolicyView.builder()
                    .taskKey(taskKey)
                    .tenantId(tenantId)
                    .taskKind(kind)
                    .taskId(taskId)
                    .desiredVersion(0L)
                    .desiredPhases(Collections.emptyMap())
                    .appliedVersion(0L)
                    .appliedPhases(null) // Explicitly null when never confirmed applied!
                    .applyStatus(TaskThrottlePolicyEntity.APPLY_NONE)
                    .currentRunEpoch(0L)
                    .protectedRunActive(false)
                    .stopGateStatus(TaskThrottlePolicyEntity.STOP_GATE_NONE)
                    .liveStatus(live)
                    .build();
        }
        boolean hasConfirmed = entity.getAppliedVersion() > 0L || entity.getAppliedAt() != null;
        return TaskThrottlePolicyView.builder()
                .taskKey(entity.getId())
                .tenantId(entity.getTenantId())
                .taskKind(entity.getTaskKind())
                .taskId(entity.getTaskId())
                .desiredVersion(entity.getDesiredVersion())
                .desiredPhases(entity.getPhases())
                .updatedBy(entity.getUpdatedBy())
                .updatedAt(entity.getUpdatedAt())
                .appliedVersion(entity.getAppliedVersion())
                .appliedPhases(hasConfirmed ? entity.getAppliedPhases() : null)
                .appliedAt(entity.getAppliedAt())
                .applyStatus(entity.getApplyStatus())
                .lastApplyReason(entity.getLastApplyReason())
                .currentRunEpoch(entity.getCurrentRunEpoch())
                .currentAgentId(entity.getCurrentAgentId())
                .protectedRunActive(entity.isProtectedRunActive())
                .stopGateStatus(entity.getStopGateStatus())
                .stopGateReason(entity.getStopGateReason())
                .liveStatus(live)
                .build();
    }

    private static Map<ThrottlePhase, ThrottlePhaseLimit> copyPhases(Map<ThrottlePhase, ThrottlePhaseLimit> input) {
        EnumMap<ThrottlePhase, ThrottlePhaseLimit> map = new EnumMap<>(ThrottlePhase.class);
        if (input != null) {
            for (Map.Entry<ThrottlePhase, ThrottlePhaseLimit> e : input.entrySet()) {
                if (e.getKey() != null && e.getValue() != null) {
                    ThrottlePhaseLimit v = e.getValue();
                    map.put(e.getKey(), ThrottlePhaseLimit.builder()
                            .enabled(v.isEnabled())
                            .bytesPerSec(v.getBytesPerSec())
                            .eventsPerSec(v.getEventsPerSec())
                            .build());
                }
            }
        }
        return map;
    }

    private static boolean hasEnabledPhase(Map<ThrottlePhase, ThrottlePhaseLimit> phases) {
        if (phases == null) {
            return false;
        }
        for (ThrottlePhaseLimit limit : phases.values()) {
            if (limit != null && limit.isEnabled()) {
                return true;
            }
        }
        return false;
    }

    private static String resolveTenantId(TaskDto taskDto, UserDetail user) {
        if (taskDto != null && taskDto.getCustomId() != null && !taskDto.getCustomId().isBlank()) {
            return taskDto.getCustomId().trim();
        }
        if (user != null && user.getCustomerId() != null && !user.getCustomerId().isBlank()) {
            return user.getCustomerId().trim();
        }
        if (taskDto != null && taskDto.getUserId() != null && !taskDto.getUserId().isBlank()) {
            return taskDto.getUserId().trim();
        }
        if (user != null && user.getUserId() != null && !user.getUserId().isBlank()) {
            return user.getUserId().trim();
        }
        return "";
    }

    private long nextDeltaSeq() {
        long ts = clock.millis() * 1000L;
        return deltaSeqClock.accumulateAndGet(ts, (cur, proposed) -> Math.max(cur + 1L, proposed));
    }
}

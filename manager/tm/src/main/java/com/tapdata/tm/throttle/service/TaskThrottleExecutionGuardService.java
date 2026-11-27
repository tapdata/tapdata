package com.tapdata.tm.throttle.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.AgentThrottleCapabilities;
import com.tapdata.tm.commons.throttle.TaskThrottlePolicyDto;
import com.tapdata.tm.commons.throttle.TaskThrottleStopEvidenceDto;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import com.tapdata.tm.throttle.entity.AgentThrottleCapabilityEntity;
import com.tapdata.tm.throttle.entity.TaskThrottleChangeLogEntity;
import com.tapdata.tm.throttle.entity.TaskThrottlePolicyEntity;
import com.tapdata.tm.throttle.entity.TaskThrottleStopEvidenceEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enforces Agent capability compatibility, verifiable stop evidence prior to protected takeover/restart,
 * monotonic CAS runEpoch assignment, heartbeat-timeout reschedule blocking, and checkpoint write fencing.
 */
@Service
@Slf4j
public class TaskThrottleExecutionGuardService {

    private final MongoOperations mongo;
    private final Clock clock;
    private final AtomicLong deltaSeqClock = new AtomicLong(1L);

    @Autowired
    public TaskThrottleExecutionGuardService(MongoOperations mongo) {
        this(mongo, Clock.systemUTC());
    }

    public TaskThrottleExecutionGuardService(MongoOperations mongo, Clock clock) {
        this.mongo = mongo;
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    public AgentThrottleCapabilityEntity registerCapabilities(AgentThrottleCapabilities cap) {
        if (cap == null || cap.getAgentId() == null || cap.getAgentId().isBlank()) {
            throw new BizException(ThrottleConstants.ERR_UNSUPPORTED_PATH, "agentId must not be blank");
        }
        long now = clock.millis();
        Query q = Query.query(Criteria.where("_id").is(cap.getAgentId().trim()));
        Update u = new Update()
                .set("_id", cap.getAgentId().trim())
                .set("schemaVersion", cap.getSchemaVersion())
                .set("throttleSupported", cap.isThrottleSupported())
                .set("meterVersion", cap.getMeterVersion())
                .set("supportedTaskKinds", cap.getSupportedTaskKinds())
                .set("supportedPaths", cap.getSupportedPaths())
                .set("supportedDimensions", cap.getSupportedDimensions())
                .set("cdcSampleConfigVersion", cap.getCdcSampleConfigVersion())
                .set("cdcSampleRefreshIntervalSeconds", cap.getCdcSampleRefreshIntervalSeconds())
                .set("reportedAt", cap.getReportedAt() > 0L ? cap.getReportedAt() : now)
                .set("updatedAt", now);
        mongo.upsert(q, u, AgentThrottleCapabilityEntity.class);
        return mongo.findOne(q, AgentThrottleCapabilityEntity.class);
    }

    public boolean isAgentCompatible(String agentId, String taskKind) {
        if (agentId == null || agentId.isBlank()) {
            return false;
        }
        AgentThrottleCapabilityEntity cap = mongo.findOne(
                Query.query(Criteria.where("_id").is(agentId.trim())),
                AgentThrottleCapabilityEntity.class);
        if (cap == null || !cap.isThrottleSupported()) {
            return false;
        }
        if (!ThrottleConstants.METER_VERSION.equals(cap.getMeterVersion())) {
            return false;
        }
        String normalizedKind = (taskKind == null || taskKind.isBlank()) ? "sync" : taskKind.trim();
        Set<String> kinds = cap.getSupportedTaskKinds();
        return kinds == null || kinds.isEmpty() || kinds.contains(normalizedKind);
    }

    public List<String> filterCompatibleAgentIds(List<String> candidateAgentIds, String taskKind) {
        if (candidateAgentIds == null || candidateAgentIds.isEmpty()) {
            return List.of();
        }
        List<String> compatible = new ArrayList<>();
        for (String id : candidateAgentIds) {
            if (isAgentCompatible(id, taskKind)) {
                compatible.add(id);
            }
        }
        return compatible;
    }

    /**
     * Submits stop evidence (either graceful stop receipt from old Agent or verified manual operator evidence).
     * Verifies epoch match, required exit/write-settled flags, and stores idempotently for CAS takeover consumption.
     */
    public TaskThrottleStopEvidenceEntity submitStopEvidence(TaskThrottleStopEvidenceDto dto, boolean operatorManual) {
        if (dto == null || isBlank(dto.getTaskId()) || isBlank(dto.getOldAgentId())
                || dto.getOldRunEpoch() <= 0L || isBlank(dto.getEvidenceRef())) {
            throw new BizException(ThrottleConstants.ERR_STOP_EVIDENCE_REQUIRED,
                    "Stop evidence requires taskId, oldAgentId, oldRunEpoch > 0, and evidenceRef");
        }
        if (!dto.isWriteSettled()) {
            throw new BizException(ThrottleConstants.ERR_STOP_EVIDENCE_REQUIRED,
                    "Stop evidence requires confirmed writeSettled=true before releasing takeover gate");
        }
        if (operatorManual && !dto.isProcessExited()) {
            throw new BizException(ThrottleConstants.ERR_STOP_EVIDENCE_REQUIRED,
                    "Manual stop evidence requires verified processExited=true");
        }

        TaskThrottlePolicyEntity policy = findPolicyByTaskId(dto.getTaskId().trim());
        if (policy == null) {
            throw new BizException(ThrottleConstants.ERR_STALE_RUN_EPOCH, "No protected throttle run found for task " + dto.getTaskId());
        }
        if (policy.getCurrentRunEpoch() != dto.getOldRunEpoch()
                || (policy.getCurrentAgentId() != null && !policy.getCurrentAgentId().equals(dto.getOldAgentId().trim()))) {
            throw new BizException(ThrottleConstants.ERR_STALE_RUN_EPOCH,
                    "Stop evidence epoch/agent mismatch: currentRunEpoch=" + policy.getCurrentRunEpoch()
                            + ", currentAgentId=" + policy.getCurrentAgentId());
        }

        String evId = TaskThrottleStopEvidenceEntity.buildEvidenceId(
                dto.getTaskId().trim(), dto.getOldAgentId().trim(), dto.getOldRunEpoch(), dto.getEvidenceRef().trim());
        TaskThrottleStopEvidenceEntity existing = mongo.findOne(
                Query.query(Criteria.where("_id").is(evId)), TaskThrottleStopEvidenceEntity.class);
        if (existing != null) {
            return existing;
        }

        long now = clock.millis();
        TaskThrottleStopEvidenceEntity entity = TaskThrottleStopEvidenceEntity.builder()
                .id(evId)
                .tenantId(policy.getTenantId())
                .taskKind(policy.getTaskKind())
                .taskId(policy.getTaskId())
                .oldAgentId(dto.getOldAgentId().trim())
                .oldRunEpoch(dto.getOldRunEpoch())
                .source(isBlank(dto.getSource())
                        ? (operatorManual ? TaskThrottleStopEvidenceDto.SOURCE_OPERATOR_MANUAL : TaskThrottleStopEvidenceDto.SOURCE_AGENT_GRACEFUL_STOP)
                        : dto.getSource().trim())
                .evidenceRef(dto.getEvidenceRef().trim())
                .observedAt(dto.getObservedAt() > 0L ? dto.getObservedAt() : now)
                .submittedBy(isBlank(dto.getSubmittedBy()) ? "system" : dto.getSubmittedBy().trim())
                .processExited(dto.isProcessExited())
                .writeSettled(dto.isWriteSettled())
                .note(dto.getNote())
                .consumed(false)
                .build();
        try {
            mongo.insert(entity);
        } catch (DuplicateKeyException dup) {
            TaskThrottleStopEvidenceEntity race = mongo.findOne(
                    Query.query(Criteria.where("_id").is(evId)), TaskThrottleStopEvidenceEntity.class);
            if (race != null) {
                return race;
            }
        }

        recordAuditLog(TaskThrottleChangeLogEntity.builder()
                .id(UUID.randomUUID().toString())
                .dedupKey("STOP:" + policy.getTaskId() + ":" + dto.getOldRunEpoch() + ":" + dto.getEvidenceRef().trim())
                .tenantId(policy.getTenantId())
                .taskKind(policy.getTaskKind())
                .taskId(policy.getTaskId())
                .taskKey(policy.getId())
                .eventType(TaskThrottleChangeLogEntity.EVENT_STOP_EVIDENCE)
                .runEpoch(dto.getOldRunEpoch())
                .policyVersion(policy.getDesiredVersion())
                .outcome(entity.getSource())
                .reason(entity.getNote())
                .operator(entity.getSubmittedBy())
                .agentId(entity.getOldAgentId())
                .occurredAt(now)
                .build());
        return entity;
    }

    /**
     * Unified authorization gate invoked before TM dispatches a task start/takeover to {@code targetAgentId}.
     * Atomically verifies capability, consumes stop evidence if an old protected epoch is unconfirmed,
     * and advances currentRunEpoch via CAS.
     */
    public AuthorizationDecision authorizeStartOrTakeover(TaskDto taskDto, String targetAgentId) {
        if (taskDto == null || taskDto.getId() == null) {
            return AuthorizationDecision.allowUnprotected();
        }
        String taskId = taskDto.getId().toHexString();
        TaskThrottlePolicyEntity policy = findPolicyByTaskId(taskId);
        if (policy == null) {
            return AuthorizationDecision.allowUnprotected();
        }
        boolean hasEnabled = hasEnabledPhase(policy);
        if (!policy.isProtectedRunActive() && !hasEnabled) {
            return AuthorizationDecision.allowUnprotected();
        }

        String kind = policy.getTaskKind() != null ? policy.getTaskKind() : taskDto.getSyncType();
        if (hasEnabled && !isAgentCompatible(targetAgentId, kind)) {
            updateStopGate(policy.getId(), TaskThrottlePolicyEntity.STOP_GATE_WAITING_COMPATIBLE_AGENT,
                    "Agent " + targetAgentId + " is not compatible with task throttling");
            return AuthorizationDecision.block(ThrottleConstants.ERR_UNSUPPORTED_PATH, "等待兼容执行路径");
        }

        long oldEpoch = policy.getCurrentRunEpoch();
        String oldAgent = policy.getCurrentAgentId();

        if (policy.isProtectedRunActive()) {
            TaskThrottleStopEvidenceEntity evidence = findUnconsumedVerifiedEvidence(taskId, oldAgent, oldEpoch);
            if (evidence == null) {
                updateStopGate(policy.getId(), TaskThrottlePolicyEntity.STOP_GATE_WAITING_EVIDENCE,
                        "Pending verified stop evidence for oldAgent=" + oldAgent + ", oldRunEpoch=" + oldEpoch);
                return AuthorizationDecision.block(ThrottleConstants.ERR_STOP_EVIDENCE_REQUIRED, "等待确认旧执行已停止");
            }
            long nextEpoch = oldEpoch + 1L;
            long now = clock.millis();
            // 1. Atomic CAS consume stop evidence so concurrent restarts cannot double-consume.
            Query evQuery = Query.query(Criteria.where("_id").is(evidence.getId()).and("consumed").is(false));
            Update evUpdate = new Update()
                    .set("consumed", true)
                    .set("consumedAt", now)
                    .set("consumedByRunEpoch", nextEpoch);
            UpdateResult evRes = mongo.updateFirst(evQuery, evUpdate, TaskThrottleStopEvidenceEntity.class);
            if (evRes == null || evRes.getModifiedCount() == 0L) {
                return AuthorizationDecision.block(ThrottleConstants.ERR_STOP_EVIDENCE_REQUIRED, "Stop evidence already consumed concurrently");
            }
            // 2. Atomic CAS advance runEpoch and bind new agent.
            TaskThrottlePolicyEntity updated = casAdvanceEpoch(policy, oldEpoch, nextEpoch, oldAgent, targetAgentId, hasEnabled);
            if (updated == null) {
                return AuthorizationDecision.block(ThrottleConstants.ERR_STALE_RUN_EPOCH, "Concurrent epoch takeover detected");
            }
            return hasEnabled
                    ? AuthorizationDecision.allowProtected(nextEpoch, toPolicyDto(updated))
                    : AuthorizationDecision.allowUnprotected();
        }

        // First protected run authorization from unprotected baseline.
        long nextEpoch = oldEpoch + 1L;
        TaskThrottlePolicyEntity updated = casAdvanceEpoch(policy, oldEpoch, nextEpoch, oldAgent, targetAgentId, true);
        if (updated == null) {
            return AuthorizationDecision.block(ThrottleConstants.ERR_STALE_RUN_EPOCH, "Concurrent startup authorization conflict");
        }
        return AuthorizationDecision.allowProtected(nextEpoch, toPolicyDto(updated));
    }

    /**
     * Releases an epoch reservation that was authorized by TM scheduling before the task
     * actually transitioned to {@code WAIT_RUN} / launched on Agent, preventing permanent lockouts.
     */
    public boolean releaseUnstartedEpoch(String taskId, long expectedRunEpoch, String expectedAgentId, String reason) {
        if (mongo == null || isBlank(taskId) || expectedRunEpoch <= 0L) {
            return false;
        }
        Criteria criteria = Criteria.where("taskId").is(taskId.trim())
                .and("currentRunEpoch").is(expectedRunEpoch)
                .and("protectedRunActive").is(true);
        if (!isBlank(expectedAgentId)) {
            criteria = criteria.and("currentAgentId").is(expectedAgentId.trim());
        }
        Update update = new Update()
                .set("protectedRunActive", false)
                .set("stopGateStatus", TaskThrottlePolicyEntity.STOP_GATE_NONE)
                .set("stopGateReason", null)
                .set("deltaSeq", nextDeltaSeq());
        UpdateResult res = mongo.updateFirst(Query.query(criteria), update, TaskThrottlePolicyEntity.class);
        boolean released = res != null && res.getModifiedCount() > 0L;
        if (released) {
            log.info("Released unstarted protected runEpoch {} for task {} (agent={}, reason={})",
                    expectedRunEpoch, taskId, expectedAgentId, reason);
        }
        return released;
    }

    /**
     * Releases any pending protected epoch on {@code taskDto} if its Agent run never started
     * (e.g. start message failure, WAIT_RUN timeout, or user stop while still in WAIT_RUN).
     */
    public boolean releaseUnstartedEpochIfPresent(TaskDto taskDto, String reason) {
        if (mongo == null || taskDto == null || taskDto.getId() == null) {
            return false;
        }
        String taskId = taskDto.getId().toHexString();
        long expectedEpoch = 0L;
        if (taskDto.getAttrs() != null && taskDto.getAttrs().get("throttleRunEpoch") instanceof Number num) {
            expectedEpoch = num.longValue();
        }
        if (expectedEpoch <= 0L) {
            TaskThrottlePolicyEntity policy = findPolicyByTaskId(taskId);
            if (policy == null || !policy.isProtectedRunActive() || policy.getCurrentRunEpoch() <= 0L) {
                return false;
            }
            expectedEpoch = policy.getCurrentRunEpoch();
        }
        return releaseUnstartedEpoch(taskId, expectedEpoch, taskDto.getAgentId(), reason);
    }

    /**
     * Prevents heartbeat-timeout schedulers from reassigning a protected task solely on ping timeout
     * when verified stop evidence has not been recorded for an active protected runEpoch.
     */
    public boolean shouldBlockTimeoutReschedule(TaskDto taskDto) {
        if (taskDto == null || taskDto.getId() == null) {
            return false;
        }
        String taskId = taskDto.getId().toHexString();
        TaskThrottlePolicyEntity policy = findPolicyByTaskId(taskId);
        if (policy == null || !policy.isProtectedRunActive() || policy.getCurrentRunEpoch() <= 0L) {
            return false;
        }
        TaskThrottleStopEvidenceEntity valid = findUnconsumedVerifiedEvidence(
                taskId, policy.getCurrentAgentId(), policy.getCurrentRunEpoch());
        if (valid == null) {
            updateStopGate(policy.getId(), TaskThrottlePolicyEntity.STOP_GATE_WAITING_EVIDENCE,
                    "Heartbeat timeout cannot override unconfirmed protected runEpoch=" + policy.getCurrentRunEpoch());
            return true;
        }
        return false;
    }

    /**
     * Rejects stale-epoch checkpoint updates while preserving unprotected legacy checkpoint writes untouched.
     *
     * @return {@code true} iff this task currently has an active protected throttle runEpoch enforced.
     */
    public boolean validateCheckpointWrite(String taskId, Long runEpoch, String agentId) {
        if (isBlank(taskId)) {
            return false;
        }
        TaskThrottlePolicyEntity policy = findPolicyByTaskId(taskId.trim());
        if (policy == null || !policy.isProtectedRunActive()) {
            return false;
        }
        if (runEpoch == null || !Objects.equals(runEpoch, policy.getCurrentRunEpoch())) {
            throw new BizException(ThrottleConstants.ERR_STALE_RUN_EPOCH,
                    "Checkpoint runEpoch rejected: provided=" + runEpoch + ", authorized=" + policy.getCurrentRunEpoch());
        }
        if (!isBlank(agentId) && !isBlank(policy.getCurrentAgentId()) && !agentId.trim().equals(policy.getCurrentAgentId())) {
            throw new BizException(ThrottleConstants.ERR_STALE_RUN_EPOCH,
                    "Checkpoint agentId rejected: provided=" + agentId + ", authorized=" + policy.getCurrentAgentId());
        }
        return true;
    }

    public TaskThrottlePolicyEntity findPolicyByTaskId(String taskId) {
        if (mongo == null || isBlank(taskId)) {
            return null;
        }
        return mongo.findOne(Query.query(Criteria.where("taskId").is(taskId)), TaskThrottlePolicyEntity.class);
    }

    public static boolean hasEnabledPhase(TaskThrottlePolicyEntity policy) {
        if (policy == null || policy.getPhases() == null) {
            return false;
        }
        for (ThrottlePhaseLimit limit : policy.getPhases().values()) {
            if (limit != null && limit.isEnabled()) {
                return true;
            }
        }
        return false;
    }

    public static TaskThrottlePolicyDto toPolicyDto(TaskThrottlePolicyEntity entity) {
        return TaskThrottlePolicyDto.builder()
                .schemaVersion(entity.getSchemaVersion())
                .tenantId(entity.getTenantId())
                .taskKind(entity.getTaskKind())
                .taskId(entity.getTaskId())
                .desiredVersion(entity.getDesiredVersion())
                .phases(entity.getPhases())
                .updatedBy(entity.getUpdatedBy())
                .updatedAt(entity.getUpdatedAt())
                .build();
    }

    private TaskThrottlePolicyEntity casAdvanceEpoch(
            TaskThrottlePolicyEntity policy,
            long expectedOldEpoch,
            long nextEpoch,
            String oldAgentId,
            String targetAgentId,
            boolean nextProtected) {
        Query q = Query.query(Criteria.where("_id").is(policy.getId()).and("currentRunEpoch").is(expectedOldEpoch));
        Update u = new Update()
                .set("currentRunEpoch", nextEpoch)
                .set("previousAgentId", oldAgentId)
                .set("currentAgentId", targetAgentId)
                .set("protectedRunActive", nextProtected)
                .set("stopGateStatus", TaskThrottlePolicyEntity.STOP_GATE_NONE)
                .set("stopGateReason", null)
                .set("deltaSeq", nextDeltaSeq());
        return mongo.findAndModify(q, u, FindAndModifyOptions.options().returnNew(true), TaskThrottlePolicyEntity.class);
    }

    private TaskThrottleStopEvidenceEntity findUnconsumedVerifiedEvidence(String taskId, String oldAgentId, long oldRunEpoch) {
        Criteria criteria = Criteria.where("taskId").is(taskId)
                .and("oldRunEpoch").is(oldRunEpoch)
                .and("consumed").is(false)
                .and("writeSettled").is(true);
        if (!isBlank(oldAgentId)) {
            criteria = criteria.and("oldAgentId").is(oldAgentId);
        }
        List<TaskThrottleStopEvidenceEntity> candidates = mongo.find(Query.query(criteria), TaskThrottleStopEvidenceEntity.class);
        if (candidates == null) {
            return null;
        }
        for (TaskThrottleStopEvidenceEntity ev : candidates) {
            if (TaskThrottleStopEvidenceDto.SOURCE_OPERATOR_MANUAL.equals(ev.getSource()) && !ev.isProcessExited()) {
                continue;
            }
            return ev;
        }
        return null;
    }

    private void updateStopGate(String policyId, String gateStatus, String reason) {
        mongo.updateFirst(
                Query.query(Criteria.where("_id").is(policyId)),
                new Update().set("stopGateStatus", gateStatus).set("stopGateReason", reason),
                TaskThrottlePolicyEntity.class);
    }

    private void recordAuditLog(TaskThrottleChangeLogEntity entry) {
        try {
            mongo.insert(entry);
        } catch (DuplicateKeyException ignored) {
            // Idempotent
        }
    }

    private long nextDeltaSeq() {
        long ts = clock.millis() * 1000L;
        return deltaSeqClock.accumulateAndGet(ts, (cur, proposed) -> Math.max(cur + 1L, proposed));
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    public record AuthorizationDecision(
            boolean allowed,
            boolean protectedRun,
            long runEpoch,
            TaskThrottlePolicyDto policy,
            String errorCode,
            String reason) {

        public static AuthorizationDecision allowUnprotected() {
            return new AuthorizationDecision(true, false, 0L, null, null, null);
        }

        public static AuthorizationDecision allowProtected(long runEpoch, TaskThrottlePolicyDto policy) {
            return new AuthorizationDecision(true, true, runEpoch, policy, null, null);
        }

        public static AuthorizationDecision block(String errorCode, String reason) {
            return new AuthorizationDecision(false, false, 0L, null, errorCode, reason);
        }
    }
}

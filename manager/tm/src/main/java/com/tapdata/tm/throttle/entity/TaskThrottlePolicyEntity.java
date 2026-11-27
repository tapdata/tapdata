package com.tapdata.tm.throttle.entity;

import com.tapdata.tm.commons.throttle.ThrottlePhase;
import com.tapdata.tm.commons.throttle.ThrottlePhaseLimit;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
import java.util.Map;

/**
 * Mongo document storing desired task throttling policy, CAS runEpoch authorization,
 * stop gate state, last applied agent summary, and bounded audit outbox.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document("TaskThrottlePolicy")
public class TaskThrottlePolicyEntity {

    public static final int MAX_OUTBOX_CAPACITY = 200;

    public static final String STOP_GATE_NONE = "NONE";
    public static final String STOP_GATE_WAITING_EVIDENCE = "WAITING_STOP_EVIDENCE";
    public static final String STOP_GATE_WAITING_COMPATIBLE_AGENT = "WAITING_COMPATIBLE_AGENT";

    public static final String APPLY_NONE = "NONE";
    public static final String APPLY_PENDING = "PENDING";
    public static final String APPLY_APPLIED = "APPLIED";
    public static final String APPLY_FAILED = "FAILED";
    public static final String APPLY_WAITING_COMPATIBLE_AGENT = "WAITING_COMPATIBLE_AGENT";

    /**
     * Canonical unique id formatted as {@code tenantId:taskKind:taskId}.
     */
    @Id
    private String id;

    private String tenantId;
    private String taskKind;
    @Indexed(name = "idx_throttle_policy_task_id")
    private String taskId;

    @Builder.Default
    private int schemaVersion = 1;
    private long desiredVersion;
    private Map<ThrottlePhase, ThrottlePhaseLimit> phases;
    private String updatedBy;
    private Long updatedAt;

    /**
     * Monotonically increasing run epoch fencing stale engines and checkpoint writes.
     */
    private long currentRunEpoch;
    private String currentAgentId;
    private String previousAgentId;
    private boolean protectedRunActive;

    @Builder.Default
    private String stopGateStatus = STOP_GATE_NONE;
    private String stopGateReason;
    private long deltaSeq;

    private long appliedVersion;
    private Map<ThrottlePhase, ThrottlePhaseLimit> appliedPhases;
    private Long appliedAt;

    @Builder.Default
    private String applyStatus = APPLY_NONE;
    private String lastApplyReason;

    private List<AuditOutboxEntry> pendingAuditOutbox;

    public static String buildTaskKey(String tenantId, String taskKind, String taskId) {
        String t = tenantId == null ? "" : tenantId.trim();
        String k = (taskKind == null || taskKind.isBlank()) ? "sync" : taskKind.trim();
        String id = taskId == null ? "" : taskId.trim();
        return t + ":" + k + ":" + id;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AuditOutboxEntry {
        private String entryId;
        private String dedupKey;
        private String eventType;
        private long beforeDesiredVersion;
        private long afterDesiredVersion;
        private Map<ThrottlePhase, ThrottlePhaseLimit> beforePhases;
        private Map<ThrottlePhase, ThrottlePhaseLimit> afterPhases;
        private String operator;
        private long occurredAt;
        private String detail;
    }
}

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
 * Deduplicated audit and receipt history entry for task throttling changes, receipts, and stop evidence.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document("TaskThrottleChangeLog")
public class TaskThrottleChangeLogEntity {

    public static final String EVENT_POLICY_CHANGE = "POLICY_CHANGE";
    public static final String EVENT_APPLY_RECEIPT = "APPLY_RECEIPT";
    public static final String EVENT_STOP_EVIDENCE = "STOP_EVIDENCE";

    @Id
    private String id;

    @Indexed(unique = true)
    private String dedupKey;

    private String tenantId;
    private String taskKind;
    @Indexed
    private String taskId;
    @Indexed
    private String taskKey;

    private String eventType;
    private long runEpoch;
    private long policyVersion;
    private Map<ThrottlePhase, ThrottlePhaseLimit> beforePhases;
    private Map<ThrottlePhase, ThrottlePhaseLimit> afterPhases;
    private String outcome;
    private String reason;
    private List<Long> skippedVersions;
    private String operator;
    private String agentId;
    private long occurredAt;
}

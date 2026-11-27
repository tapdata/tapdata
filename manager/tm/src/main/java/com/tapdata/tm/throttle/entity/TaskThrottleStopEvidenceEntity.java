package com.tapdata.tm.throttle.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Verifiable execution stop and write-settled evidence persisted for atomic CAS consumption prior to takeover.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document("TaskThrottleStopEvidence")
public class TaskThrottleStopEvidenceEntity {

    @Id
    private String id;

    private String tenantId;
    private String taskKind;
    @Indexed
    private String taskId;
    private String oldAgentId;
    private long oldRunEpoch;
    private String source;
    private String evidenceRef;
    private long observedAt;
    private String submittedBy;
    private boolean processExited;
    private boolean writeSettled;
    private String note;

    private boolean consumed;
    private Long consumedAt;
    private Long consumedByRunEpoch;

    public static String buildEvidenceId(String taskId, String oldAgentId, long oldRunEpoch, String evidenceRef) {
        return taskId + ":" + oldAgentId + ":" + oldRunEpoch + ":" + evidenceRef;
    }
}

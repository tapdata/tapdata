package com.tapdata.tm.commons.throttle;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Verifiable stop evidence required before TM authorizes a new runEpoch for a throttled/protected task.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TaskThrottleStopEvidenceDto implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String SOURCE_AGENT_GRACEFUL_STOP = "AGENT_GRACEFUL_STOP";
    public static final String SOURCE_OPERATOR_MANUAL = "OPERATOR_MANUAL";

    private String tenantId;
    private String taskKind;
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
}

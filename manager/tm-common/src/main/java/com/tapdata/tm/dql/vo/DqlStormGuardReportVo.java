package com.tapdata.tm.dql.vo;

import lombok.Data;

import java.io.Serializable;

/**
 * Safe Engine-to-TM observability payload for an activated unknown-error guard.
 * It deliberately contains no record payload or raw exception text.
 */
@Data
public class DqlStormGuardReportVo implements Serializable {
    private String taskId;
    private String taskName;
    private String agentId;
    private String guardKey;
    private Long windowSeconds;
    private Long windowCount;
    private Long guardThreshold;
    private Double batchRatio;
    private Double maxBatchRatio;
    private Long suppressedCountEstimate;
    private String routeDecision;
    private String safeReason;
    private Long occurredAt;
}

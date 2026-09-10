package io.tapdata.dql.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

/**
 * Safe Engine-to-TM observability payload emitted when unknown-error Storm Guard routes to task handling.
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DqlStormGuardReport {
    private String taskName;
    private String agentId;
    private String guardKey;
    private Long windowSeconds;
    private Long windowCount;
    private Long guardThreshold;
    private Double batchRatio;
    private Double maxBatchRatio;
    private Long suppressedCountEstimate;
    private DqlRouteDecision routeDecision;
    private String safeReason;
    private Long occurredAt;
}

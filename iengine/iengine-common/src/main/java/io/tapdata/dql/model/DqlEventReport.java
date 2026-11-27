package io.tapdata.dql.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Engine request body for POST /api/task/{taskId}/dql-events/report.
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DqlEventReport {
    private String eventId;
    private String taskRecordId;
    private String taskName;
    private Long taskVersion;
    private String agentId;
    private String sourceNodeId;
    private String sourceNodeName;
    private String targetNodeId;
    private String targetNodeName;
    private String failedNodeId;
    private String failedNodeName;
    private String failedStage;
    private String sourceTable;
    private String targetTable;
    private String tableId;
    private String dmlType;
    private Long eventTime;
    private Long captureSeq;
    private Map<String, Object> eventKey;
    private Boolean eventKeyMissing;
    private String eventIdentity;
    private String recordIdentity;
    private String recordIdentityType;
    private List<String> recordIdentityFields;
    private String errorCode;
    private DqlExceptionScope exceptionScope;
    private DqlRouteDecision routeDecision;
    private DqlErrorType errorType;
    private String classificationReason;
    private DqlClassificationConfidence classificationConfidence;
    private String errorDetails;
    private String rawErrorRef;

    @JsonUnwrapped
    private DqlPayloadSnapshot payload = new DqlPayloadSnapshot();
}

package com.tapdata.tm.dql.vo;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class DqlEventReportVo implements Serializable {
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
    private String payloadFormat;
    private Object payloadData;
    private String payloadHash;
    private Long payloadSize;
    private Boolean payloadComplete;
    private Map<String, Object> payloadPreview;
    private Boolean payloadPreviewTruncated;
    private String errorType;
    private String errorCode;
    private String exceptionScope;
    private String routeDecision;
    private String classificationReason;
    private String classificationConfidence;
    private String errorDetails;
    private String rawErrorRef;
}

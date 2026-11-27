package com.tapdata.tm.dql.vo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Public detail representation of a DLQ event. It is deliberately independent from the
 * persistence DTO so internal payload and identity fields cannot leak through serialization.
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DqlEventDetailVo implements Serializable {
    private String id;
    private String eventId;
    private String taskId;
    private String taskName;
    private String sourceTable;
    private String targetTable;
    private String dmlType;
    private String errorType;
    private String errorCode;
    private Date eventTime;
    private Date failedAt;
    private Long captureSeq;
    private String status;
    private String notReprocessableReason;
    private Integer recoveryCount;
    private Date lastRecoveryTime;
    private String sourceNodeId;
    private String sourceNodeName;
    private String targetNodeId;
    private String targetNodeName;
    private String failedNodeId;
    private String failedNodeName;
    private String stage;
    private String tableId;
    private String eventKey;
    private Boolean eventKeyMissing;
    private String payloadFormat;
    private String payloadHash;
    private Long payloadSize;
    private Boolean payloadComplete;
    private Map<String, Object> payloadPreview;
    private Boolean payloadPreviewTruncated;
    private String errorDetails;
    private String rawErrorRef;
    private List<DqlRecoveryAttemptVo> recoveryAttempts;

    /** Retained for internal callers; it is not part of the public detail response. */
    @JsonIgnore
    private DqlRecoveryBatchDto currentBatch;
}

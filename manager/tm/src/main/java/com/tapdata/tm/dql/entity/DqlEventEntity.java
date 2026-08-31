package com.tapdata.tm.dql.entity;

import com.tapdata.tm.base.entity.Entity;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
@Document("dql_events")
@EqualsAndHashCode(callSuper = true)
public class DqlEventEntity extends Entity {
    @Field(DqlEventDto.FIELD_EVENT_ID)
    private String eventId;
    @Field(DqlEventDto.FIELD_TASK_ID)
    private String taskId;
    @Field(DqlEventDto.FIELD_TASK_RECORD_ID)
    private String taskRecordId;
    @Field(DqlEventDto.FIELD_TASK_NAME)
    private String taskName;
    @Field(DqlEventDto.FIELD_TASK_VERSION)
    private Long taskVersion;
    @Field(DqlEventDto.FIELD_AGENT_ID)
    private String agentId;
    @Field(DqlEventDto.FIELD_SOURCE_NODE_ID)
    private String sourceNodeId;
    @Field(DqlEventDto.FIELD_SOURCE_NODE_NAME)
    private String sourceNodeName;
    @Field(DqlEventDto.FIELD_TARGET_NODE_ID)
    private String targetNodeId;
    @Field(DqlEventDto.FIELD_TARGET_NODE_NAME)
    private String targetNodeName;
    @Field(DqlEventDto.FIELD_FAILED_NODE_ID)
    private String failedNodeId;
    @Field(DqlEventDto.FIELD_FAILED_NODE_NAME)
    private String failedNodeName;
    @Field(DqlEventDto.FIELD_FAILED_STAGE)
    private String failedStage;
    @Field(DqlEventDto.FIELD_SOURCE_TABLE)
    private String sourceTable;
    @Field(DqlEventDto.FIELD_TARGET_TABLE)
    private String targetTable;
    @Field(DqlEventDto.FIELD_TABLE_ID)
    private String tableId;
    @Field(DqlEventDto.FIELD_DML_TYPE)
    private String dmlType;
    @Field(DqlEventDto.FIELD_EVENT_TIME)
    private Date eventTime;
    @Field(DqlEventDto.FIELD_CAPTURE_SEQ)
    private Long captureSeq;
    @Field(DqlEventDto.FIELD_FAILED_AT)
    private Date failedAt;
    @Field(DqlEventDto.FIELD_EVENT_KEY)
    private Map<String, Object> eventKey;
    @Field(DqlEventDto.FIELD_EVENT_KEY_MISSING)
    private Boolean eventKeyMissing;
    @Field(DqlEventDto.FIELD_EVENT_IDENTITY)
    private String eventIdentity;
    @Field(DqlEventDto.FIELD_RECORD_IDENTITY)
    private String recordIdentity;
    @Field(DqlEventDto.FIELD_RECORD_IDENTITY_TYPE)
    private String recordIdentityType;
    @Field(DqlEventDto.FIELD_RECORD_IDENTITY_FIELDS)
    private List<String> recordIdentityFields;
    @Field(DqlEventDto.FIELD_PAYLOAD_FORMAT)
    private String payloadFormat;
    @Field(DqlEventDto.FIELD_PAYLOAD_DATA)
    private Object payloadData;
    @Field(DqlEventDto.FIELD_PAYLOAD_HASH)
    private String payloadHash;
    @Field(DqlEventDto.FIELD_PAYLOAD_SIZE)
    private Long payloadSize;
    @Field(DqlEventDto.FIELD_PAYLOAD_COMPLETE)
    private Boolean payloadComplete;
    @Field(DqlEventDto.FIELD_PAYLOAD_PREVIEW)
    private Map<String, Object> payloadPreview;
    @Field(DqlEventDto.FIELD_PAYLOAD_PREVIEW_TRUNCATED)
    private Boolean payloadPreviewTruncated;
    @Field(DqlEventDto.FIELD_ERROR_TYPE)
    private String errorType;
    @Field(DqlEventDto.FIELD_ERROR_CODE)
    private String errorCode;
    @Field(DqlEventDto.FIELD_EXCEPTION_SCOPE)
    private String exceptionScope;
    @Field(DqlEventDto.FIELD_ROUTE_DECISION)
    private String routeDecision;
    @Field(DqlEventDto.FIELD_CLASSIFICATION_REASON)
    private String classificationReason;
    @Field(DqlEventDto.FIELD_CLASSIFICATION_CONFIDENCE)
    private String classificationConfidence;
    @Field(DqlEventDto.FIELD_ERROR_DETAILS)
    private String errorDetails;
    @Field(DqlEventDto.FIELD_ERROR_DETAILS_TRUNCATED)
    private Boolean errorDetailsTruncated;
    @Field(DqlEventDto.FIELD_RAW_ERROR_REF)
    private String rawErrorRef;
    private String status;
    @Field(DqlEventDto.FIELD_RECOVERY_STATUS_BEFORE_SYNC)
    private String recoveryStatusBeforeSync;
    @Field(DqlEventDto.FIELD_NOT_REPROCESSABLE_REASON)
    private String notReprocessableReason;
    @Field(DqlEventDto.FIELD_RECOVERY_COUNT)
    private Integer recoveryCount;
    @Field(DqlEventDto.FIELD_LAST_RECOVERY_TIME)
    private Date lastRecoveryTime;
    @Field(DqlEventDto.FIELD_LAST_RECOVERY_USER_ID)
    private String lastRecoveryUserId;
    @Field(DqlEventDto.FIELD_LAST_RECOVERY_USER_NAME)
    private String lastRecoveryUserName;
    @Field(DqlEventDto.FIELD_LAST_RECOVERY_RESULT)
    private String lastRecoveryResult;
    @Field(DqlEventDto.FIELD_CURRENT_BATCH_ID)
    private String currentBatchId;
    @Field(DqlEventDto.FIELD_OVERWRITE_RISK)
    private Boolean overwriteRisk;
    @Field(DqlEventDto.FIELD_OVERWRITE_RISK_MESSAGE)
    private String overwriteRiskMessage;
    @Field(DqlEventDto.FIELD_LATER_SUCCESS_AT)
    private Date laterSuccessAt;
    @Field(DqlEventDto.FIELD_LATER_SUCCESS_EVENT_TIME)
    private Date laterSuccessEventTime;
    @Field(DqlEventDto.FIELD_LATER_SUCCESS_CAPTURE_SEQ)
    private Long laterSuccessCaptureSeq;
    @Field(DqlEventDto.FIELD_LATER_SUCCESS_DML_TYPE)
    private String laterSuccessDmlType;
    @Field(DqlEventDto.FIELD_RECOVERY_ATTEMPTS)
    private List<DqlRecoveryAttemptDto> recoveryAttempts;
    private Date created;
    private Date updated;
    @Field(DqlEventDto.FIELD_TTL_AT)
    private Date ttlAt;
}

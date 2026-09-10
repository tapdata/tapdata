package com.tapdata.tm.dql.dto;

import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serializable;
import java.util.Date;

@Data
public class DqlRecoveryAttemptDto implements Serializable {
    public static final String FIELD_ATTEMPT_ID = "attempt_id";
    public static final String FIELD_BATCH_ID = "batch_id";
    public static final String FIELD_OPERATOR_ID = "operator_id";
    public static final String FIELD_OPERATOR_NAME = "operator_name";
    public static final String FIELD_TASK_VERSION = "task_version";
    public static final String FIELD_STARTED_AT = "started_at";
    public static final String FIELD_FINISHED_AT = "finished_at";
    public static final String FIELD_RESULT = "result";
    public static final String FIELD_MESSAGE = "message";
    public static final String FIELD_ERROR_CODE = "error_code";
    public static final String FIELD_ERROR_DETAILS = "error_details";

    @Field(FIELD_ATTEMPT_ID)
    private String attemptId;
    @Field(FIELD_BATCH_ID)
    private String batchId;
    @Field(FIELD_OPERATOR_ID)
    private String operatorId;
    @Field(FIELD_OPERATOR_NAME)
    private String operatorName;
    @Field(FIELD_TASK_VERSION)
    private Long taskVersion;
    @Field(FIELD_STARTED_AT)
    private Date startedAt;
    @Field(FIELD_FINISHED_AT)
    private Date finishedAt;
    private String result;
    private String message;
    @Field(FIELD_ERROR_CODE)
    private String errorCode;
    @Field(FIELD_ERROR_DETAILS)
    private String errorDetails;
}

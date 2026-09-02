package com.tapdata.tm.dql.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * A compact, user-visible audit record for a DLQ recovery batch.
 */
@Data
public class DqlRecoveryAuditEntryDto implements Serializable {
    public static final String FIELD_TYPE = "type";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_EVENT_ID = "event_id";
    public static final String FIELD_ATTEMPT_ID = "attempt_id";
    public static final String FIELD_MESSAGE = "message";
    public static final String FIELD_OCCURRED_AT = "occurred_at";
    public static final String FIELD_OPERATOR_ID = "operator_id";
    public static final String FIELD_OPERATOR_NAME = "operator_name";

    private String type;
    private String status;
    private String eventId;
    private String attemptId;
    private String message;
    private Date occurredAt;
    private String operatorId;
    private String operatorName;
}

package com.tapdata.tm.dql.vo;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * Public list representation of a DQL event. Persistence-only payload and audit fields are
 * intentionally absent from this type.
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DqlEventListVo implements Serializable {
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
    private Integer recoveryCount;
    private Date lastRecoveryTime;
}

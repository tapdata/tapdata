package com.tapdata.tm.dql.vo;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/** Public recovery history representation used by the event detail drawer. */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DqlRecoveryAttemptVo implements Serializable {
    private String attemptId;
    private String batchId;
    private Date startedAt;
    private Date finishedAt;
    private String result;
    private String message;
    private String errorCode;
    private String errorMessage;
}

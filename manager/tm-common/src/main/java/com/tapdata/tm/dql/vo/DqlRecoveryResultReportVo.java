package com.tapdata.tm.dql.vo;

import lombok.Data;

import java.io.Serializable;

@Data
public class DqlRecoveryResultReportVo implements Serializable {
    private String batchId;
    private String eventId;
    private String attemptId;
    private String type;
    private String result;
    private String message;
    private String errorCode;
    private String errorDetails;
    private Long startedAt;
    private Long finishedAt;
}

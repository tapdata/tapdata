package com.tapdata.tm.dql.vo;

import lombok.Data;
import com.tapdata.tm.dql.dto.DqlRecoveryNodeStateDto;

import java.io.Serializable;
import java.util.List;

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
    private Long pingTime;
    private Long finishedAt;
    private List<DqlRecoveryNodeStateDto> nodeStates;
}

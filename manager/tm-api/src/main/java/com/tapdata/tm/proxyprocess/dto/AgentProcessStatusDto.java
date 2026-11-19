package com.tapdata.tm.proxyprocess.dto;

import lombok.Data;

import java.util.Date;

@Data
public class AgentProcessStatusDto {
    private String agentId;
    private Date startTime;
    private Long pid;
    private String status;
    private Date statusReportTime;
    private Double cpuUsage;
    private Double memoryUsage;
    private String errorReason;
}

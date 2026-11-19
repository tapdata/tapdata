package com.tapdata.tm.proxyprocess.dto;

import lombok.Data;

import java.util.Date;

@Data
public class AgentProcessDto {
    private String proxyProcessId;
    private String agentName;
    private String agentType;
    private String agentId;
    private String advertiseIp;
    private Integer advertisePort;
    private Date startTime;
    private String status;
    private Date statusReportTime;
    private Long pid;
    private Double cpuUsage;
    private Double memoryUsage;
    private String errorReason;
    private Date lastRegisterTime;
}

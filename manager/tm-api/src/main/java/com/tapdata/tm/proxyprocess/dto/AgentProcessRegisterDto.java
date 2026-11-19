package com.tapdata.tm.proxyprocess.dto;

import lombok.Data;

import java.util.Date;

@Data
public class AgentProcessRegisterDto {
    private String agentName;
    private String agentType;
    private String agentId;
    private String advertiseIp;
    private Integer advertisePort;
    private Date startTime;
}

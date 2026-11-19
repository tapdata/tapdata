package com.tapdata.tm.proxyprocess.dto;

import lombok.Data;

@Data
public class AgentProcessQueryDto {
    private String proxyProcessId;
    private String agentType;
    private String status;
    private String sortBy;
    private String sortOrder;
    private Integer page = 1;
    private Integer pageSize = 20;
}

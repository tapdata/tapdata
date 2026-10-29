package com.tapdata.tm.proxyprocess.dto;

import lombok.Data;

@Data
public class AgentProcessRegisterResponseDto {
    private String proxyProcessId;
    private String agentId;
    private String token;
}

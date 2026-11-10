package com.tapdata.tm.proxyprocess.service;

import com.tapdata.tm.proxyprocess.dto.AgentProcessDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterResponseDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessStatusDto;
import com.tapdata.tm.proxyprocess.entity.AgentProcessEntity;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.proxyprocess.dto.AgentProcessQueryDto;

import java.util.List;

public interface AgentProcessService {
    AgentProcessRegisterResponseDto register(AgentProcessRegisterDto request);

    void reportStatus(AgentProcessStatusDto request, String authorization);

    String effectiveStatus(AgentProcessEntity entity);

    AgentProcessDto toDto(AgentProcessEntity entity);

    Page<AgentProcessDto> list(AgentProcessQueryDto query);

    List<String> listAgentTypes();

    void delete(String proxyProcessId);
}

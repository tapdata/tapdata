package com.tapdata.tm.proxyprocess.service;

import com.tapdata.tm.proxyprocess.dto.AgentProcessDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterResponseDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessStatusDto;
import com.tapdata.tm.proxyprocess.entity.AgentProcessEntity;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.proxyprocess.dto.AgentProcessQueryDto;

import java.util.List;

public interface AgentProcessService {
    /**
     * Register a Reader on behalf of the caller TM has already authenticated.
     *
     * @param user the caller resolved from its access token; the registration belongs to it
     */
    AgentProcessRegisterResponseDto register(AgentProcessRegisterDto request, UserDetail user);

    /** Record a Reader's status report. The caller is authenticated by the endpoint. */
    void reportStatus(AgentProcessStatusDto request);

    String effectiveStatus(AgentProcessEntity entity);

    AgentProcessDto toDto(AgentProcessEntity entity);

    Page<AgentProcessDto> list(AgentProcessQueryDto query);

    List<String> listAgentTypes();

    void delete(String proxyProcessId);

    /**
     * Reveal the client secret a Reader generated, so an authorised caller can persist how to
     * obtain an access token for it.
     *
     * <p>Only the hash is stored; this re-reports the plaintext the Reader supplied when it
     * registered. Callers must already be permitted to manage the Reader.
     *
     * @return the plaintext secret, or {@code null} when the Reader has none
     */
    String revealClientSecret(String proxyProcessId);
}

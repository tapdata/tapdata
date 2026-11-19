package com.tapdata.tm.proxyprocess.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.proxyprocess.client.ProxyProcessHttpClient;
import com.tapdata.tm.proxyprocess.constant.AgentProcessStatus;
import com.tapdata.tm.proxyprocess.dto.ProxyProcessCommandResult;
import com.tapdata.tm.proxyprocess.dto.ProxyProcessHttpRequest;
import com.tapdata.tm.proxyprocess.entity.AgentProcessEntity;
import com.tapdata.tm.proxyprocess.repository.AgentProcessRepository;
import com.tapdata.tm.proxyprocess.service.AgentProcessService;
import com.tapdata.tm.proxyprocess.service.ProxyProcessCommandService;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.Map;

@Service
public class ProxyProcessCommandServiceImpl implements ProxyProcessCommandService {
    private final AgentProcessRepository agentProcessRepository;
    private final AgentProcessService agentProcessService;
    private final ProxyProcessHttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ProxyProcessCommandServiceImpl(AgentProcessRepository agentProcessRepository,
                                          AgentProcessService agentProcessService,
                                          ProxyProcessHttpClient httpClient) {
        this.agentProcessRepository = agentProcessRepository;
        this.agentProcessService = agentProcessService;
        this.httpClient = httpClient;
    }

    @Override
    public ProxyProcessCommandResult execute(String proxyProcessId, String type, Object value) {
        if (StringUtils.isBlank(proxyProcessId) || StringUtils.isBlank(type)) {
            throw new IllegalArgumentException("proxyProcessId and type are required");
        }
        if (!ObjectId.isValid(proxyProcessId)) {
            throw new IllegalArgumentException("Invalid proxyProcessId");
        }

        AgentProcessEntity agentProcess = agentProcessRepository.findById(new ObjectId(proxyProcessId))
                .orElseThrow(() -> new IllegalArgumentException("Agent process not found"));
        if (!AgentProcessStatus.RUNNING.equals(agentProcessService.effectiveStatus(agentProcess))) {
            throw new IllegalStateException("Agent process is not running");
        }
        if (StringUtils.isBlank(agentProcess.getAdvertiseIp()) || agentProcess.getAdvertisePort() == null
                || agentProcess.getAdvertisePort() <= 0 || StringUtils.isBlank(agentProcess.getToken())) {
            throw new IllegalStateException("Agent process network information is incomplete");
        }

        ProxyProcessHttpRequest request = toHttpRequest(value);
        validateHttpRequest(request);
        String targetUrl = buildTargetUrl(agentProcess, request);
        return httpClient.execute(targetUrl, agentProcess.getToken(), request);
    }

    private ProxyProcessHttpRequest toHttpRequest(Object value) {
        if (value instanceof ProxyProcessHttpRequest request) {
            return request;
        }
        if (value == null) {
            throw new IllegalArgumentException("Command value is required");
        }
        try {
            return objectMapper.convertValue(value, ProxyProcessHttpRequest.class);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Command value must contain method and path", e);
        }
    }

    private void validateHttpRequest(ProxyProcessHttpRequest request) {
        if (request == null || StringUtils.isBlank(request.getMethod()) || StringUtils.isBlank(request.getPath())) {
            throw new IllegalArgumentException("Command value must contain method and path");
        }
        String path = request.getPath().trim();
        if (!path.startsWith("/") || path.startsWith("//") || path.contains("://") || path.contains("?")) {
            throw new IllegalArgumentException("Proxy process path must be a relative path");
        }
        URI uri = URI.create(path);
        if (uri.isAbsolute() || uri.getHost() != null) {
            throw new IllegalArgumentException("Proxy process path must not contain a host");
        }
        try {
            org.springframework.http.HttpMethod.valueOf(request.getMethod().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported HTTP method");
        }
    }

    private String buildTargetUrl(AgentProcessEntity agentProcess, ProxyProcessHttpRequest request) {
        UriComponentsBuilder builder = UriComponentsBuilder
                .fromUriString("http://" + agentProcess.getAdvertiseIp() + ":" + agentProcess.getAdvertisePort())
                .path(request.getPath());
        if (request.getQuery() != null) {
            for (Map.Entry<String, Object> entry : request.getQuery().entrySet()) {
                if (StringUtils.isBlank(entry.getKey())) {
                    throw new IllegalArgumentException("Query parameter name must not be blank");
                }
                builder.queryParam(entry.getKey(), entry.getValue());
            }
        }
        return builder.build().encode().toUriString();
    }
}

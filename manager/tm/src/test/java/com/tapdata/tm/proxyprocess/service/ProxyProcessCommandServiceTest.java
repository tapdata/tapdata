package com.tapdata.tm.proxyprocess.service;

import com.tapdata.tm.proxyprocess.client.ProxyProcessHttpClient;
import com.tapdata.tm.proxyprocess.constant.AgentProcessStatus;
import com.tapdata.tm.proxyprocess.dto.ProxyProcessCommandResult;
import com.tapdata.tm.proxyprocess.dto.ProxyProcessHttpRequest;
import com.tapdata.tm.proxyprocess.entity.AgentProcessEntity;
import com.tapdata.tm.proxyprocess.repository.AgentProcessRepository;
import com.tapdata.tm.proxyprocess.service.impl.ProxyProcessCommandServiceImpl;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ProxyProcessCommandServiceTest {
    @Test
    void executeShouldResolveExplicitProxyProcessAndForwardSynchronously() {
        AgentProcessRepository agentProcessRepository = mock(AgentProcessRepository.class);
        AgentProcessService agentProcessService = mock(AgentProcessService.class);
        ProxyProcessHttpClient httpClient = mock(ProxyProcessHttpClient.class);
        String proxyProcessId = new ObjectId().toHexString();
        AgentProcessEntity agent = agent(proxyProcessId);
        when(agentProcessRepository.findById(new ObjectId(proxyProcessId))).thenReturn(Optional.of(agent));
        when(agentProcessService.effectiveStatus(agent)).thenReturn(AgentProcessStatus.RUNNING);
        ProxyProcessCommandResult expected = new ProxyProcessCommandResult();
        expected.setStatusCode(200);
        expected.setBody("ok");
        when(httpClient.execute(anyString(), eq("agent-token"), any(ProxyProcessHttpRequest.class))).thenReturn(expected);
        ProxyProcessCommandService service = new ProxyProcessCommandServiceImpl(
                agentProcessRepository, agentProcessService, httpClient);

        ProxyProcessHttpRequest request = new ProxyProcessHttpRequest();
        request.setMethod("POST");
        request.setPath("/api/db2-z/cdc/start");
        request.setQuery(Map.of("slot", "cdc-1"));
        request.setBody(Map.of("checkpoint", 10));

        ProxyProcessCommandResult result = service.execute(proxyProcessId, "start", request);

        assertEquals(expected, result);
        verify(httpClient).execute(eq("http://10.0.0.1:8080/api/db2-z/cdc/start?slot=cdc-1"),
                eq("agent-token"), eq(request));
    }

    @Test
    void executeShouldRejectMissingProxyProcessAndAbsolutePath() {
        AgentProcessRepository agentProcessRepository = mock(AgentProcessRepository.class);
        AgentProcessService agentProcessService = mock(AgentProcessService.class);
        ProxyProcessHttpClient httpClient = mock(ProxyProcessHttpClient.class);
        ProxyProcessCommandService service = new ProxyProcessCommandServiceImpl(
                agentProcessRepository, agentProcessService, httpClient);
        ProxyProcessHttpRequest request = new ProxyProcessHttpRequest();
        request.setMethod("GET");
        request.setPath("http://untrusted.example/api");

        assertThrows(IllegalArgumentException.class,
                () -> service.execute(new ObjectId().toHexString(), "query", request));
        verifyNoInteractions(httpClient);
    }

    @Test
    void executeShouldRejectOfflineOrFailedAgent() {
        AgentProcessRepository agentProcessRepository = mock(AgentProcessRepository.class);
        AgentProcessService agentProcessService = mock(AgentProcessService.class);
        ProxyProcessHttpClient httpClient = mock(ProxyProcessHttpClient.class);
        String proxyProcessId = new ObjectId().toHexString();
        AgentProcessEntity agent = agent(proxyProcessId);
        when(agentProcessRepository.findById(new ObjectId(proxyProcessId))).thenReturn(Optional.of(agent));
        when(agentProcessService.effectiveStatus(agent)).thenReturn(AgentProcessStatus.OFFLINE);
        ProxyProcessCommandService service = new ProxyProcessCommandServiceImpl(
                agentProcessRepository, agentProcessService, httpClient);
        ProxyProcessHttpRequest request = new ProxyProcessHttpRequest();
        request.setMethod("GET");
        request.setPath("/api/health");

        assertThrows(IllegalStateException.class,
                () -> service.execute(proxyProcessId, "query", request));
        verifyNoInteractions(httpClient);
    }

    private AgentProcessEntity agent(String proxyProcessId) {
        AgentProcessEntity agent = new AgentProcessEntity();
        agent.setId(new ObjectId(proxyProcessId));
        agent.setAgentType("Db2 for z cdc reader");
        agent.setAdvertiseIp("10.0.0.1");
        agent.setAdvertisePort(8080);
        agent.setToken("agent-token");
        return agent;
    }
}

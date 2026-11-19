package com.tapdata.tm.proxyprocess.service.impl;

import com.tapdata.tm.proxyprocess.constant.AgentProcessStatus;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterResponseDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessStatusDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessQueryDto;
import com.tapdata.tm.proxyprocess.entity.AgentProcessEntity;
import com.tapdata.tm.proxyprocess.repository.AgentProcessRepository;
import com.tapdata.tm.base.dto.Page;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AgentProcessServiceImplTest {

    @Test
    void registerShouldCreateProcessAndReturnToken() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        when(repository.findByAgentId("agent-1")).thenReturn(Optional.empty());
        when(repository.saveAgentProcess(any(AgentProcessEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        AgentProcessServiceImpl service = new AgentProcessServiceImpl(repository);

        AgentProcessRegisterDto request = registerRequest();
        AgentProcessRegisterResponseDto response = service.register(request);

        assertNotNull(response.getToken());
        assertFalse(response.getToken().isBlank());
        assertEquals("agent-1", response.getAgentId());
        verify(repository).saveAgentProcess(argThat(entity ->
                "agent-1".equals(entity.getAgentId())
                        && "Db2 for z cdc reader".equals(entity.getAgentType())
                        && entity.getToken() != null
                        && entity.getStatus() == null));
    }

    @Test
    void registerWithSameAgentAndStartTimeShouldReuseToken() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        AgentProcessEntity existing = entity("agent-1", "token-1");
        existing.setStartTime(1000L);
        when(repository.findByAgentId("agent-1")).thenReturn(Optional.of(existing));
        when(repository.saveAgentProcess(any(AgentProcessEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        AgentProcessServiceImpl service = new AgentProcessServiceImpl(repository);

        AgentProcessRegisterDto request = registerRequest();
        request.setStartTime(new Date(1000));

        AgentProcessRegisterResponseDto response = service.register(request);

        assertEquals("token-1", response.getToken());
        verify(repository).saveAgentProcess(existing);
    }

    @Test
    void registerWithNewStartTimeShouldRotateTokenAndClearOldRuntimeMetrics() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        AgentProcessEntity existing = entity("agent-1", "token-1");
        existing.setStartTime(1000L);
        existing.setPid(11L);
        existing.setCpuUsage(20D);
        existing.setMemoryUsage(30D);
        when(repository.findByAgentId("agent-1")).thenReturn(Optional.of(existing));
        when(repository.saveAgentProcess(any(AgentProcessEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        AgentProcessServiceImpl service = new AgentProcessServiceImpl(repository);
        AgentProcessRegisterDto request = registerRequest();
        request.setStartTime(new Date(2000));

        AgentProcessRegisterResponseDto response = service.register(request);

        assertNotEquals("token-1", response.getToken());
        assertNull(existing.getPid());
        assertNull(existing.getCpuUsage());
        assertNull(existing.getMemoryUsage());
    }

    @Test
    void statusReportShouldUpdateMetricsAndStatus() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        AgentProcessEntity existing = entity("agent-1", "token-1");
        existing.setStartTime(1000L);
        when(repository.findByToken("token-1")).thenReturn(Optional.of(existing));
        when(repository.saveAgentProcess(any(AgentProcessEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        AgentProcessServiceImpl service = new AgentProcessServiceImpl(repository);

        AgentProcessStatusDto report = new AgentProcessStatusDto();
        report.setAgentId("agent-1");
        report.setStartTime(new Date(1000));
        report.setPid(1234L);
        report.setStatus(AgentProcessStatus.RUNNING);
        report.setStatusReportTime(new Date());
        report.setCpuUsage(12.5D);
        report.setMemoryUsage(34.5D);

        service.reportStatus(report, "Bearer token-1");

        verify(repository).saveAgentProcess(argThat(entity ->
                AgentProcessStatus.RUNNING.equals(entity.getStatus())
                        && Long.valueOf(1234L).equals(entity.getPid())
                        && Double.valueOf(12.5D).equals(entity.getCpuUsage())
                        && Double.valueOf(34.5D).equals(entity.getMemoryUsage())
                        && entity.getStatusReportTime() != null));
    }

    @Test
    void statusShouldBecomeOfflineAfterFiveMinutesWithoutReport() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        AgentProcessEntity existing = entity("agent-1", "token-1");
        existing.setStatus(AgentProcessStatus.RUNNING);
        existing.setStatusReportTime(System.currentTimeMillis() - 5 * 60 * 1000L - 1);
        when(repository.findByToken("token-1")).thenReturn(Optional.of(existing));
        AgentProcessServiceImpl service = new AgentProcessServiceImpl(repository);

        assertEquals(AgentProcessStatus.OFFLINE, service.effectiveStatus(existing));
    }

    @Test
    void failedStatusShouldRemainFailedWithItsErrorReason() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        AgentProcessServiceImpl service = new AgentProcessServiceImpl(repository);
        AgentProcessEntity existing = entity("agent-1", "token-1");
        existing.setStatus(AgentProcessStatus.FAILED);
        existing.setErrorReason("CDC reader stopped");
        existing.setStatusReportTime(System.currentTimeMillis() - 10 * 60 * 1000L);

        assertEquals(AgentProcessStatus.FAILED, service.effectiveStatus(existing));
        assertEquals("CDC reader stopped", service.toDto(existing).getErrorReason());
    }

    @Test
    void statusReportShouldRejectUnknownToken() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        when(repository.findByToken("missing")).thenReturn(Optional.empty());
        AgentProcessServiceImpl service = new AgentProcessServiceImpl(repository);

        AgentProcessStatusDto report = new AgentProcessStatusDto();
        report.setAgentId("agent-1");

        assertThrows(IllegalArgumentException.class, () -> service.reportStatus(report, "Bearer missing"));
    }

    @Test
    void statusReportShouldRejectUnknownStatus() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        AgentProcessEntity existing = entity("agent-1", "token-1");
        existing.setStartTime(1000L);
        when(repository.findByToken("token-1")).thenReturn(Optional.of(existing));
        AgentProcessServiceImpl service = new AgentProcessServiceImpl(repository);
        AgentProcessStatusDto report = new AgentProcessStatusDto();
        report.setAgentId("agent-1");
        report.setStartTime(new Date(1000));
        report.setPid(1L);
        report.setStatus("UNKNOWN");
        report.setStatusReportTime(new Date());

        assertThrows(IllegalArgumentException.class, () -> service.reportStatus(report, "Bearer token-1"));
    }

    @Test
    void registerStatusAndListShouldExposeCurrentStateWithoutToken() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        AgentProcessEntity[] persisted = new AgentProcessEntity[1];
        when(repository.findByAgentId("agent-1")).thenReturn(Optional.empty());
        when(repository.saveAgentProcess(any(AgentProcessEntity.class))).thenAnswer(invocation -> {
            persisted[0] = invocation.getArgument(0);
            return persisted[0];
        });
        AgentProcessServiceImpl service = new AgentProcessServiceImpl(repository);

        AgentProcessRegisterResponseDto registration = service.register(registerRequest());
        when(repository.findByToken(registration.getToken())).thenReturn(Optional.of(persisted[0]));
        AgentProcessStatusDto report = new AgentProcessStatusDto();
        report.setAgentId("agent-1");
        report.setStartTime(new Date(persisted[0].getStartTime()));
        report.setPid(99L);
        report.setStatus(AgentProcessStatus.RUNNING);
        report.setStatusReportTime(new Date());
        service.reportStatus(report, "Bearer " + registration.getToken());
        when(repository.findAll(any(org.springframework.data.mongodb.core.query.Query.class)))
                .thenReturn(java.util.List.of(persisted[0]));

        AgentProcessQueryDto query = new AgentProcessQueryDto();
        Page<com.tapdata.tm.proxyprocess.dto.AgentProcessDto> page = service.list(query);

        assertEquals(1, page.getTotal());
        assertEquals(AgentProcessStatus.RUNNING, page.getItems().get(0).getStatus());
        assertFalse(page.getItems().get(0).toString().contains(registration.getToken()));
    }

    @Test
    void listShouldFilterTypeAndEffectiveStatusAndPageByRequestedOrder() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        AgentProcessEntity offline = entity("agent-offline", "token-offline");
        offline.setAgentName("z-name");
        offline.setAgentType("Db2 for z cdc reader");
        offline.setStatus(AgentProcessStatus.RUNNING);
        offline.setStatusReportTime(System.currentTimeMillis() - 6 * 60 * 1000L);
        AgentProcessEntity running = entity("agent-running", "token-running");
        running.setAgentName("a-name");
        running.setAgentType("Db2 for z cdc reader");
        running.setStatus(AgentProcessStatus.RUNNING);
        running.setStatusReportTime(System.currentTimeMillis());
        when(repository.findAll(any(org.springframework.data.mongodb.core.query.Query.class)))
                .thenReturn(List.of(offline, running));
        AgentProcessServiceImpl service = new AgentProcessServiceImpl(repository);

        AgentProcessQueryDto query = new AgentProcessQueryDto();
        query.setAgentType("Db2 for z cdc reader");
        query.setStatus(AgentProcessStatus.OFFLINE);
        query.setSortBy("agentName");
        query.setSortOrder("ASC");
        query.setPage(1);
        query.setPageSize(10);

        Page<com.tapdata.tm.proxyprocess.dto.AgentProcessDto> result = service.list(query);

        assertEquals(1, result.getTotal());
        assertEquals("agent-offline", result.getItems().get(0).getAgentId());
        assertEquals(AgentProcessStatus.OFFLINE, result.getItems().get(0).getStatus());
    }

    @Test
    void listShouldFilterByProxyProcessId() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        String proxyProcessId = new ObjectId().toHexString();
        AgentProcessEntity target = entity("agent-target", "token-target");
        target.setId(new ObjectId(proxyProcessId));
        AgentProcessEntity other = entity("agent-other", "token-other");
        other.setId(new ObjectId());
        when(repository.findAll(any(org.springframework.data.mongodb.core.query.Query.class)))
                .thenReturn(List.of(target, other));
        AgentProcessServiceImpl service = new AgentProcessServiceImpl(repository);

        AgentProcessQueryDto query = new AgentProcessQueryDto();
        query.setProxyProcessId(proxyProcessId);

        Page<com.tapdata.tm.proxyprocess.dto.AgentProcessDto> result = service.list(query);

        assertEquals(1, result.getTotal());
        assertEquals("agent-target", result.getItems().get(0).getAgentId());
    }

    @Test
    void deleteShouldRemoveProcessRegistration() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        String proxyProcessId = new ObjectId().toHexString();
        when(repository.deleteById(any(ObjectId.class))).thenReturn(true);
        AgentProcessServiceImpl service = new AgentProcessServiceImpl(repository);

        service.delete(proxyProcessId);

        verify(repository).deleteById(new ObjectId(proxyProcessId));
    }

    private AgentProcessRegisterDto registerRequest() {
        AgentProcessRegisterDto request = new AgentProcessRegisterDto();
        request.setAgentName("proxy-1");
        request.setAgentType("Db2 for z cdc reader");
        request.setAgentId("agent-1");
        request.setAdvertiseIp("10.0.0.1");
        request.setAdvertisePort(8080);
        request.setStartTime(new Date());
        return request;
    }

    private AgentProcessEntity entity(String agentId, String token) {
        AgentProcessEntity entity = new AgentProcessEntity();
        entity.setAgentId(agentId);
        entity.setAgentType("Db2 for z cdc reader");
        entity.setToken(token);
        return entity;
    }
}

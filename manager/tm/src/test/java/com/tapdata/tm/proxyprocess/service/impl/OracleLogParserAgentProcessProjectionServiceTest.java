package com.tapdata.tm.proxyprocess.service.impl;

import com.tapdata.tm.cluster.dto.RawServerInfo;
import com.tapdata.tm.cluster.dto.RawServerStateDto;
import com.tapdata.tm.cluster.service.RawServerStateService;
import com.tapdata.tm.proxyprocess.constant.AgentProcessType;
import com.tapdata.tm.proxyprocess.entity.OracleLogParserAgentProcessEntity;
import com.tapdata.tm.proxyprocess.repository.AgentProcessRepository;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class OracleLogParserAgentProcessProjectionServiceTest {

    @Test
    void handleReportShouldPersistBareServiceIdAsAgentId() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        RawServerStateService rawServerStateService = mock(RawServerStateService.class);
        when(repository.upsertOracleLogParser(any(OracleLogParserAgentProcessEntity.class))).thenReturn(true);
        OracleLogParserAgentProcessProjectionService service =
                new OracleLogParserAgentProcessProjectionService(repository, rawServerStateService);

        service.handleReport(state("service-1"));

        verify(repository).upsertOracleLogParser(argThat(entity ->
                "service-1".equals(entity.getAgentId())
                        && AgentProcessType.ORACLE_LOG_PARSER.getValue().equals(entity.getAgentType())));
    }

    @Test
    void refreshServiceWithoutLatestStateShouldDeleteByBareServiceId() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        RawServerStateService rawServerStateService = mock(RawServerStateService.class);
        when(rawServerStateService.getLatestOracleLogParserState("service-1")).thenReturn(List.of());
        OracleLogParserAgentProcessProjectionService service =
                new OracleLogParserAgentProcessProjectionService(repository, rawServerStateService);

        service.refreshService("service-1");

        // Must match the key written by upsert(), otherwise the stale projection is never removed.
        verify(repository).deleteOracleLogParser("service-1");
        verify(repository, never()).deleteOracleLogParser("OracleLogParser:service-1");
        verify(repository, never()).upsertOracleLogParser(any());
    }

    @Test
    void refreshServiceShouldReuseLatestState() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        RawServerStateService rawServerStateService = mock(RawServerStateService.class);
        when(rawServerStateService.getLatestOracleLogParserState("service-1"))
                .thenReturn(List.of(state("service-1")));
        OracleLogParserAgentProcessProjectionService service =
                new OracleLogParserAgentProcessProjectionService(repository, rawServerStateService);

        service.refreshService("service-1");

        verify(repository).upsertOracleLogParser(argThat(entity -> "service-1".equals(entity.getAgentId())));
        verify(repository, never()).deleteOracleLogParser(any());
    }

    @Test
    void refreshServiceShouldIgnoreBlankServiceId() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        RawServerStateService rawServerStateService = mock(RawServerStateService.class);
        OracleLogParserAgentProcessProjectionService service =
                new OracleLogParserAgentProcessProjectionService(repository, rawServerStateService);

        service.refreshService(null);
        service.refreshService("  ");

        // A null/blank key would turn the Mongo $eq filter into a match on missing fields.
        verifyNoInteractions(repository);
        verifyNoInteractions(rawServerStateService);
    }

    @Test
    void initializeLatestReportsShouldFailFastWithFailedServiceIds() {
        AgentProcessRepository repository = mock(AgentProcessRepository.class);
        RawServerStateService rawServerStateService = mock(RawServerStateService.class);
        when(rawServerStateService.getLatestOracleLogParserStates())
                .thenReturn(List.of(state("service-1"), state("service-2")));
        when(repository.upsertOracleLogParser(any(OracleLogParserAgentProcessEntity.class)))
                .thenAnswer(invocation -> {
                    OracleLogParserAgentProcessEntity entity = invocation.getArgument(0);
                    if ("service-2".equals(entity.getAgentId())) {
                        throw new IllegalStateException("boom");
                    }
                    return true;
                });
        OracleLogParserAgentProcessProjectionService service =
                new OracleLogParserAgentProcessProjectionService(repository, rawServerStateService);

        IllegalStateException error = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, service::initializeLatestReports);
        assertTrue(error.getMessage().contains("service-2"));
    }

    private static RawServerStateDto state(String serviceId) {
        RawServerInfo reportedData = new RawServerInfo(100L, System.currentTimeMillis(), 1.0, 2.0,
                "sleep", null, "parser-" + serviceId);
        RawServerStateDto dto = new RawServerStateDto();
        dto.setServiceId(serviceId);
        dto.setDataSource(AgentProcessType.ORACLE_LOG_PARSER.getValue());
        dto.setTimestamp(new Date());
        dto.setServiceIP("127.0.0.1");
        dto.setServicePort(8080);
        dto.setReportedData(reportedData);
        return dto;
    }
}

package com.tapdata.tm.proxyprocess.service.impl;

import com.tapdata.tm.cluster.dto.RawServerInfo;
import com.tapdata.tm.cluster.dto.RawServerStateDto;
import com.tapdata.tm.cluster.service.RawServerStateService;
import com.tapdata.tm.proxyprocess.constant.AgentProcessStatus;
import com.tapdata.tm.proxyprocess.constant.AgentProcessType;
import com.tapdata.tm.proxyprocess.entity.OracleLogParserAgentProcessEntity;
import com.tapdata.tm.proxyprocess.repository.AgentProcessRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.List;

/** Projects the latest Oracle Log Parser reports into the AgentProcess collection. */
@Service
@Slf4j
@RequiredArgsConstructor
public class OracleLogParserAgentProcessProjectionService {
    private static final String AGENT_TYPE = AgentProcessType.ORACLE_LOG_PARSER.getValue();
    private static final String AGENT_ID_PREFIX = AGENT_TYPE + ":";

    private final AgentProcessRepository agentProcessRepository;
    private final RawServerStateService rawServerStateService;

    public int initializeLatestReports() {
        List<RawServerStateDto> latestStates = rawServerStateService.getLatestOracleLogParserStates();
        int projected = 0;
        int skipped = 0;
        List<String> failedServiceIds = new java.util.ArrayList<>();
        for (RawServerStateDto state : latestStates) {
            try {
                if (upsert(state)) {
                    projected++;
                } else {
                    skipped++;
                }
            } catch (Exception e) {
                failedServiceIds.add(state == null ? null : state.getServiceId());
                log.error("Failed to initialize OracleLogParser AgentProcess for serviceId {}",
                        state == null ? null : state.getServiceId(), e);
            }
        }
        log.info("OracleLogParser AgentProcess initialization finished: total={}, projected={}, skipped={}, failedServiceIds={}",
                latestStates.size(), projected, skipped, failedServiceIds);
        if (!failedServiceIds.isEmpty()) {
            throw new IllegalStateException("Failed to initialize OracleLogParser AgentProcess for serviceIds: "
                    + failedServiceIds);
        }
        return projected;
    }

    public void handleReport(RawServerStateDto state) {
        upsert(state);
    }

    public void refreshService(String serviceId) {
        if (StringUtils.isBlank(serviceId)) {
            return;
        }
        List<RawServerStateDto> latestStates = rawServerStateService.getLatestOracleLogParserState(serviceId);
        if (latestStates.isEmpty()) {
            agentProcessRepository.deleteOracleLogParser(agentId(serviceId));
            return;
        }
        upsert(latestStates.get(0));
    }

    public static boolean isOracleLogParserDataSource(String dataSource) {
        return RawServerStateService.ORACLE_LOG_PARSER_DATA_SOURCES.contains(dataSource);
    }

    private boolean upsert(RawServerStateDto state) {
        if (state == null || StringUtils.isBlank(state.getServiceId()) || state.getTimestamp() == null) {
            log.warn("Skip OracleLogParser report without serviceId or timestamp");
            return false;
        }

        RawServerInfo reportedData = state.getReportedData();
        String sourceState = reportedData == null ? "notExists" : reportedData.getState();
        OracleLogParserAgentProcessEntity process = new OracleLogParserAgentProcessEntity();
        process.setAgentId(state.getServiceId());
        process.setAgentType(AGENT_TYPE);
        process.setAgentName(reportedData == null ? null : reportedData.getName());
        process.setSourceServiceId(state.getServiceId());
        process.setSourceState(sourceState);
        process.setAdvertiseIp(state.getServiceIP());
        process.setAdvertisePort(state.getServicePort());
        process.setStartTime(reportedData == null ? null : reportedData.getCreateTime());
        process.setStatus(status(sourceState));
        process.setStatusReportTime(state.getTimestamp().getTime());
        process.setPid(reportedData == null ? null : reportedData.getPid());
        process.setCpuUsage(reportedData == null ? null : reportedData.getCpuPercent());
        process.setMemoryUsage(reportedData == null || reportedData.getMemoryPercent() == null
                ? null : reportedData.getMemoryPercent().doubleValue());
        process.setErrorReason(reportedData == null ? null : reportedData.getMsg());
        process.setLastRegisterTime(state.getTimestamp().getTime());

        return agentProcessRepository.upsertOracleLogParser(process);
    }

    private String status(String sourceState) {
        if ("sleep".equals(sourceState)) {
            return AgentProcessStatus.RUNNING;
        }
        if ("notExists".equals(sourceState)) {
            return AgentProcessStatus.OFFLINE;
        }
        return AgentProcessStatus.FAILED;
    }

    private String agentId(String serviceId) {
        return AGENT_ID_PREFIX + serviceId;
    }
}

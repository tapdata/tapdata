package com.tapdata.tm.throttle.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.throttle.AgentThrottleCapabilities;
import com.tapdata.tm.commons.throttle.AgentThrottleDeltaResponse;
import com.tapdata.tm.commons.throttle.TaskThrottleApplyReceipt;
import com.tapdata.tm.commons.throttle.TaskThrottleStopEvidenceDto;
import com.tapdata.tm.commons.throttle.TaskThrottleTelemetrySample;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.throttle.entity.AgentThrottleCapabilityEntity;
import com.tapdata.tm.throttle.entity.TaskThrottleStopEvidenceEntity;
import com.tapdata.tm.throttle.service.TaskThrottleExecutionGuardService;
import com.tapdata.tm.throttle.service.TaskThrottleTelemetryAndRankingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping({"/api/agent-throttle", "/agent-throttle"})
public class AgentThrottleController extends BaseController {

    private static final Logger logger = LoggerFactory.getLogger(AgentThrottleController.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final TypeReference<List<TaskThrottleApplyReceipt>> RECEIPT_LIST_TYPE = new TypeReference<>() {
    };
    private static final TypeReference<Map<String, List<TaskThrottleTelemetrySample>>> TELEMETRY_MAP_TYPE = new TypeReference<>() {
    };

    private final TaskThrottleExecutionGuardService guardService;
    private final TaskThrottleTelemetryAndRankingService telemetryService;

    @Autowired
    public AgentThrottleController(
            TaskThrottleExecutionGuardService guardService,
            TaskThrottleTelemetryAndRankingService telemetryService) {
        this.guardService = guardService;
        this.telemetryService = telemetryService;
    }

    @PostMapping("/capabilities")
    public ResponseMessage<AgentThrottleCapabilityEntity> registerCapabilities(
            @RequestBody AgentThrottleCapabilities capabilities) {
        return success(guardService.registerCapabilities(capabilities));
    }

    @GetMapping("/delta")
    public ResponseMessage<AgentThrottleDeltaResponse> pullDelta(
            @RequestParam("agentId") String agentId,
            @RequestParam(value = "cursor", defaultValue = "0") long cursor) {
        return success(telemetryService.pullDelta(agentId, cursor));
    }

    @PostMapping("/receipts")
    public ResponseMessage<Void> uploadReceipts(@RequestBody Map<String, Object> payload) {
        if (payload != null) {
            String agentId = payload.get("agentId") == null ? null : String.valueOf(payload.get("agentId"));
            Object raw = payload.get("receipts");
            if (raw != null) {
                List<TaskThrottleApplyReceipt> receipts = MAPPER.convertValue(raw, RECEIPT_LIST_TYPE);
                telemetryService.ingestReceipts(agentId, receipts);
            }
        }
        return success();
    }

    @PostMapping("/telemetry")
    public ResponseMessage<Void> uploadTelemetry(@RequestBody Map<String, Object> payload) {
        if (payload != null) {
            String agentId = payload.get("agentId") == null ? null : String.valueOf(payload.get("agentId"));
            Object raw = payload.get("samplesByTask");
            if (raw != null) {
                Map<String, List<TaskThrottleTelemetrySample>> samples = MAPPER.convertValue(raw, TELEMETRY_MAP_TYPE);
                telemetryService.ingestTelemetry(agentId, samples);
            }
        }
        return success();
    }

    @PostMapping("/stop-evidence")
    public ResponseMessage<TaskThrottleStopEvidenceEntity> uploadStopEvidence(
            @RequestBody TaskThrottleStopEvidenceDto dto) {
        try {
            TaskThrottleStopEvidenceEntity saved = guardService.submitStopEvidence(dto, false);
            clearTaskLiveStatus(dto);
            return success(saved);
        } catch (BizException ex) {
            if (ThrottleConstants.ERR_STALE_RUN_EPOCH.equals(ex.getErrorCode())) {
                logger.info("Ignoring stale or deleted-task stop evidence from agent {}: {}",
                        dto == null ? null : dto.getOldAgentId(), ex.getMessage());
                return success(null);
            }
            throw ex;
        }
    }

    private void clearTaskLiveStatus(TaskThrottleStopEvidenceDto dto) {
        if (dto != null && dto.getTaskId() != null) {
            telemetryService.clearLiveStatus(dto.getTaskId());
        }
    }
}

package com.tapdata.tm.throttle.controller;

import com.tapdata.tm.audit.AuditAction;
import com.tapdata.tm.audit.Audited;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.throttle.TaskThrottleStopEvidenceDto;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.service.ModuleAccess;
import com.tapdata.tm.throttle.dto.LogCollectorThrottleOverviewView;
import com.tapdata.tm.throttle.dto.TaskThrottleLiveStatusView;
import com.tapdata.tm.throttle.dto.TaskThrottlePolicyUpdateRequest;
import com.tapdata.tm.throttle.dto.TaskThrottlePolicyView;
import com.tapdata.tm.throttle.dto.TaskThrottleRankingResponse;
import com.tapdata.tm.throttle.dto.TaskThrottleSampleConfigView;
import com.tapdata.tm.throttle.dto.TaskThrottleVersionConflictException;
import com.tapdata.tm.throttle.entity.TaskThrottleChangeLogEntity;
import com.tapdata.tm.throttle.entity.TaskThrottleStopEvidenceEntity;
import com.tapdata.tm.throttle.service.LogCollectorThrottleOverviewService;
import com.tapdata.tm.throttle.service.TaskThrottleExecutionGuardService;
import com.tapdata.tm.throttle.service.TaskThrottlePolicyService;
import com.tapdata.tm.throttle.service.TaskThrottleSampleConfigService;
import com.tapdata.tm.throttle.service.TaskThrottleTelemetryAndRankingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/task-throttle")
public class TaskThrottleController extends BaseController {

    public static final String SETTINGS_PERMISSION = "v2_system_settings_menu";

    private final TaskThrottlePolicyService policyService;
    private final TaskThrottleExecutionGuardService guardService;
    private final TaskThrottleTelemetryAndRankingService telemetryService;
    private final TaskThrottleSampleConfigService sampleConfigService;

    @Autowired(required = false)
    private LogCollectorThrottleOverviewService logCollectorOverviewService;

    @Autowired(required = false)
    private ModuleAccess moduleAccess;

    @Autowired
    public TaskThrottleController(
            TaskThrottlePolicyService policyService,
            TaskThrottleExecutionGuardService guardService,
            TaskThrottleTelemetryAndRankingService telemetryService,
            TaskThrottleSampleConfigService sampleConfigService) {
        this.policyService = policyService;
        this.guardService = guardService;
        this.telemetryService = telemetryService;
        this.sampleConfigService = sampleConfigService;
    }

    public void setLogCollectorOverviewService(LogCollectorThrottleOverviewService logCollectorOverviewService) {
        this.logCollectorOverviewService = logCollectorOverviewService;
    }

    public void setModuleAccess(ModuleAccess moduleAccess) {
        this.moduleAccess = moduleAccess;
    }

    @GetMapping("/logCollector/{taskId}/overview")
    public ResponseMessage<LogCollectorThrottleOverviewView> getLogCollectorOverview(
            @PathVariable("taskId") String taskId) {
        if (logCollectorOverviewService == null) {
            throw new BizException(ThrottleConstants.ERR_UNSUPPORTED_PATH, "LogCollector overview service is unavailable");
        }
        return success(logCollectorOverviewService.getOverview(taskId, currentUser()));
    }

    protected UserDetail currentUser() {
        return getLoginUser();
    }

    @GetMapping("/{kind}/{taskId}")
    public ResponseMessage<TaskThrottlePolicyView> getPolicy(
            @PathVariable("kind") String kind,
            @PathVariable("taskId") String taskId) {
        return success(policyService.getPolicyView(kind, taskId, currentUser()));
    }

    @PutMapping("/{kind}/{taskId}")
    @Audited(value = AuditAction.TASK_UPDATE, objectId = "#taskId")
    public ResponseEntity<ResponseMessage<Object>> updatePolicy(
            @PathVariable("kind") String kind,
            @PathVariable("taskId") String taskId,
            @RequestBody TaskThrottlePolicyUpdateRequest request) {
        try {
            TaskThrottlePolicyView updated = policyService.updatePolicy(kind, taskId, request, currentUser());
            return ResponseEntity.ok(success(updated));
        } catch (TaskThrottleVersionConflictException conflict) {
            ResponseMessage<Object> msg = new ResponseMessage<>();
            msg.setCode(conflict.getErrorCode());
            msg.setMessage(conflict.getMessage());
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("errorCode", conflict.getErrorCode());
            detail.put("latestVersion", conflict.getLatestVersion());
            detail.put("latestView", conflict.getLatestView());
            msg.setData(detail);
            return ResponseEntity.status(HttpStatus.CONFLICT).body(msg);
        }
    }

    public ResponseMessage<List<TaskThrottleChangeLogEntity>> listChanges(
            String kind, String taskId, int page, int size) {
        return listChanges(kind, taskId, page, size, null, null, null);
    }

    @GetMapping("/{kind}/{taskId}/changes")
    public ResponseMessage<List<TaskThrottleChangeLogEntity>> listChanges(
            @PathVariable("kind") String kind,
            @PathVariable("taskId") String taskId,
            @RequestParam(value = "page", defaultValue = "0") int page,
            @RequestParam(value = "size", defaultValue = "50") int size,
            @RequestParam(value = "startAt", required = false) Long startAt,
            @RequestParam(value = "endAt", required = false) Long endAt,
            @RequestParam(value = "eventType", required = false) String eventType) {
        return success(policyService.listChanges(kind, taskId, currentUser(), page, size, startAt, endAt, eventType));
    }

    @PostMapping("/{kind}/{taskId}/stop-evidence")
    @Audited(value = AuditAction.TASK_UPDATE, objectId = "#taskId")
    public ResponseMessage<TaskThrottleStopEvidenceEntity> submitManualStopEvidence(
            @PathVariable("kind") String kind,
            @PathVariable("taskId") String taskId,
            @RequestBody TaskThrottleStopEvidenceDto request) {
        UserDetail user = currentUser();
        String normalizedKind = policyService.normalizeAndValidateKind(kind);
        policyService.validateTaskAccess(taskId, normalizedKind, user);
        if (request == null) {
            throw new BizException(ThrottleConstants.ERR_STOP_EVIDENCE_REQUIRED, "Stop evidence payload is required");
        }
        request.setTaskId(taskId);
        request.setTaskKind(normalizedKind);
        request.setSource(TaskThrottleStopEvidenceDto.SOURCE_OPERATOR_MANUAL);
        if ((request.getSubmittedBy() == null || request.getSubmittedBy().isBlank()) && user != null) {
            request.setSubmittedBy(user.getUsername() != null ? user.getUsername() : user.getUserId());
        }
        TaskThrottleStopEvidenceEntity saved = guardService.submitStopEvidence(request, true);
        telemetryService.clearLiveStatus(taskId);
        return success(saved);
    }

    @GetMapping("/live-status")
    public ResponseMessage<Map<String, TaskThrottleLiveStatusView>> getLiveStatuses(
            @RequestParam(value = "taskIds", required = false) List<String> taskIds) {
        List<String> authorizedTaskIds = policyService.filterAccessibleTaskIds(taskIds, currentUser());
        return success(telemetryService.evaluateLiveStatuses(authorizedTaskIds));
    }

    @GetMapping("/ranking")
    public ResponseMessage<TaskThrottleRankingResponse> getRanking(
            @RequestParam(value = "direction", defaultValue = "OUTPUT") ThrottleDirection direction,
            @RequestParam(value = "windowSeconds", defaultValue = "300") long windowSeconds,
            @RequestParam(value = "top", defaultValue = "5") int top) {
        return success(telemetryService.queryEstimatedDataRateRanking(currentUser(), direction, windowSeconds, top));
    }

    @GetMapping("/sample-config")
    public ResponseMessage<TaskThrottleSampleConfigView> getSampleConfig() {
        if (moduleAccess != null) {
            moduleAccess.require(currentUser(), SETTINGS_PERMISSION);
        }
        return success(sampleConfigService.getConfigView());
    }

    @PutMapping("/sample-config")
    @Audited(value = AuditAction.SETTINGS_UPDATE)
    public ResponseMessage<TaskThrottleSampleConfigView> updateSampleConfig(
            @RequestBody Map<String, Object> body) {
        if (moduleAccess != null) {
            moduleAccess.require(currentUser(), SETTINGS_PERMISSION);
        }
        Object rawValue = null;
        if (body != null) {
            rawValue = body.containsKey("intervalSeconds") ? body.get("intervalSeconds") : body.get("value");
        }
        return success(sampleConfigService.updateInterval(rawValue));
    }
}

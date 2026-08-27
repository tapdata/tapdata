package com.tapdata.tm.dql.service;

import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.alarm.AlarmComponentEnum;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.AlarmTypeEnum;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import com.tapdata.tm.dql.vo.DqlStormGuardReportVo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class DqlEventAlarmService {
    private static final int MAX_SAFE_TEXT_LENGTH = 512;
    private static final String REDACTED = "[redacted]";
    private static final String[] SENSITIVE_TOKENS = {
            "password", "passwd", "secret", "token", "access_token", "authorization", "credential",
            "apikey", "payload", "recordidentity", "eventkey", "stacktrace"
    };
    private static final Logger LOGGER = LoggerFactory.getLogger(DqlEventAlarmService.class);

    @Autowired(required = false)
    private AlarmService alarmService;

    public void setAlarmService(AlarmService alarmService) {
        this.alarmService = alarmService;
    }

    public void notifyEventCreated(DqlEventDto event) {
        if (event == null) {
            return;
        }
        Map<String, Object> params = new LinkedHashMap<>();
        put(params, "taskName", event.getTaskName());
        put(params, "eventId", event.getEventId());
        put(params, "sourceTable", event.getSourceTable());
        put(params, "targetTable", event.getTargetTable());
        put(params, "dmlType", event.getDmlType());
        put(params, "errorType", event.getErrorType());
        put(params, "errorCode", event.getErrorCode());
        put(params, "failedAt", event.getFailedAt());
        put(params, "pageUrl", "/exception-events");
        put(params, "alarmDate", Instant.now().toString());
        save(AlarmKeyEnum.TASK_DQL_EVENT, event.getAgentId(), event.getTaskId(), event.getTaskName(), params);
    }

    public void notifySaveFailed(String taskId, String reason) {
        Map<String, Object> params = new LinkedHashMap<>();
        put(params, "taskName", taskId);
        put(params, "taskId", taskId);
        put(params, "eventId", "unknown");
        put(params, "errorCode", "DQL_EVENT_SAVE_FAILED");
        put(params, "errorMessage", reason);
        put(params, "failedAt", new Date());
        put(params, "pageUrl", "/exception-events");
        put(params, "alarmDate", Instant.now().toString());
        save(AlarmKeyEnum.TASK_DQL_SAVE_FAILED, null, taskId, taskId, params);
    }

    public void notifyRecoveryFailed(DqlRecoveryBatchDto batch) {
        notifyRecovery(AlarmKeyEnum.TASK_DQL_RECOVERY_FAILED, batch);
    }

    public void notifyBatchPartialFailed(DqlRecoveryBatchDto batch) {
        notifyRecovery(AlarmKeyEnum.TASK_DQL_RECOVERY_FAILED, batch);
    }

    public void notifyStormGuard(DqlStormGuardReportVo report) {
        if (report == null) {
            return;
        }
        Map<String, Object> params = new LinkedHashMap<>();
        put(params, "taskName", report.getTaskName());
        put(params, "taskId", report.getTaskId());
        put(params, "guardKey", report.getGuardKey());
        put(params, "guardWindowSeconds", report.getWindowSeconds());
        put(params, "guardThreshold", report.getGuardThreshold());
        put(params, "suppressedCountEstimate", report.getSuppressedCountEstimate());
        put(params, "routeDecision", report.getRouteDecision());
        put(params, "safeReason", report.getSafeReason());
        put(params, "pageUrl", "/exception-events");
        put(params, "alarmDate", Instant.now().toString());
        save(AlarmKeyEnum.TASK_DQL_STORM_GUARD, report.getAgentId(), report.getTaskId(), report.getTaskName(), params);
    }

    private void notifyRecovery(AlarmKeyEnum key, DqlRecoveryBatchDto batch) {
        if (batch == null) {
            return;
        }
        Map<String, Object> params = new LinkedHashMap<>();
        put(params, "taskName", batch.getTaskName());
        put(params, "taskId", batch.getTaskId());
        put(params, "batchId", batch.getBatchId());
        put(params, "operatorName", batch.getOperatorName());
        put(params, "recoveryStatus", batch.getStatus());
        put(params, "successCount", defaultCount(batch.getSuccessCount()));
        put(params, "failedCount", defaultCount(batch.getFailedCount()));
        put(params, "skippedCount", defaultCount(batch.getSkippedCount()));
        put(params, "failedAt", batch.getFinishedAt() == null ? new Date() : batch.getFinishedAt());
        put(params, "pageUrl", "/exception-events");
        put(params, "alarmDate", Instant.now().toString());
        save(key, batch.getAgentId(), batch.getTaskId(), batch.getTaskName(), params);
    }

    private void put(Map<String, Object> params, String key, Object value) {
        if (value instanceof String text) {
            params.put(key, safeText(text));
        } else if (value != null) {
            params.put(key, value);
        } else {
            params.put(key, "");
        }
    }

    private Integer defaultCount(Integer value) {
        return value == null ? 0 : Math.max(0, value);
    }

    private String safeText(String value) {
        if (StringUtils.isBlank(value)) {
            return "";
        }
        String compact = value.replaceAll("[\\r\\n\\t]+", " ").trim();
        if (StringUtils.containsAnyIgnoreCase(compact, SENSITIVE_TOKENS)) {
            return REDACTED;
        }
        return compact.length() <= MAX_SAFE_TEXT_LENGTH
                ? compact
                : compact.substring(0, MAX_SAFE_TEXT_LENGTH);
    }

    private void save(AlarmKeyEnum key,
                      String agentId,
                      String taskId,
                      String taskName,
                      Map<String, Object> params) {
        if (alarmService == null) {
            return;
        }
        AlarmInfo alarm = AlarmInfo.builder()
                .status(AlarmStatusEnum.ING)
                .level(Level.WARNING)
                .component(AlarmComponentEnum.FE)
                .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM)
                .agentId(safeText(agentId))
                .taskId(safeText(taskId))
                .name(safeText(taskName))
                .node(safeText(taskName))
                .summary(key.name())
                .metric(key)
                .param(params)
                .build();
        try {
            alarmService.save(alarm);
        } catch (RuntimeException exception) {
            // Alarm delivery must not change DQL persistence or recovery control flow.
            LOGGER.warn("Failed to save DQL alarm {} for task {}", key, taskId, exception);
        }
    }
}

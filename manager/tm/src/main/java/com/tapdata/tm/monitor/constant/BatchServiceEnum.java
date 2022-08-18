package com.tapdata.tm.monitor.constant;

import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.monitor.dto.TaskLogDto;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogQueryParam;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum BatchServiceEnum {
    TASKCONSOLE(TaskDagCheckLogService.class.getName(), "getLogs", "/api/task-console", TaskLogDto.class.getName()),
    TASK_MONITORING_LOG(MonitoringLogsService.class.getName(), "query", "/api/MonitoringLogs/query", MonitoringLogQueryParam.class.getName()),
    MEASUREMENTQUERY(MeasurementServiceV2.class.getName(), "getSamples", "/api/measurement/query/v2", MeasurementQueryParam.class.getName());

    private final String service;
    private final String method;
    private final String uri;
    private final String param;

    public static BatchServiceEnum getEnumByServiceAndMethod(String uri) {
        for (BatchServiceEnum value : BatchServiceEnum.values()) {
            if (value.getUri().equals(uri)) {
                return value;
            }
        }
        return null;
    }
}


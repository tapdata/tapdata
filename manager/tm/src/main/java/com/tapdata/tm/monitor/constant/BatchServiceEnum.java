package com.tapdata.tm.monitor.constant;

import com.tapdata.tm.alarm.dto.AlarmListReqDto;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.autoinspect.service.TaskAutoInspectResultsService;
import com.tapdata.tm.monitor.dto.TaskLogDto;
import com.tapdata.tm.monitor.param.IdFilterPageParam;
import com.tapdata.tm.monitor.param.IdParam;
import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogCountParam;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogQueryParam;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.task.bean.TaskRecordDto;
import com.tapdata.tm.task.service.TaskConsoleService;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.service.TaskRecordService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.vo.RelationTaskRequest;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum BatchServiceEnum {
    TASKCONSOLE(TaskDagCheckLogService.class.getName(), "getLogs", "/api/task-console", TaskLogDto.class.getName()),
    TASK_MONITORING_LOG_QUERY(MonitoringLogsService.class.getName(), "query", "/api/MonitoringLogs/query", MonitoringLogQueryParam.class.getName()),
    TASK_MONITORING_LOG_COUNT(MonitoringLogsService.class.getName(), "count", "/api/MonitoringLogs/count", MonitoringLogCountParam.class.getName()),
    TASK_AUTO_INSPECT_RESULTS_TOTAL(TaskService.class.getName(), "totalAutoInspectResultsDiffTables", "/api/task/auto-inspect-totals", IdParam.class.getName()),
    TASK_AUTO_INSPECT_RESULTS_GROUP_BY_TABLE(TaskAutoInspectResultsService.class.getName(), "groupByTable", "/api/task/auto-inspect-results-group-by-table", IdFilterPageParam.class.getName()),
    MEASUREMENTQUERY(MeasurementServiceV2.class.getName(), "getSamples", "/api/measurement/query/v2", MeasurementQueryParam.class.getName()),
    TASK_ALARM_LIST(AlarmService.class.getName(), "listByTask", "/api/alarm/list_task", AlarmListReqDto.class.getName()),
    TASK_RELATION(TaskConsoleService.class.getName(), "getRelationTasks", "/api/task-console/relations", RelationTaskRequest.class.getName()),
    TASK_RECORD(TaskRecordService.class.getName(), "queryRecords", "/api/task/records", TaskRecordDto.class.getName())
    ;

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


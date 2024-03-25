package com.tapdata.tm.monitoringlogs.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogCountParam;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogExportParam;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogQueryParam;
import com.tapdata.tm.monitoringlogs.vo.MonitoringLogCountVo;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.task.entity.TaskDagCheckLog;

import java.util.Date;
import java.util.List;
import java.util.zip.ZipOutputStream;

public interface MonitoringLogsService {
    void batchSave(List<MonitoringLogsDto> monitoringLoges, UserDetail user);

    Page<MonitoringLogsDto> query(MonitoringLogQueryParam param);

    List<MonitoringLogCountVo> count(MonitoringLogCountParam param);

    List<MonitoringLogCountVo> count(String taskId, String taskRecordId);

    void export(MonitoringLogExportParam param, ZipOutputStream stream);

    void taskStateMachineLog(TaskDto taskDto, UserDetail user, DataFlowEvent event,
                             StateMachineResult stateMachineResult, long cost);

    void startTaskMonitoringLog(TaskDto taskDto, UserDetail user, Date date);

    void startTaskErrorLog(TaskDto taskDto, UserDetail user, Object e, Level level);

    void startTaskErrorStackTrace(TaskDto taskDto, UserDetail user, Throwable e, Level level);

    void agentAssignMonitoringLog(TaskDto taskDto, String assigned, Integer available, UserDetail user, Date now);

    void deleteLogs(String taskId);

    List<TaskDagCheckLog> getJsNodeLog(String testRunTaskId, String taskName, String nodeName);
}

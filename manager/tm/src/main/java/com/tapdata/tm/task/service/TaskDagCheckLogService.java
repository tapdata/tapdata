package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.monitor.dto.TaskLogDto;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;

import java.util.List;
import java.util.Locale;

public interface TaskDagCheckLogService {
    TaskDagCheckLog save(TaskDagCheckLog log);

    List<TaskDagCheckLog> saveAll(List<TaskDagCheckLog> logs);

    List<TaskDagCheckLog> dagCheck(TaskDto taskDto, UserDetail userDetail, boolean onlySave, Locale locale);

    TaskDagCheckLogVo getLogs(TaskLogDto dto, UserDetail userDetail, Locale locale);

    void removeAllByTaskId(String taskId);

    TaskDagCheckLog createLog(String taskId, String nodeId, String userId, Level grade, DagOutputTemplateEnum templateEnum, String template, Object ... param);
}

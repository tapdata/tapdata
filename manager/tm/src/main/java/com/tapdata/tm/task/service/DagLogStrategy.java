package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.entity.TaskDagCheckLog;

import java.util.List;
import java.util.Locale;

public interface DagLogStrategy {
    List<TaskDagCheckLog> getLogs(TaskDto taskDto, UserDetail userDetail, Locale locale);
}

package com.tapdata.tm.task.service;

import com.tapdata.tm.task.bean.TaskResetLogs;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

public interface TaskResetLogService {
    void save(TaskResetLogs taskResetLogs);

    List<TaskResetLogs> findByTaskId(String taskId);

    List<TaskResetLogs> find(Query query);

    void clearLogByTaskId(String taskId);



}

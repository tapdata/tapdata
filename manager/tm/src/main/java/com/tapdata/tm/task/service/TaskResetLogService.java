package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskResetEventDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.dto.TaskLogDto;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

public interface TaskResetLogService {
    TaskResetEventDto save(TaskResetEventDto taskResetEventDto, UserDetail userDetail);

    List<TaskResetEventDto> find(Query query);

    void clearLogByTaskId(String taskId);


    TaskDagCheckLogVo getLogs(TaskLogDto dto);
}

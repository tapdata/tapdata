package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.constant.HaveCheckLogException;

public interface TaskSaveService {
    void taskSaveCheckLog(TaskDto taskDto, UserDetail userDetail) throws HaveCheckLogException;
}

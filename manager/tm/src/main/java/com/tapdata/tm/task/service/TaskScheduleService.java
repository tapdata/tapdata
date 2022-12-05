package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;

public interface TaskScheduleService {

    void scheduling(TaskDto taskDto, UserDetail user);

}

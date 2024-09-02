package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;

public interface ILicenseService {
    public abstract boolean checkTaskPipelineLimit(TaskDto taskDto, UserDetail user);
}

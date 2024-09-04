package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import org.springframework.stereotype.Service;

@Service
public class DefaultLicenseService implements ILicenseService{
    @Override
    public boolean checkTaskPipelineLimit(TaskDto taskDto, UserDetail user) {
        return true;
    }
}

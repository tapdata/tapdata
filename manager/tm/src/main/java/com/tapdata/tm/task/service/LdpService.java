package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;

public interface LdpService {

    TaskDto createFdmTask(TaskDto task, UserDetail user);


    TaskDto createMdmTask(TaskDto task, UserDetail user);

    void createLdpMetaByTask(String taskId, UserDetail user);
}

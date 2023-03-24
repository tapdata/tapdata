package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;

import java.util.List;
import java.util.Map;

public interface LdpService {

    TaskDto createFdmTask(TaskDto task, UserDetail user);


    TaskDto createMdmTask(TaskDto task, String tagId, UserDetail user);

    void afterLdpTask(String taskId, UserDetail user);


    Map<String, TaskDto> queryFdmTaskByTags(List<String> tagIds, UserDetail user);
}

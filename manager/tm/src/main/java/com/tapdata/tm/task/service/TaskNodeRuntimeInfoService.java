package com.tapdata.tm.task.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.TaskNodeRuntimeInfoDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.entity.TaskNodeRuntimeInfoEntity;
import com.tapdata.tm.task.repository.TaskNodeRuntimeInfoRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2021/11/03
 * @Description:
 */
@Service
@Slf4j
public class TaskNodeRuntimeInfoService extends BaseService<TaskNodeRuntimeInfoDto, TaskNodeRuntimeInfoEntity, ObjectId, TaskNodeRuntimeInfoRepository> {
    public TaskNodeRuntimeInfoService(@NonNull TaskNodeRuntimeInfoRepository repository) {
        super(repository, TaskNodeRuntimeInfoDto.class, TaskNodeRuntimeInfoEntity.class);
    }

    protected void beforeSave(TaskNodeRuntimeInfoDto TaskRuntimeInfoDto, UserDetail user) {
    }
}

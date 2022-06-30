package com.tapdata.tm.task.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.TaskDatabaseRuntimeInfoDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.entity.TaskDatabaseRuntimeInfoEntity;
import com.tapdata.tm.task.repository.TaskDatabaseRuntimeInfoRepository;
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
public class TaskDatabaseRuntimeInfoService extends BaseService<TaskDatabaseRuntimeInfoDto, TaskDatabaseRuntimeInfoEntity, ObjectId, TaskDatabaseRuntimeInfoRepository> {
    public TaskDatabaseRuntimeInfoService(@NonNull TaskDatabaseRuntimeInfoRepository repository) {
        super(repository, TaskDatabaseRuntimeInfoDto.class, TaskDatabaseRuntimeInfoEntity.class);
    }

    protected void beforeSave(TaskDatabaseRuntimeInfoDto TaskRuntimeInfoDto, UserDetail user) {
    }
}

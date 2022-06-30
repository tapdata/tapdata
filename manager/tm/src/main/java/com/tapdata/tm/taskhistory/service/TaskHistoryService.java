package com.tapdata.tm.taskhistory.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.taskhistory.dto.TaskHistoryDto;
import com.tapdata.tm.taskhistory.entity.TaskHistoryEntity;
import com.tapdata.tm.taskhistory.repository.TaskHistoryRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2021/12/16
 * @Description:
 */
@Service
@Slf4j
public class TaskHistoryService extends BaseService<TaskHistoryDto, TaskHistoryEntity, ObjectId, TaskHistoryRepository> {
    public TaskHistoryService(@NonNull TaskHistoryRepository repository) {
        super(repository, TaskHistoryDto.class, TaskHistoryEntity.class);
    }

    protected void beforeSave(TaskHistoryDto taskHistory, UserDetail user) {

    }
}
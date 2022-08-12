package com.tapdata.tm.task.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.TaskRunHistoryDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.entity.TaskRunHistoryEntity;
import com.tapdata.tm.task.repository.TaskRunHistoryRepository;
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
public class TaskRunHistoryService extends BaseService<TaskRunHistoryDto, TaskRunHistoryEntity, ObjectId, TaskRunHistoryRepository> {
    public TaskRunHistoryService(@NonNull TaskRunHistoryRepository repository) {
        super(repository, TaskRunHistoryDto.class, TaskRunHistoryEntity.class);
    }

    @Override
    protected void beforeSave(TaskRunHistoryDto TaskRunHistory, UserDetail user) {
    }
}

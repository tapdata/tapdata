package com.tapdata.tm.task.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.TaskCollectionObjDto;
import com.tapdata.tm.commons.task.dto.TaskRunHistoryDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.entity.TaskCollectionObj;
import com.tapdata.tm.task.entity.TaskRunHistoryEntity;
import com.tapdata.tm.task.repository.TaskCollectionObjRepository;
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
public class TaskCollectionObjService extends BaseService<TaskCollectionObjDto, TaskCollectionObj, ObjectId, TaskCollectionObjRepository> {
    public TaskCollectionObjService(@NonNull TaskCollectionObjRepository repository) {
        super(repository, TaskCollectionObjDto.class, TaskCollectionObj.class);
    }

    @Override
    protected void beforeSave(TaskCollectionObjDto taskCollectionObjDto, UserDetail user) {
    }
}

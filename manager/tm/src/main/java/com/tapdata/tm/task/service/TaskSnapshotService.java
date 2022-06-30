package com.tapdata.tm.task.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.task.dto.TaskSnapshotsDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.entity.TaskSnapshotsEntity;
import com.tapdata.tm.task.repository.TaskSnapshotRepository;
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
public class TaskSnapshotService extends BaseService<TaskSnapshotsDto, TaskSnapshotsEntity, ObjectId, TaskSnapshotRepository> {
    public TaskSnapshotService(@NonNull TaskSnapshotRepository repository) {
        super(repository, TaskSnapshotsDto.class, TaskSnapshotsEntity.class);
    }

    protected void beforeSave(TaskSnapshotsDto taskSnapshotDto, UserDetail user) {
    }
}

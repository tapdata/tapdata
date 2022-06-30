package com.tapdata.tm.task.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.task.entity.TaskDatabaseRuntimeInfoEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/11/03
 * @Description:
 */
@Repository
public class TaskDatabaseRuntimeInfoRepository extends BaseRepository<TaskDatabaseRuntimeInfoEntity, ObjectId> {
    public TaskDatabaseRuntimeInfoRepository(MongoTemplate mongoOperations) {
        super(TaskDatabaseRuntimeInfoEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

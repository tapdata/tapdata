package com.tapdata.tm.task.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.task.entity.TaskNodeRuntimeInfoEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/11/03
 * @Description:
 */
@Repository
public class TaskNodeRuntimeInfoRepository extends BaseRepository<TaskNodeRuntimeInfoEntity, ObjectId> {
    public TaskNodeRuntimeInfoRepository(MongoTemplate mongoOperations) {
        super(TaskNodeRuntimeInfoEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

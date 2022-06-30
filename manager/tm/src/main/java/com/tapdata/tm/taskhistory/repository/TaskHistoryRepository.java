package com.tapdata.tm.taskhistory.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.taskhistory.entity.TaskHistoryEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/12/16
 * @Description:
 */
@Repository
public class TaskHistoryRepository extends BaseRepository<TaskHistoryEntity, ObjectId> {
    public TaskHistoryRepository(MongoTemplate mongoOperations) {
        super(TaskHistoryEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

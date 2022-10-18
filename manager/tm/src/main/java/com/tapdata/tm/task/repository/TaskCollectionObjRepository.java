package com.tapdata.tm.task.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.task.entity.TaskCollectionObj;
import com.tapdata.tm.task.entity.TaskRunHistoryEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/11/03
 * @Description:
 */
@Repository
public class TaskCollectionObjRepository extends BaseRepository<TaskCollectionObj, ObjectId> {
    public TaskCollectionObjRepository(MongoTemplate mongoOperations) {
        super(TaskCollectionObj.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

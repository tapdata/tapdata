package com.tapdata.tm.task.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.task.entity.TaskEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

/**
 * @Author:
 * @Date: 2021/11/03
 * @Description:
 */
@Repository
public class TaskRepository extends BaseRepository<TaskEntity, ObjectId> {
    public TaskRepository(MongoTemplate mongoOperations) {
        super(TaskEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

    public TaskEntity importEntity(TaskEntity entity, UserDetail userDetail) {
        Assert.notNull(entity, "Entity must not be null!");

        applyUserDetail(entity, userDetail);
        beforeCreateEntity(entity, userDetail);
        return mongoOperations.insert(entity, entityInformation.getCollectionName());
    }
}

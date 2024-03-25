package com.tapdata.tm.inspect.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.inspect.entity.TimerLockEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Repository
public class TimerRepository extends BaseRepository<TimerLockEntity, ObjectId> {
    public TimerRepository(MongoTemplate mongoOperations) {
        super(TimerLockEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

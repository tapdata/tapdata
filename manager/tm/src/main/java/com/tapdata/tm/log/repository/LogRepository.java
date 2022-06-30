package com.tapdata.tm.log.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.log.entity.LogEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Repository
public class LogRepository extends BaseRepository<LogEntity, ObjectId> {
    public LogRepository(MongoTemplate mongoOperations) {
        super(LogEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

package com.tapdata.tm.inspect.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.inspect.entity.InspectResultEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Repository
public class InspectResultRepository extends BaseRepository<InspectResultEntity, ObjectId> {
    public InspectResultRepository(MongoTemplate mongoOperations) {
        super(InspectResultEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

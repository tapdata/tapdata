package com.tapdata.tm.inspect.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.inspect.entity.InspectEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Repository
public class InspectRepository extends BaseRepository<InspectEntity, ObjectId> {
    public InspectRepository(MongoTemplate mongoOperations) {
        super(InspectEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

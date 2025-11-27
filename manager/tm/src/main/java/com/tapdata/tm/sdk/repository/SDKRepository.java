package com.tapdata.tm.sdk.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.sdk.entity.SDKEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2025/07/01
 * @Description:
 */
@Repository
public class SDKRepository extends BaseRepository<SDKEntity, ObjectId> {
    public SDKRepository(MongoTemplate mongoOperations) {
        super(SDKEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

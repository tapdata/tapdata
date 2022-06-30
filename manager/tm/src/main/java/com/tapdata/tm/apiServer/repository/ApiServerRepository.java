package com.tapdata.tm.apiServer.repository;

import com.tapdata.tm.apiServer.entity.ApiServerEntity;
import com.tapdata.tm.base.reporitory.BaseRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Repository
public class ApiServerRepository extends BaseRepository<ApiServerEntity, ObjectId> {
    public ApiServerRepository(MongoTemplate mongoOperations) {
        super(ApiServerEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

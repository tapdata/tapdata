package com.tapdata.tm.apicallstats.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.apicallstats.entity.ApiCallStatsEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2024/08/26
 * @Description:
 */
@Repository
public class ApiCallStatsRepository extends BaseRepository<ApiCallStatsEntity, ObjectId> {
    public ApiCallStatsRepository(MongoTemplate mongoOperations) {
        super(ApiCallStatsEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

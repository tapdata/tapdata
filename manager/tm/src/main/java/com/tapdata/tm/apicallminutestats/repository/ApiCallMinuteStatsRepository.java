package com.tapdata.tm.apicallminutestats.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.apicallminutestats.entity.ApiCallMinuteStatsEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2024/08/29
 * @Description:
 */
@Repository
public class ApiCallMinuteStatsRepository extends BaseRepository<ApiCallMinuteStatsEntity, ObjectId> {
    public ApiCallMinuteStatsRepository(MongoTemplate mongoOperations) {
        super(ApiCallMinuteStatsEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

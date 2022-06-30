package com.tapdata.tm.insights.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.insights.entity.InsightsEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/10/14
 * @Description:
 */
@Repository
public class InsightsRepository extends BaseRepository<InsightsEntity, ObjectId> {
    public InsightsRepository(MongoTemplate mongoOperations) {
        super(InsightsEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

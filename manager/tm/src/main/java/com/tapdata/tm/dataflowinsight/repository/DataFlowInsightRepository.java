package com.tapdata.tm.dataflowinsight.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.dataflowinsight.entity.DataFlowInsightEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Repository
public class DataFlowInsightRepository extends BaseRepository<DataFlowInsightEntity, ObjectId> {
    public DataFlowInsightRepository(MongoTemplate mongoOperations) {
        super(DataFlowInsightEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

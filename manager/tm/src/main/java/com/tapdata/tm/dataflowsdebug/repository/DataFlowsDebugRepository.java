package com.tapdata.tm.dataflowsdebug.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.dataflowsdebug.entity.DataFlowsDebugEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Repository
public class DataFlowsDebugRepository extends BaseRepository<DataFlowsDebugEntity, ObjectId> {
    public DataFlowsDebugRepository(MongoTemplate mongoOperations) {
        super(DataFlowsDebugEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

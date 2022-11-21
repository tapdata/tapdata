package com.tapdata.tm.dataflowrecord.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.dataflowrecord.entity.DataFlowRecordEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2022/03/04
 * @Description:
 */
@Repository
public class DataFlowRecordRepository extends BaseRepository<DataFlowRecordEntity, ObjectId> {
    public DataFlowRecordRepository(MongoTemplate mongoOperations) {
        super(DataFlowRecordEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

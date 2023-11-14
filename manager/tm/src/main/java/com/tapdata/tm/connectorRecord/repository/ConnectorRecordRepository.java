package com.tapdata.tm.connectorRecord.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.connectorRecord.entity.ConnectorRecordEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ConnectorRecordRepository extends BaseRepository<ConnectorRecordEntity, ObjectId> {
    public ConnectorRecordRepository(MongoTemplate mongoOperations) {
        super(ConnectorRecordEntity.class, mongoOperations);
    }
    @Override
    protected void init() {
        super.init();
    }
}

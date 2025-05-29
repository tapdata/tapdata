package com.tapdata.tm.cluster.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.cluster.entity.RawServerStateEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class RawServerStateRepository extends BaseRepository<RawServerStateEntity, ObjectId> {
    public RawServerStateRepository(MongoTemplate mongoOperations) {
        super(RawServerStateEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

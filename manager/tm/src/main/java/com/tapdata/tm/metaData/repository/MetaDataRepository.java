package com.tapdata.tm.metaData.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.inspect.entity.InspectEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;

public class MetaDataRepository extends BaseRepository<InspectEntity, ObjectId> {
    public MetaDataRepository(MongoTemplate mongoOperations) {
        super(InspectEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}
package com.tapdata.tm.livedataplatform.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.livedataplatform.entity.LiveDataPlatformEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class LiveDataPlatformRepository extends BaseRepository<LiveDataPlatformEntity, ObjectId> {
    public LiveDataPlatformRepository(MongoTemplate mongoOperations) {
        super(LiveDataPlatformEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

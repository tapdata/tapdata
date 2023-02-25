package com.tapdata.tm.hazelcastPersistence.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.hazelcastPersistence.entity.HazelcastPersistenceEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2022/10/18
 * @Description:
 */
@Repository
public class HazelcastPersistenceRepository extends BaseRepository<HazelcastPersistenceEntity, ObjectId> {
    public HazelcastPersistenceRepository(MongoTemplate mongoOperations) {
        super(HazelcastPersistenceEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

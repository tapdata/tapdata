package com.tapdata.tm.roleMapping.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.roleMapping.entity.RoleMappingEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class RoleMappingRepository extends BaseRepository<RoleMappingEntity, ObjectId> {
    public RoleMappingRepository(MongoTemplate mongoOperations) {
        super(RoleMappingEntity.class, mongoOperations);
    }
}

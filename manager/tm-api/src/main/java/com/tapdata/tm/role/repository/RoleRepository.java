package com.tapdata.tm.role.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.role.entity.RoleEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class RoleRepository extends BaseRepository<RoleEntity, ObjectId> {

    public RoleRepository(MongoTemplate mongoOperations) {
        super(RoleEntity.class, mongoOperations);
    }
}

package com.tapdata.tm.typemappings.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.typemappings.entity.TypeMappingsEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Repository
public class TypeMappingsRepository extends BaseRepository<TypeMappingsEntity, ObjectId> {
    public TypeMappingsRepository(MongoTemplate mongoOperations) {
        super(TypeMappingsEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

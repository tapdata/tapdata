package com.tapdata.tm.modules.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.modules.entity.ModulesEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/10/14
 * @Description:
 */
@Repository
public class ModulesRepository extends BaseRepository<ModulesEntity, ObjectId> {
    public ModulesRepository(MongoTemplate mongoOperations) {
        super(ModulesEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

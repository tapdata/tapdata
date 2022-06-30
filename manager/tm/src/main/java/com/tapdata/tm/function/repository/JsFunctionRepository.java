package com.tapdata.tm.function.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.function.entity.JsFunctionEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2022/04/07
 * @Description:
 */
@Repository
public class JsFunctionRepository extends BaseRepository<JsFunctionEntity, ObjectId> {
    public JsFunctionRepository(MongoTemplate mongoOperations) {
        super(JsFunctionEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

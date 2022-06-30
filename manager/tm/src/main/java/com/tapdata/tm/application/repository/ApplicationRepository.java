package com.tapdata.tm.application.repository;

import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.base.reporitory.BaseRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Repository
public class ApplicationRepository extends BaseRepository<ApplicationEntity, ObjectId> {
    public ApplicationRepository(MongoTemplate mongoOperations) {
        super(ApplicationEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

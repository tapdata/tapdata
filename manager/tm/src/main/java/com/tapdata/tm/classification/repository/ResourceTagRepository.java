package com.tapdata.tm.classification.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.classification.entity.ClassificationEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author: Zed
 * @Date: 2021/8/24
 * @Description:
 */
@Repository
public class ResourceTagRepository extends BaseRepository<ClassificationEntity, ObjectId> {
    public ResourceTagRepository(MongoTemplate mongoOperations) {
        super(ClassificationEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

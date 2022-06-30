package com.tapdata.tm.customNode.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.customNode.entity.CustomNodeEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2022/03/09
 * @Description:
 */
@Repository
public class CustomNodeRepository extends BaseRepository<CustomNodeEntity, ObjectId> {
    public CustomNodeRepository(MongoTemplate mongoOperations) {
        super(CustomNodeEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

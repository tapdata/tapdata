package com.tapdata.tm.customNode.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.customNode.entity.CustomNodeEntity;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

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

    public CustomNodeEntity importEntity(CustomNodeEntity entity, UserDetail userDetail) {
        Assert.notNull(entity, "Entity must not be null!");

        applyUserDetail(entity, userDetail);
        beforeCreateEntity(entity, userDetail);
        return mongoOperations.insert(entity, entityInformation.getCollectionName());
    }
}

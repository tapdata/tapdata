package com.tapdata.tm.metadatainstance.repository;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

/**
 * @Author:
 * @Date: 2021/09/11
 * @Description:
 */
@Repository
public class MetadataInstancesRepository extends BaseRepository<MetadataInstancesEntity, ObjectId> {
    public MetadataInstancesRepository(MongoTemplate mongoOperations) {
        super(MetadataInstancesEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

    public MetadataInstancesEntity importEntity(MetadataInstancesEntity entity, UserDetail userDetail) {
        Assert.notNull(entity, "Entity must not be null!");

        applyUserDetail(entity, userDetail);
        beforeCreateEntity(entity, userDetail);
        return mongoOperations.insert(entity, entityInformation.getCollectionName());
    }
}

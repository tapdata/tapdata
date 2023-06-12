package com.tapdata.tm.metadatainstance.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

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
}

package com.tapdata.tm.sdkModule.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.sdkModule.entity.SdkModuleEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2025/07/02
 * @Description:
 */
@Repository
public class SdkModuleRepository extends BaseRepository<SdkModuleEntity, ObjectId> {
    public SdkModuleRepository(MongoTemplate mongoOperations) {
        super(SdkModuleEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

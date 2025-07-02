package com.tapdata.tm.sdkVersion.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.sdkVersion.entity.SdkVersionEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2025/07/02
 * @Description:
 */
@Repository
public class SdkVersionRepository extends BaseRepository<SdkVersionEntity, ObjectId> {
    public SdkVersionRepository(MongoTemplate mongoOperations) {
        super(SdkVersionEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

package com.tapdata.tm.license.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.license.entity.LicenseEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/12/06
 * @Description:
 */
@Repository
public class LicenseRepository extends BaseRepository<LicenseEntity, ObjectId> {
    public LicenseRepository(MongoTemplate mongoOperations) {
        super(LicenseEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

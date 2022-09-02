package com.tapdata.tm.verison.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.verison.entity.VersionEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2022/08/15
 * @Description:
 */
@Repository
public class VersionRepository extends BaseRepository<VersionEntity, ObjectId> {
    public VersionRepository(MongoTemplate mongoOperations) {
        super(VersionEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

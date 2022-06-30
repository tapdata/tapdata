package com.tapdata.tm.ds.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.ds.entity.PdkSourceEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author: Zed
 * @Date: 2021/8/24
 * @Description:
 */
@Repository
public class PdkSourceRepository extends BaseRepository<PdkSourceEntity, ObjectId> {
    public PdkSourceRepository(MongoTemplate mongoOperations) {
        super(PdkSourceEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

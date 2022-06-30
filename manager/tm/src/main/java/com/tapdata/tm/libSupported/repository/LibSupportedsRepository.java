package com.tapdata.tm.libSupported.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.libSupported.entity.LibSupportedsEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/12/21
 * @Description:
 */
@Repository
public class LibSupportedsRepository extends BaseRepository<LibSupportedsEntity, ObjectId> {
    public LibSupportedsRepository(MongoTemplate mongoOperations) {
        super(LibSupportedsEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

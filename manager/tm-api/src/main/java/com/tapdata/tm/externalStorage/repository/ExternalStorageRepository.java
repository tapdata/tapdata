package com.tapdata.tm.externalStorage.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.externalStorage.entity.ExternalStorageEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2022/09/07
 * @Description:
 */
@Repository
public class ExternalStorageRepository extends BaseRepository<ExternalStorageEntity, ObjectId> {
    public ExternalStorageRepository(MongoTemplate mongoOperations) {
        super(ExternalStorageEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

package com.tapdata.tm.deleteCaches.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.deleteCaches.entity.DeleteCachesEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2022/01/25
 * @Description:
 */
@Repository
public class DeleteCachesRepository extends BaseRepository<DeleteCachesEntity, ObjectId> {
    public DeleteCachesRepository(MongoTemplate mongoOperations) {
        super(DeleteCachesEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

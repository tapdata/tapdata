package com.tapdata.tm.databasetags.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.databasetags.entity.DatabaseTagsEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2022/03/15
 * @Description:
 */
@Repository
public class DatabaseTagsRepository extends BaseRepository<DatabaseTagsEntity, ObjectId> {
    public DatabaseTagsRepository(MongoTemplate mongoOperations) {
        super(DatabaseTagsEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

package com.tapdata.tm.datacategory.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.datacategory.entity.DataCategoryEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/10/18
 * @Description:
 */
@Repository
public class DataCategoryRepository extends BaseRepository<DataCategoryEntity, ObjectId> {
    public DataCategoryRepository(MongoTemplate mongoOperations) {
        super(DataCategoryEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

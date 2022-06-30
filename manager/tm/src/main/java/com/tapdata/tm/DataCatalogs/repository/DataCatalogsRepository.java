package com.tapdata.tm.DataCatalogs.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.DataCatalogs.entity.DataCatalogsEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2022/01/24
 * @Description:
 */
@Repository
public class DataCatalogsRepository extends BaseRepository<DataCatalogsEntity, ObjectId> {
    public DataCatalogsRepository(MongoTemplate mongoOperations) {
        super(DataCatalogsEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

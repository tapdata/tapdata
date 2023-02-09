package com.tapdata.tm.ds.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

/**
 * @Author: Zed
 * @Date: 2021/8/24
 * @Description:
 */
@Repository
public class DataSourceRepository extends BaseRepository<DataSourceEntity, ObjectId> {
    public DataSourceRepository(MongoTemplate mongoOperations) {
        super(DataSourceEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }

    public DataSourceEntity importEntity(DataSourceEntity entity, UserDetail userDetail) {
        Assert.notNull(entity, "Entity must not be null!");

        applyUserDetail(entity, userDetail);
        beforeCreateEntity(entity, userDetail);
        return mongoOperations.insert(entity, entityInformation.getCollectionName());
    }
}

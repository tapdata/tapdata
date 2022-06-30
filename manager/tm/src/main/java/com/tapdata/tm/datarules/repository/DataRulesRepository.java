package com.tapdata.tm.datarules.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.datarules.entity.DataRulesEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2021/10/19
 * @Description:
 */
@Repository
public class DataRulesRepository extends BaseRepository<DataRulesEntity, ObjectId> {
    public DataRulesRepository(MongoTemplate mongoOperations) {
        super(DataRulesEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

package com.tapdata.tm.function.inspect.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.function.inspect.entity.InspectFunctionEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;


/**
 * @Author: Gavin
 * @Date: 2023/03/27
 * @Description:
 */
@Repository
public class InspectFunctionRepository extends BaseRepository<InspectFunctionEntity, ObjectId> {
    public InspectFunctionRepository(MongoTemplate mongoOperations) {
        super(InspectFunctionEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

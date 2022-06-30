package com.tapdata.tm.javascript.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.inspect.entity.InspectEntity;
import com.tapdata.tm.javascript.entity.FunctionsEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class FunctionRepository extends BaseRepository<FunctionsEntity, ObjectId>  {
    public FunctionRepository(MongoTemplate mongoOperations) {
        super(FunctionsEntity.class, mongoOperations);
    }
}
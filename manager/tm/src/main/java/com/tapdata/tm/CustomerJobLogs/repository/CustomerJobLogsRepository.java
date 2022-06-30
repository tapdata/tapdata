package com.tapdata.tm.CustomerJobLogs.repository;

import com.tapdata.tm.CustomerJobLogs.entity.CustomerJobLogsEntity;
import com.tapdata.tm.base.reporitory.BaseRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/*
 * @Author: Steven
 * @Date: 2021/12/11
 * @Description:
 */
@Repository
public class CustomerJobLogsRepository extends BaseRepository<CustomerJobLogsEntity, ObjectId> {

    public CustomerJobLogsRepository(MongoTemplate mongoOperations) {
        super(CustomerJobLogsEntity.class, mongoOperations);
    }

}

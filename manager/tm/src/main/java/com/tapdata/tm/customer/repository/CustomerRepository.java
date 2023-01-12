package com.tapdata.tm.customer.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.customer.entity.CustomerEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @Author:
 * @Date: 2022/02/14
 * @Description:
 */
@Repository
public class CustomerRepository extends BaseRepository<CustomerEntity, ObjectId> {
    public CustomerRepository(MongoTemplate mongoOperations) {
        super(CustomerEntity.class, mongoOperations);
    }

    @Override
    protected void init() {
        super.init();
    }
}

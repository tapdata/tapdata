package com.tapdata.tm.v2.api.pool.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.worker.entity.ConnectionPoolEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/4/29 14:47 Create
 * @description
 */
@Repository
public class ConnectionPoolInfoRepository extends BaseRepository<ConnectionPoolEntity, ObjectId> {

    public ConnectionPoolInfoRepository(MongoTemplate mongoOperations) {
        super(ConnectionPoolEntity.class, mongoOperations);
    }
}
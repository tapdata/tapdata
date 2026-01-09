package com.tapdata.tm.v2.api.usage.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/1/7 11:48 Create
 * @description
 */
@Repository
public class ServerUsageMetricRepository extends BaseRepository<ServerUsageMetric, ObjectId> {
    public ServerUsageMetricRepository(MongoTemplate mongoOperations) {
        super(ServerUsageMetric.class, mongoOperations);
    }
}

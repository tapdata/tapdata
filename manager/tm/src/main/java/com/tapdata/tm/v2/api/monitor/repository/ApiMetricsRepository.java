package com.tapdata.tm.v2.api.monitor.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 10:09 Create
 * @description
 */
@Repository
public class ApiMetricsRepository extends BaseRepository<ApiMetricsRaw, ObjectId> {
    public ApiMetricsRepository(MongoTemplate mongoOperations) {
        super(ApiMetricsRaw.class, mongoOperations);
    }
}

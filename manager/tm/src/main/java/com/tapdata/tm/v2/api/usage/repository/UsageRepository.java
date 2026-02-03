package com.tapdata.tm.v2.api.usage.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.worker.entity.ServerUsage;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 16:18 Create
 * @description
 */
@Repository
public class UsageRepository extends BaseRepository<ServerUsage, ObjectId> {
    public UsageRepository(MongoTemplate mongoOperations) {
        super(ServerUsage.class, mongoOperations);
    }
}

package com.tapdata.tm.monitoringlogs.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.monitoringlogs.entity.MonitoringLogsEntity;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @Author:
 * @Date: 2022/06/20
 * @Description:
 */
@Repository
public class MonitoringLogsRepository extends BaseRepository<MonitoringLogsEntity, ObjectId> {
    public MonitoringLogsRepository(@Qualifier(value = "logMongoTemplate") CompletableFuture<MongoTemplate> mongoOperations) throws ExecutionException, InterruptedException {
        super(MonitoringLogsEntity.class, mongoOperations.get());
    }

    @Override
    protected void init() {
        super.init();
    }
}

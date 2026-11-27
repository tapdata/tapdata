package com.tapdata.tm.throttle.service;

import com.tapdata.tm.throttle.entity.TaskThrottlePolicyEntity;
import com.tapdata.tm.throttle.entity.TaskThrottleTelemetryEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Ensures MongoDB indexes for throttle telemetry retention (TTL) and hot-path queries.
 */
@Component
@Slf4j
public class TaskThrottleIndexInitializer implements ApplicationListener<ApplicationReadyEvent> {

    static final String IDX_TELEMETRY_EXPIRE_AT_TTL = "idx_throttle_telemetry_expire_at_ttl";
    static final String IDX_TELEMETRY_TASK_PHASE_DIR_END = "idx_throttle_telemetry_task_phase_dir_end";
    static final String IDX_POLICY_TASK_ID = "idx_throttle_policy_task_id";

    private final MongoOperations mongo;

    @Autowired
    public TaskThrottleIndexInitializer(MongoOperations mongo) {
        this.mongo = mongo;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        ensureIndexes();
    }

    public void ensureIndexes() {
        if (mongo == null) {
            return;
        }
        ensureSingleIndex(
                TaskThrottleTelemetryEntity.class,
                IDX_TELEMETRY_EXPIRE_AT_TTL,
                new Index().on("expireAt", Sort.Direction.ASC)
                        .named(IDX_TELEMETRY_EXPIRE_AT_TTL)
                        .expire(TaskThrottleTelemetryEntity.TELEMETRY_TTL_SECONDS, TimeUnit.SECONDS)
                        .background());
        ensureSingleIndex(
                TaskThrottleTelemetryEntity.class,
                IDX_TELEMETRY_TASK_PHASE_DIR_END,
                new Index().on("taskId", Sort.Direction.ASC)
                        .on("phase", Sort.Direction.ASC)
                        .on("direction", Sort.Direction.ASC)
                        .on("windowEndUtc", Sort.Direction.DESC)
                        .named(IDX_TELEMETRY_TASK_PHASE_DIR_END)
                        .background());
        ensureSingleIndex(
                TaskThrottlePolicyEntity.class,
                IDX_POLICY_TASK_ID,
                new Index().on("taskId", Sort.Direction.ASC)
                        .named(IDX_POLICY_TASK_ID)
                        .background());
    }

    private void ensureSingleIndex(Class<?> entityClass, String indexName, IndexDefinition index) {
        try {
            IndexOperations indexOps = mongo.indexOps(entityClass);
            if (indexOps == null) {
                return;
            }
            indexOps.ensureIndex(index);
        } catch (RuntimeException ex) {
            log.warn("Unable to ensure throttle index {} on {}: {}",
                    indexName, entityClass.getSimpleName(), ex.getMessage());
        }
    }
}

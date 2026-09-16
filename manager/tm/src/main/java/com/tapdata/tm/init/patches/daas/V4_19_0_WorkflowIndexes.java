package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Create Workflow MongoDB indexes for DAAS (aligned with WorkflowMongoIndexes).
 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.19-0")
public class V4_19_0_WorkflowIndexes extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(V4_19_0_WorkflowIndexes.class);

    public static final String DEFINITION = "WorkflowDefinition";
    public static final String VERSION = "WorkflowVersion";
    public static final String RUN = "WorkflowRun";
    public static final String STEP_RUN = "WorkflowStepRun";
    public static final String TRIGGER_RECORD = "WorkflowTriggerRecord";
    public static final String TRIGGER_STATE = "WorkflowTriggerState";
    public static final String TASK_OPERATION = "WorkflowTaskOperation";

    public V4_19_0_WorkflowIndexes(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);
        if (mongoTemplate == null) {
            logger.error("MongoTemplate bean not found, patch execution failed");
            return;
        }
        ensureDefinition(mongoTemplate);
        ensureVersion(mongoTemplate);
        ensureRun(mongoTemplate);
        ensureStepRun(mongoTemplate);
        ensureTriggerRecord(mongoTemplate);
        ensureTriggerState(mongoTemplate);
        ensureTaskOperation(mongoTemplate);
    }

    private void ensureDefinition(MongoTemplate mongoTemplate) {
        createIndexIfNeed(mongoTemplate, DEFINITION,
                new Document("deleted", 1).append("enabled", 1).append("nextFireAt", 1),
                new IndexOptions().name("deleted_1_enabled_1_nextFireAt_1").background(true));
        createIndexIfNeed(mongoTemplate, DEFINITION,
                new Document("name", 1).append("deleted", 1),
                new IndexOptions().name("name_1_deleted_1").background(true));
    }

    private void ensureVersion(MongoTemplate mongoTemplate) {
        createIndexIfNeed(mongoTemplate, VERSION,
                new Document("workflowId", 1).append("version", 1),
                new IndexOptions().name("workflowId_1_version_1").unique(true).background(true));
    }

    private void ensureRun(MongoTemplate mongoTemplate) {
        createIndexIfNeed(mongoTemplate, RUN,
                new Document("workflowId", 1).append("active", 1),
                new IndexOptions().name("workflowId_1_active_unique")
                        .unique(true)
                        .partialFilterExpression(new Document("active", true))
                        .background(true));
        createIndexIfNeed(mongoTemplate, RUN,
                new Document("active", 1).append("nextWakeAt", 1).append("leaseUntil", 1),
                new IndexOptions().name("active_1_nextWakeAt_1_leaseUntil_1").background(true));
        createIndexIfNeed(mongoTemplate, RUN,
                new Document("workflowId", 1).append("startedAt", -1),
                new IndexOptions().name("workflowId_1_startedAt_-1").background(true));
        createIndexIfNeed(mongoTemplate, RUN,
                new Document("status", 1).append("startedAt", -1),
                new IndexOptions().name("status_1_startedAt_-1").background(true));
    }

    private void ensureStepRun(MongoTemplate mongoTemplate) {
        createIndexIfNeed(mongoTemplate, STEP_RUN,
                new Document("runId", 1).append("stepId", 1).append("attempt", 1),
                new IndexOptions().name("runId_1_stepId_1_attempt_1").unique(true).background(true));
    }

    private void ensureTriggerRecord(MongoTemplate mongoTemplate) {
        createIndexIfNeed(mongoTemplate, TRIGGER_RECORD,
                new Document("dedupeKey", 1),
                new IndexOptions().name("dedupeKey_1").unique(true).background(true));
    }

    private void ensureTriggerState(MongoTemplate mongoTemplate) {
        createIndexIfNeed(mongoTemplate, TRIGGER_STATE,
                new Document("stateKey", 1),
                new IndexOptions().name("stateKey_1").unique(true).background(true));
        createIndexIfNeed(mongoTemplate, TRIGGER_STATE,
                new Document("workflowId", 1).append("taskId", 1),
                new IndexOptions().name("workflowId_1_taskId_1").background(true));
    }

    private void ensureTaskOperation(MongoTemplate mongoTemplate) {
        createIndexIfNeed(mongoTemplate, TASK_OPERATION,
                new Document("expiresAt", 1),
                new IndexOptions().name("expiresAt_ttl").expireAfter(0L, TimeUnit.SECONDS).background(true));
        createIndexIfNeed(mongoTemplate, TASK_OPERATION,
                new Document("taskId", 1).append("consumedAt", 1),
                new IndexOptions().name("taskId_1_consumedAt_1").background(true));
    }

    private void createIndexIfNeed(MongoTemplate mongoTemplate, String collectionName, Document keys, IndexOptions options) {
        String indexName = options.getName();
        List<Document> indexList = mongoTemplate.getCollection(collectionName)
                .listIndexes()
                .into(new ArrayList<>());
        boolean indexExists = indexList.stream()
                .anyMatch(doc -> indexName.equals(doc.getString("name")));
        if (indexExists) {
            logger.info("Index {} already exists in collection {}, skipping creation", indexName, collectionName);
            return;
        }
        try {
            mongoTemplate.getCollection(collectionName).createIndex(keys, options);
            logger.info("Created index {} on collection {}", indexName, collectionName);
        } catch (Exception e) {
            logger.warn("Skip creating index {} on {}: {}", indexName, collectionName, e.getMessage());
        }
    }
}

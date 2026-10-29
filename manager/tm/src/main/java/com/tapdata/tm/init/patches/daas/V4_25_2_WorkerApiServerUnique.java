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
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Removes active API Server worker duplicates before enforcing the active worker key.
 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.25-2")
public class V4_25_2_WorkerApiServerUnique extends AbsPatch {
    static final String COLLECTION_NAME = "Workers";
    static final String WORKER_TYPE = "api-server";
    static final String INDEX_NAME = "unq_worker_api_server_active_process";
    private static final Logger logger = LogManager.getLogger(V4_25_2_WorkerApiServerUnique.class);

    public V4_25_2_WorkerApiServerUnique(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);

        Query activeWorkers = activeWorkersQuery();
        // Normalize the legacy missing/null flag so the partial unique index covers every
        // active API Server record while still allowing delete=true history to remain.
        mongoTemplate.updateMulti(activeWorkers, Update.update("delete", false), COLLECTION_NAME);

        List<Document> workers = mongoTemplate.getCollection(COLLECTION_NAME)
                .find(activeWorkers.getQueryObject())
                .into(new ArrayList<>());
        Map<String, List<Document>> workersByProcess = new HashMap<>();
        workers.forEach(worker -> workersByProcess
                .computeIfAbsent(worker.getString("process_id"), key -> new ArrayList<>())
                .add(worker));

        workersByProcess.values().stream()
                .filter(group -> group.size() > 1)
                .forEach(group -> removeDuplicates(mongoTemplate, group));

        IndexOptions options = new IndexOptions()
                .unique(true)
                .name(INDEX_NAME)
                .partialFilterExpression(new Document("worker_type", WORKER_TYPE)
                        .append("delete", false)
                        .append("process_id", new Document("$gt", "")));
        boolean indexExists = mongoTemplate.getCollection(COLLECTION_NAME).listIndexes()
                .into(new ArrayList<>()).stream()
                .anyMatch(index -> INDEX_NAME.equals(index.getString("name")));
        if (!indexExists) {
            mongoTemplate.getCollection(COLLECTION_NAME)
                    .createIndex(new Document("process_id", 1).append("worker_type", 1), options);
        }
    }

    static Query activeWorkersQuery() {
        return Query.query(Criteria.where("worker_type").is(WORKER_TYPE)
                .and("delete").ne(true)
                .and("process_id").gt(""));
    }

    static Document selectCanonical(List<Document> workers) {
        return workers.stream()
                .max(Comparator.comparing(V4_25_2_WorkerApiServerUnique::hasWorkerStatus)
                        .thenComparingLong(V4_25_2_WorkerApiServerUnique::pingTime)
                        .thenComparingLong(V4_25_2_WorkerApiServerUnique::lastUpdated)
                        .thenComparing(V4_25_2_WorkerApiServerUnique::objectId,
                                Comparator.nullsFirst(Comparator.naturalOrder())))
                .orElse(null);
    }

    private static void removeDuplicates(MongoTemplate mongoTemplate, List<Document> workers) {
        Document canonical = selectCanonical(workers);
        workers.stream()
                .filter(worker -> !worker.getObjectId("_id").equals(canonical.getObjectId("_id")))
                .forEach(worker -> mongoTemplate.remove(
                        Query.query(Criteria.where("_id").is(worker.getObjectId("_id"))), COLLECTION_NAME));
    }

    private static boolean hasWorkerStatus(Document worker) {
        Object status = worker.get("worker_status");
        return status instanceof Map && !((Map<?, ?>) status).isEmpty()
                || status instanceof Document && !((Document) status).isEmpty();
    }

    private static long pingTime(Document worker) {
        return numberValue(worker.get("ping_time"));
    }

    private static long lastUpdated(Document worker) {
        Object value = worker.get("last_updated");
        if (value instanceof java.util.Date) {
            return ((java.util.Date) value).getTime();
        }
        return numberValue(value);
    }

    private static long numberValue(Object value) {
        return value instanceof Number ? ((Number) value).longValue() : 0L;
    }

    private static org.bson.types.ObjectId objectId(Document worker) {
        return worker.getObjectId("_id");
    }
}

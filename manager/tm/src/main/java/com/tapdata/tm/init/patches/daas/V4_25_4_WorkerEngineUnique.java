package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.worker.entity.field.WorkerField;
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
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Removes active engine (connector / transformer) worker duplicates and enforces one active
 * record per process, mirroring {@link V4_25_2_WorkerApiServerUnique} for API Server workers.
 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.25-4")
public class V4_25_4_WorkerEngineUnique extends AbsPatch {
    static final String COLLECTION_NAME = "Workers";
    static final String WORKER_TYPE_FIELD = WorkerField.WORKER_TYPE.field();
    static final String PROCESS_ID_FIELD = WorkerField.PROCESS_ID.field();
    static final String DELETED_FIELD = WorkerField.DELETED.field();
    static final String CONNECTOR = "connector";
    static final String TRANSFORMER = "transformer";
    static final String CONNECTOR_INDEX_NAME = "unq_worker_connector_active_process";
    static final String TRANSFORMER_INDEX_NAME = "unq_worker_transformer_active_process";
    private static final Logger logger = LogManager.getLogger(V4_25_4_WorkerEngineUnique.class);

    public V4_25_4_WorkerEngineUnique(PatchType type, PatchVersion version) {
        super(type, version);
    }

    /**
     * Each type gets its own key pattern: Workers already holds {process_id, worker_type},
     * {worker_type, process_id} and {process_id, worker_type, deleted}, and MongoDB before 5.0
     * rejects a second index with the same key pattern but different options.
     */
    static Map<String, Document> indexKeys() {
        Map<String, Document> keys = new LinkedHashMap<>();
        keys.put(CONNECTOR, new Document(WORKER_TYPE_FIELD, 1).append(PROCESS_ID_FIELD, 1).append(DELETED_FIELD, 1));
        keys.put(TRANSFORMER, new Document(PROCESS_ID_FIELD, 1).append(DELETED_FIELD, 1).append(WORKER_TYPE_FIELD, 1));
        return keys;
    }

    static String indexName(String workerType) {
        return CONNECTOR.equals(workerType) ? CONNECTOR_INDEX_NAME : TRANSFORMER_INDEX_NAME;
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);
        indexKeys().forEach((workerType, key) -> uniqueActiveWorkers(mongoTemplate, workerType, key));
    }

    static void uniqueActiveWorkers(MongoTemplate mongoTemplate, String workerType, Document key) {
        Query activeWorkers = activeWorkersQuery(workerType);
        // Normalize the missing/null flag so the partial unique index covers every active record,
        // while deleted=true history stays outside the index.
        mongoTemplate.updateMulti(activeWorkers, Update.update(DELETED_FIELD, false), COLLECTION_NAME);

        List<Document> workers = mongoTemplate.getCollection(COLLECTION_NAME)
                .find(activeWorkers.getQueryObject())
                .into(new ArrayList<>());
        Map<String, List<Document>> workersByProcess = new LinkedHashMap<>();
        workers.forEach(worker -> workersByProcess
                .computeIfAbsent(worker.getString(PROCESS_ID_FIELD), k -> new ArrayList<>())
                .add(worker));
        workersByProcess.values().stream()
                .filter(group -> group.size() > 1)
                .forEach(group -> removeDuplicates(mongoTemplate, workerType, group));

        Document activeWorkerFilter = new Document(WORKER_TYPE_FIELD, workerType)
                .append(DELETED_FIELD, false)
                .append(PROCESS_ID_FIELD, new Document("$gt", ""));
        ensureUniqueIndex(mongoTemplate.getCollection(COLLECTION_NAME), key, indexName(workerType), activeWorkerFilter);
    }

    static Query activeWorkersQuery(String workerType) {
        return Query.query(Criteria.where(WORKER_TYPE_FIELD).is(workerType)
                .and(DELETED_FIELD).ne(true)
                .and(PROCESS_ID_FIELD).gt(""));
    }

    /**
     * Heartbeats never clear {@code stopping}, so keeping a stopping record would leave the engine
     * filtered from scheduling. Prefer schedulable records, then the API Server patch ordering.
     */
    static Document selectCanonical(List<Document> workers) {
        return workers.stream()
                .max(Comparator.comparing((Document worker) -> !Boolean.TRUE.equals(worker.get("stopping")))
                        .thenComparingLong(V4_25_4_WorkerEngineUnique::pingTime)
                        .thenComparingLong(V4_25_4_WorkerEngineUnique::lastUpdated)
                        .thenComparing(V4_25_4_WorkerEngineUnique::hasWorkerStatus)
                        .thenComparing(worker -> worker.getObjectId("_id"),
                                Comparator.nullsFirst(Comparator.naturalOrder())))
                .orElse(null);
    }

    private static long pingTime(Document worker) {
        return numberValue(worker.get("ping_time"));
    }

    private static long lastUpdated(Document worker) {
        Object value = worker.get("last_updated");
        return value instanceof Date ? ((Date) value).getTime() : numberValue(value);
    }

    private static boolean hasWorkerStatus(Document worker) {
        Object status = worker.get("worker_status");
        return status instanceof Map && !((Map<?, ?>) status).isEmpty();
    }

    private static long numberValue(Object value) {
        return value instanceof Number ? ((Number) value).longValue() : 0L;
    }

    private static void removeDuplicates(MongoTemplate mongoTemplate, String workerType, List<Document> workers) {
        Document canonical = selectCanonical(workers);
        workers.stream()
                .filter(worker -> !worker.getObjectId("_id").equals(canonical.getObjectId("_id")))
                .forEach(worker -> {
                    logger.info("Remove duplicate {} worker {} of process {}, keep {}", workerType,
                            worker.getObjectId("_id"), worker.getString(PROCESS_ID_FIELD), canonical.getObjectId("_id"));
                    mongoTemplate.remove(Query.query(Criteria.where("_id").is(worker.getObjectId("_id"))), COLLECTION_NAME);
                });
    }

    static void ensureUniqueIndex(MongoCollection<Document> collection, Document key, String name, Document partialFilter) {
        Document existing = collection.listIndexes().into(new ArrayList<>()).stream()
                .filter(index -> name.equals(index.getString("name")))
                .findFirst()
                .orElse(null);
        if (existing != null && key.equals(existing.get("key"))
                && existing.getBoolean("unique", false)
                && partialFilter.equals(existing.get("partialFilterExpression"))) {
            return;
        }
        if (existing != null) {
            try {
                collection.dropIndex(name);
            } catch (Exception e) {
                logger.warn("Failed to drop index {}, it may have been removed concurrently", name, e);
            }
        }
        collection.createIndex(key, new IndexOptions().unique(true).name(name).partialFilterExpression(partialFilter));
    }
}

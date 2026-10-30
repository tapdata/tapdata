package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.worker.entity.field.WorkerField;
import com.tapdata.tm.worker.entity.field.WorkerType;
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
    static final String WORKER_TYPE = WorkerType.API_SERVER.getType();
    static final String WORKER_TYPE_FIELD = WorkerField.WORKER_TYPE.field();
    static final String PROCESS_ID_FIELD = WorkerField.PROCESS_ID.field();
    static final String INDEX_NAME = "unq_worker_api_server_active_process";
    static final String WORKER_STATUS_INDEX_NAME = "update_worker_status_index";
    // Compatibility-only input from the superseded Worker field; never used by normal Worker logic.
    static final String LEGACY_DELETE_FIELD = "delete";
    static final String DELETED_FIELD = WorkerField.DELETED.field();
    private static final Logger logger = LogManager.getLogger(V4_25_2_WorkerApiServerUnique.class);

    public V4_25_2_WorkerApiServerUnique(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        logger.info("Execute java patch: {}...", getClass().getName());
        MongoTemplate mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);

        migrateLegacyDeleteField(mongoTemplate);
        Query activeWorkers = activeWorkersQuery();
        // Normalize the legacy missing/null flag so the partial unique index covers every
        // active API Server record while still allowing deleted=true history to remain.
        mongoTemplate.updateMulti(activeWorkers, Update.update(DELETED_FIELD, false), COLLECTION_NAME);

        List<Document> workers = mongoTemplate.getCollection(COLLECTION_NAME)
                .find(activeWorkers.getQueryObject())
                .into(new ArrayList<>());
        Map<String, List<Document>> workersByProcess = new HashMap<>();
        workers.forEach(worker -> workersByProcess
                .computeIfAbsent(worker.getString(PROCESS_ID_FIELD), key -> new ArrayList<>())
                .add(worker));

        workersByProcess.values().stream()
                .filter(group -> group.size() > 1)
                .forEach(group -> removeDuplicates(mongoTemplate, group));

        Document activeWorkerKey = new Document(PROCESS_ID_FIELD, 1).append(WORKER_TYPE_FIELD, 1);
        Document activeWorkerFilter = new Document(WORKER_TYPE_FIELD, WORKER_TYPE)
                .append(DELETED_FIELD, false)
                .append(PROCESS_ID_FIELD, new Document("$gt", ""));
        IndexOptions options = new IndexOptions()
                .unique(true)
                .name(INDEX_NAME)
                .partialFilterExpression(activeWorkerFilter);
        ensureIndex(mongoTemplate, activeWorkerKey, options, true, activeWorkerFilter);

        Document workerStatusKey = new Document(PROCESS_ID_FIELD, 1)
                .append(WORKER_TYPE_FIELD, 1)
                .append(DELETED_FIELD, 1);
        ensureIndex(mongoTemplate, workerStatusKey,
                new IndexOptions().name(WORKER_STATUS_INDEX_NAME).background(true), false, null);
    }

    static Query activeWorkersQuery() {
        return Query.query(Criteria.where(WORKER_TYPE_FIELD).is(WORKER_TYPE)
                .and(DELETED_FIELD).ne(true)
                .and(PROCESS_ID_FIELD).gt(""));
    }

    private static void migrateLegacyDeleteField(MongoTemplate mongoTemplate) {
        Query legacyDeletedWorkers = Query.query(Criteria.where(WORKER_TYPE_FIELD).is(WORKER_TYPE)
                .and(LEGACY_DELETE_FIELD).is(true));
        mongoTemplate.updateMulti(legacyDeletedWorkers,
                new Update().set(DELETED_FIELD, true).unset(LEGACY_DELETE_FIELD), COLLECTION_NAME);

        Query legacyWorkers = Query.query(Criteria.where(WORKER_TYPE_FIELD).is(WORKER_TYPE)
                .and(LEGACY_DELETE_FIELD).exists(true));
        mongoTemplate.updateMulti(legacyWorkers, new Update().unset(LEGACY_DELETE_FIELD), COLLECTION_NAME);
    }

    private static void ensureIndex(MongoTemplate mongoTemplate, Document expectedKey, IndexOptions options,
                                    boolean expectedUnique, Document expectedPartialFilter) {
        var collection = mongoTemplate.getCollection(COLLECTION_NAME);
        Document existing = collection.listIndexes().into(new ArrayList<>()).stream()
                .filter(index -> options.getName().equals(index.getString("name")))
                .findFirst()
                .orElse(null);
        if (existing != null && expectedKey.equals(existing.get("key"))
                && expectedUnique == existing.getBoolean("unique", false)
                && (expectedPartialFilter == null
                ? !existing.containsKey("partialFilterExpression")
                : expectedPartialFilter.equals(existing.get("partialFilterExpression")))) {
            return;
        }
        if (existing != null) {
            collection.dropIndex(options.getName());
        }
        collection.createIndex(expectedKey, options);
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

package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import io.tapdata.utils.AppType;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Extends dashboard measurement retention without shortening an existing TTL and adds the API node trend index.
 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.25-3")
public class V4_25_3_DashboardMetricsIndexes extends AbsPatch {
    static final long MEASUREMENT_TTL_SECONDS = TimeUnit.DAYS.toSeconds(8);
    static final String API_METRICS_NODE_TREND_INDEX = "api_metrics_node_trend";
    static final String MEASUREMENT_LATE_ROLLUP_INDEX = "idx_task_dashboard_dirty";

    public V4_25_3_DashboardMetricsIndexes(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        MongoTemplate defaultTemplate = SpringContextHelper.getBean(MongoTemplate.class);
        ensureApiMetricsNodeTrendIndex(defaultTemplate);

        @SuppressWarnings("unchecked")
        CompletableFuture<MongoTemplate> obsTemplateFuture = SpringContextHelper.applicationContext
                .getBean("obsMongoTemplate", CompletableFuture.class);
        ensureMeasurementRetention(obsTemplateFuture.join());
    }

    static void ensureApiMetricsNodeTrendIndex(MongoTemplate mongoTemplate) {
        String collectionName = MongoUtils.getCollectionName(ApiMetricsRaw.class);
        MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);
        Document expectedKeys = new Document("metricType", 1)
                .append("physicalNodeId", 1)
                .append("timeGranularity", 1)
                .append("timeStart", 1)
                .append("processId", 1);
        List<Document> indexes = collection.listIndexes().into(new ArrayList<>());
        boolean equivalentIndexExists = indexes.stream().anyMatch(index -> expectedKeys.equals(index.get("key")));
        if (equivalentIndexExists) {
            return;
        }
        Document conflictingName = indexes.stream()
                .filter(index -> API_METRICS_NODE_TREND_INDEX.equals(index.getString("name")))
                .findFirst()
                .orElse(null);
        if (conflictingName != null) {
            throw new IllegalStateException("Index name " + API_METRICS_NODE_TREND_INDEX + " exists with a different key pattern");
        }
        collection.createIndex(expectedKeys, new IndexOptions().name(API_METRICS_NODE_TREND_INDEX).background(true));
    }

    static void ensureMeasurementRetention(MongoTemplate obsMongoTemplate) {
        String collectionName = MeasurementEntity.COLLECTION_NAME;
        MongoCollection<Document> collection = obsMongoTemplate.getCollection(collectionName);
        List<Document> indexes = collection.listIndexes().into(new ArrayList<>());
        ensureLateTaskRollupIndex(collection, indexes);
        Document dateIndex = indexes.stream()
                .filter(V4_25_3_DashboardMetricsIndexes::isSingleDateIndex)
                .findFirst()
                .orElse(null);
        if (dateIndex == null) {
            collection.createIndex(new Document("date", -1),
                    new IndexOptions().name("AgentMeasurementV2_date_ttl").expireAfter(MEASUREMENT_TTL_SECONDS, TimeUnit.SECONDS));
            return;
        }

        Object currentTtl = dateIndex.get("expireAfterSeconds");
        if (currentTtl instanceof Number && ((Number) currentTtl).longValue() >= MEASUREMENT_TTL_SECONDS) {
            return;
        }
        Document collMod = new Document("collMod", collectionName)
                .append("index", new Document("keyPattern", dateIndex.get("key"))
                        .append("expireAfterSeconds", MEASUREMENT_TTL_SECONDS));
        obsMongoTemplate.getDb().runCommand(collMod);
    }

    static void ensureLateTaskRollupIndex(MongoCollection<Document> collection, List<Document> indexes) {
        Document expectedKeys = new Document("grnty", 1)
                .append(MeasurementEntity.FIELD_DASHBOARD_ROLLUP_DIRTY, 1)
                .append("tags.taskId", 1)
                .append("date", 1);
        if (indexes.stream().anyMatch(index -> expectedKeys.equals(index.get("key")))) {
            return;
        }
        Document conflictingName = indexes.stream()
                .filter(index -> MEASUREMENT_LATE_ROLLUP_INDEX.equals(index.getString("name")))
                .findFirst()
                .orElse(null);
        if (conflictingName != null) {
            throw new IllegalStateException("Index name " + MEASUREMENT_LATE_ROLLUP_INDEX + " exists with a different key pattern");
        }
        collection.createIndex(expectedKeys, new IndexOptions().name(MEASUREMENT_LATE_ROLLUP_INDEX).background(true));
    }

    private static boolean isSingleDateIndex(Document index) {
        Object key = index.get("key");
        return key instanceof Document && ((Document) key).size() == 1 && ((Document) key).containsKey("date");
    }
}

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

/**
 * Adds the dashboard measurement and API node trend indexes.
 */
@PatchAnnotation(appType = AppType.DAAS, version = "4.25-3")
public class V4_25_3_DashboardMetricsIndexes extends AbsPatch {
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
        MongoCollection<Document> measurement = obsTemplateFuture.join().getCollection(MeasurementEntity.COLLECTION_NAME);
        ensureLateTaskRollupIndex(measurement, measurement.listIndexes().into(new ArrayList<>()));
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
}

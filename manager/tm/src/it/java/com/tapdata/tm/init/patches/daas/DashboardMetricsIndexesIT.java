package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchTypeEnums;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import io.tapdata.utils.AppType;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DashboardMetricsIndexesIT {

    @Test
    @DisplayName("preserves measurement TTL and creates dashboard indexes idempotently")
    void shouldUpgradeDashboardIndexesAgainstMongo() throws Exception {
        String mongoUri = System.getenv("TAPDATA_TEST_MONGO_URI");
        Assumptions.assumeTrue(mongoUri != null && !mongoUri.isBlank(),
                "TAPDATA_TEST_MONGO_URI must point to an isolated Mongo database for this integration test");
        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getenv("TAPDATA_TEST_MONGO_ALLOW_TEMP_DATABASE")),
                "TAPDATA_TEST_MONGO_ALLOW_TEMP_DATABASE=true is required because the test creates and drops a random database");

        MongoClient client = MongoClients.create(mongoUri);
        String databasePrefix = "tap12023_dashboard_indexes_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        MongoTemplate defaultTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(client, databasePrefix + "_default"));
        MongoTemplate obsTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(client, databasePrefix + "_obs"));
        GenericApplicationContext applicationContext = new GenericApplicationContext();
        org.springframework.context.ApplicationContext previousContext = SpringContextHelper.applicationContext;
        Throwable failure = null;
        try {
            obsTemplate.createCollection(MeasurementEntity.COLLECTION_NAME);
            defaultTemplate.createCollection(MongoUtils.getCollectionName(ApiMetricsRaw.class));
            obsTemplate.getCollection(MeasurementEntity.COLLECTION_NAME)
                    .insertOne(new Document("date", new Date()).append("tags", new Document()).append("samples", List.of()));
            obsTemplate.getCollection(MeasurementEntity.COLLECTION_NAME)
                    .createIndex(new Document("date", -1), new com.mongodb.client.model.IndexOptions()
                            .name("measurement_date_ttl")
                            .expireAfter(7L, TimeUnit.DAYS));

            applicationContext.registerBean("mongoTemplate", MongoTemplate.class, () -> defaultTemplate);
            applicationContext.registerBean("obsMongoTemplate", CompletableFuture.class,
                    () -> CompletableFuture.completedFuture(obsTemplate));
            applicationContext.refresh();
            SpringContextHelper.applicationContext = applicationContext;

            V4_25_3_DashboardMetricsIndexes patch = new V4_25_3_DashboardMetricsIndexes(
                    new PatchType(AppType.DAAS, PatchTypeEnums.Script), new PatchVersion(4, 25, 3));
            patch.run();
            awaitIndex(obsTemplate, MeasurementEntity.COLLECTION_NAME,
                    V4_25_3_DashboardMetricsIndexes.MEASUREMENT_LATE_ROLLUP_INDEX);
            awaitIndex(defaultTemplate, MongoUtils.getCollectionName(ApiMetricsRaw.class),
                    V4_25_3_DashboardMetricsIndexes.API_METRICS_NODE_TREND_INDEX);
            assertMeasurementTtl(obsTemplate, TimeUnit.DAYS.toSeconds(7));
            assertEquals(1L, obsTemplate.getCollection(MeasurementEntity.COLLECTION_NAME).countDocuments());
            assertMeasurementLateRollupIndex(obsTemplate);
            assertApiNodeIndex(defaultTemplate);

            patch.run();
            assertMeasurementTtl(obsTemplate, TimeUnit.DAYS.toSeconds(7));
            assertEquals(1, countApiNodeIndexes(defaultTemplate));

            obsTemplate.getDb().runCommand(new Document("collMod", MeasurementEntity.COLLECTION_NAME)
                    .append("index", new Document("keyPattern", new Document("date", -1))
                            .append("expireAfterSeconds", TimeUnit.DAYS.toSeconds(9))));
            patch.run();
            assertMeasurementTtl(obsTemplate, TimeUnit.DAYS.toSeconds(9));
        } catch (Exception | Error e) {
            failure = e;
            throw e;
        } finally {
            SpringContextHelper.applicationContext = previousContext;
            failure = attemptCleanup(failure, () -> {
                if (applicationContext.isActive()) {
                    applicationContext.close();
                }
            });
            failure = attemptCleanup(failure, () -> defaultTemplate.getDb().drop());
            failure = attemptCleanup(failure, () -> obsTemplate.getDb().drop());
            failure = attemptCleanup(failure, client::close);
            rethrowFailure(failure);
        }
    }

    private Throwable attemptCleanup(Throwable failure, Runnable cleanup) {
        try {
            cleanup.run();
        } catch (RuntimeException | Error cleanupFailure) {
            if (failure == null) {
                return cleanupFailure;
            }
            if (failure != cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
        return failure;
    }

    private void rethrowFailure(Throwable failure) throws Exception {
        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure instanceof Error error) {
            throw error;
        }
    }

    private void awaitIndex(MongoTemplate template, String collection, String indexName) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000L;
        while (System.currentTimeMillis() < deadline) {
            boolean present = template.getCollection(collection).listIndexes().into(new java.util.ArrayList<>()).stream()
                    .anyMatch(index -> indexName.equals(index.getString("name")));
            if (present) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("Index " + indexName + " was not created within 15 seconds");
    }

    private void assertMeasurementTtl(MongoTemplate template, long expectedSeconds) {
        Document index = template.getCollection(MeasurementEntity.COLLECTION_NAME).listIndexes().into(new ArrayList<>()).stream()
                .filter(item -> item.get("key", Document.class).equals(new Document("date", -1)))
                .findFirst()
                .orElseThrow();
        assertEquals(expectedSeconds, ((Number) index.get("expireAfterSeconds")).longValue());
    }

    private void assertApiNodeIndex(MongoTemplate template) {
        List<Document> indexes = template.getCollection(MongoUtils.getCollectionName(ApiMetricsRaw.class))
                .listIndexes().into(new ArrayList<>());
        List<Document> matchingIndexes = indexes.stream()
                .filter(index -> V4_25_3_DashboardMetricsIndexes.API_METRICS_NODE_TREND_INDEX.equals(index.getString("name")))
                .toList();
        assertEquals(1, matchingIndexes.size());
        assertEquals(new Document("metricType", 1)
                .append("physicalNodeId", 1)
                .append("timeGranularity", 1)
                .append("timeStart", 1)
                .append("processId", 1), matchingIndexes.get(0).get("key"));
        assertTrue(!Boolean.TRUE.equals(matchingIndexes.get(0).getBoolean("unique")));
    }

    private void assertMeasurementLateRollupIndex(MongoTemplate template) {
        List<Document> indexes = template.getCollection(MeasurementEntity.COLLECTION_NAME)
                .listIndexes().into(new ArrayList<>());
        List<Document> matchingIndexes = indexes.stream()
                .filter(index -> V4_25_3_DashboardMetricsIndexes.MEASUREMENT_LATE_ROLLUP_INDEX.equals(index.getString("name")))
                .toList();
        assertEquals(1, matchingIndexes.size());
        assertEquals(new Document("grnty", 1)
                .append(MeasurementEntity.FIELD_DASHBOARD_ROLLUP_DIRTY, 1)
                .append("tags.taskId", 1)
                .append("date", 1), matchingIndexes.get(0).get("key"));
    }

    private long countApiNodeIndexes(MongoTemplate template) {
        List<Document> indexes = template.getCollection(MongoUtils.getCollectionName(ApiMetricsRaw.class))
                .listIndexes().into(new ArrayList<>());
        return indexes.stream().filter(index -> V4_25_3_DashboardMetricsIndexes.API_METRICS_NODE_TREND_INDEX
                .equals(index.getString("name"))).count();
    }
}

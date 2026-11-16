package com.tapdata.tm.v2.api.monitor.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.entity.ApiCallField;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.base.field.BaseEntityFields;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.ApiMetricsRawFields;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiMetricsRawScheduleExecutorIT {

    @Test
    void keepsSnapshotCountAtomicAndMergesAcrossBoundedSameMinuteBatches() throws Exception {
        String mongoUri = System.getenv("TAPDATA_TEST_MONGO_URI");
        Assumptions.assumeTrue(mongoUri != null && !mongoUri.isBlank(),
                "TAPDATA_TEST_MONGO_URI must point to an isolated Mongo database for this integration test");
        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getenv("TAPDATA_TEST_MONGO_ALLOW_TEMP_DATABASE")),
                "TAPDATA_TEST_MONGO_ALLOW_TEMP_DATABASE=true is required because the test creates and drops a random database");

        MongoClient client = MongoClients.create(mongoUri);
        Document hello;
        try {
            hello = client.getDatabase("admin").runCommand(new Document("hello", 1));
        } catch (RuntimeException | Error e) {
            client.close();
            throw e;
        }
        if (!hello.containsKey("setName") && !"isdbgrid".equals(hello.getString("msg"))) {
            client.close();
            Assumptions.assumeTrue(false, "Mongo transactions require a replica set or mongos");
            return;
        }
        String databaseName = "tap12023_api_raw_transaction_" + UUID.randomUUID().toString().replace("-", "");
        MongoTemplate template = new MongoTemplate(new SimpleMongoClientDatabaseFactory(client, databaseName));

        CountDownLatch rawWriteComplete = new CountDownLatch(1);
        CountDownLatch allowCommit = new CountDownLatch(1);
        AtomicBoolean pauseOnce = new AtomicBoolean();
        PausingExecutor pausingExecutor = new PausingExecutor(rawWriteComplete, allowCommit, pauseOnce);
        ReflectionTestUtils.setField(pausingExecutor, "mongoTemplate", template);
        ApiMetricsRawService snapshotReader = new ApiMetricsRawService();
        ReflectionTestUtils.setField(snapshotReader, "mongoTemplate", template);

        ExecutorService writerPool = null;
        Future<?> writer = null;
        Throwable failure = null;
        try {
            String callCollection = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
            String rawCollection = MongoUtils.getCollectionNameIgnore(ApiMetricsRaw.class);
            assertNotNull(callCollection);
            assertNotNull(rawCollection);
            template.createCollection(callCollection);
            template.createCollection(rawCollection);

            long nowMillis = System.currentTimeMillis();
            long firstMinuteStartMillis = floorToMinute(nowMillis - 120_000L);
            long firstRequestAt = firstMinuteStartMillis + 10_000L;
            List<Document> concurrentCalls = calls(40, nowMillis - 600_000L, firstRequestAt, "api-concurrent-it");
            template.getCollection(apiCallCollection()).insertMany(concurrentCalls);

            writerPool = Executors.newSingleThreadExecutor();
            writer = writerPool.submit(pausingExecutor::aggregateApiCall);
            assertTrue(rawWriteComplete.await(20, TimeUnit.SECONDS), "writer did not flush Raw before commit");

            long firstBucket = TimeGranularity.MINUTE.fixTime(firstRequestAt / 1000L);
            assertEquals(40L, snapshotReader.dashboardRequestCounts(firstBucket, firstBucket + 60L,
                    TimeGranularity.MINUTE, MetricTypes.ALL, null).get(firstBucket));

            allowCommit.countDown();
            writer.get(20, TimeUnit.SECONDS);
            assertEquals(40L, snapshotReader.dashboardRequestCounts(firstBucket, firstBucket + 60L,
                    TimeGranularity.MINUTE, MetricTypes.ALL, null).get(firstBucket));

            long secondMinuteStartMillis = floorToMinute(nowMillis - 240_000L);
            long secondRequestAt = secondMinuteStartMillis + 20_000L;
            int crossBatchCount = ApiMetricsRawScheduleExecutor.BATCH_READ_SIZE + 1;
            template.getCollection(apiCallCollection()).insertMany(
                    calls(crossBatchCount, nowMillis - 600_000L, secondRequestAt, "api-cross-batch-it"));

            ApiMetricsRawScheduleExecutor executor = new ApiMetricsRawScheduleExecutor();
            ReflectionTestUtils.setField(executor, "mongoTemplate", template);
            executor.aggregateApiCall();

            long secondBucket = TimeGranularity.MINUTE.fixTime(secondRequestAt / 1000L);
            ApiMetricsRaw minute = findAllMinute(template, secondBucket);
            assertNotNull(minute);
            assertEquals((long) crossBatchCount, minute.getReqCount());
            assertNotNull(minute.getSubMetrics());
            assertEquals((long) crossBatchCount, minute.getSubMetrics().values().stream()
                    .mapToLong(ApiMetricsRaw::getReqCount).sum());
            assertEquals(crossBatchCount, template.count(Query.query(Criteria.where(ApiCallField.HAS_METRIC.field())
                    .is(true).and(ApiCallField.ALL_PATH_ID.field()).is("api-cross-batch-it")),
                    ApiCallEntity.class, apiCallCollection()));
            assertEquals(0L, template.count(Query.query(Criteria.where(ApiCallField.HAS_METRIC.field())
                    .is(false).and(ApiCallField.ALL_PATH_ID.field()).is("api-cross-batch-it")),
                    ApiCallEntity.class, apiCallCollection()));
        } catch (Exception | Error e) {
            failure = e;
            throw e;
        } finally {
            allowCommit.countDown();
            boolean writerTerminated = stopWriter(writerPool);
            if (writerTerminated) {
                failure = attemptCleanup(failure, () -> template.getDb().drop());
            } else if (failure == null) {
                failure = new IllegalStateException("writer did not terminate; temporary database was retained");
            }
            failure = attemptCleanup(failure, client::close);
            rethrowFailure(failure);
        }
    }

    private static class PausingExecutor extends ApiMetricsRawScheduleExecutor {
        private final CountDownLatch rawWriteComplete;
        private final CountDownLatch allowCommit;
        private final AtomicBoolean pauseOnce;

        private PausingExecutor(CountDownLatch rawWriteComplete, CountDownLatch allowCommit, AtomicBoolean pauseOnce) {
            this.rawWriteComplete = rawWriteComplete;
            this.allowCommit = allowCommit;
            this.pauseOnce = pauseOnce;
        }

        @Override
        void bulkUpsert(MongoOperations operations, List<ApiMetricsRaw> entities,
                        Function<ApiMetricsRaw, Query> queryBuilder,
                        Function<ApiMetricsRaw, org.springframework.data.mongodb.core.query.Update> updateBuilder) {
            super.bulkUpsert(operations, entities, queryBuilder, updateBuilder);
            if (!pauseOnce.compareAndSet(false, true)) {
                return;
            }
            rawWriteComplete.countDown();
            try {
                if (!allowCommit.await(20, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("timed out waiting to commit the Raw batch");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while holding the Raw transaction open", e);
            }
        }
    }

    private List<Document> calls(int count, long createTime, long reqTime, String allPathId) {
        List<Document> calls = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            calls.add(new Document(BaseEntityFields._ID.field(), new ObjectId())
                    .append(BaseEntityFields.CREATE_TIME.field(), new Date(createTime))
                    .append(ApiCallField.ALL_PATH_ID.field(), allPathId)
                    .append(ApiCallField.REQ_PATH.field(), "/" + allPathId)
                    .append(ApiCallField.API_GATEWAY_UUID.field(), "api-server-it")
                    .append(ApiCallField.PHYSICAL_NODE_ID.field(), "node-api-it")
                    .append(ApiCallField.LATENCY.field(), 10D)
                    .append(ApiCallField.DATA_QUERY_TOTAL_TIME.field(), 1D)
                    .append(ApiCallField.REQ_BYTES.field(), 8L)
                    .append(ApiCallField.REQ_TIME.field(), reqTime)
                    .append(ApiCallField.CODE.field(), 200)
                    .append(ApiCallField.HTTP_STATUS.field(), 200)
                    .append(ApiCallField.DELETE.field(), false)
                    .append(ApiCallField.HAS_METRIC.field(), false)
                    .append(ApiCallField.SUPPLEMENT.field(), false)
                    .append(ApiCallField.SUCCEED.field(), true)
                    .append(ApiCallField.WORK_O_ID.field(), "worker-api-it"));
        }
        return calls;
    }

    private ApiMetricsRaw findAllMinute(MongoTemplate template, long minuteStart) {
        Query query = Query.query(Criteria.where(ApiMetricsRawFields.METRIC_TYPE.field()).is(MetricTypes.ALL.getType())
                .and(ApiMetricsRawFields.TIME_GRANULARITY.field()).is(TimeGranularity.MINUTE.getType())
                .and(ApiMetricsRawFields.TIME_START.field()).is(minuteStart));
        return template.findOne(query, ApiMetricsRaw.class, MongoUtils.getCollectionNameIgnore(ApiMetricsRaw.class));
    }

    private String apiCallCollection() {
        return MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
    }

    private long floorToMinute(long timestampMillis) {
        long minuteMillis = TimeGranularity.MINUTE.getSeconds() * 1000L;
        return timestampMillis - timestampMillis % minuteMillis;
    }

    private boolean stopWriter(ExecutorService writerPool) {
        if (writerPool == null) {
            return true;
        }
        writerPool.shutdown();
        try {
            if (writerPool.awaitTermination(20, TimeUnit.SECONDS)) {
                return true;
            }
            writerPool.shutdownNow();
            return writerPool.awaitTermination(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            writerPool.shutdownNow();
            return false;
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
}

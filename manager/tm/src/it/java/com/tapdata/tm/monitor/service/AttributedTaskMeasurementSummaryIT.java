package com.tapdata.tm.monitor.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.constant.Granularity;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.vo.TaskMetricsTrendVo;
import com.tapdata.tm.task.service.TaskService;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class AttributedTaskMeasurementSummaryIT {

    @Test
    void atomicallySummarizesRetainedSamplesAndOverlaysLateMinuteOnHourlyRead() throws Exception {
        String mongoUri = System.getenv("TAPDATA_TEST_MONGO_URI");
        Assumptions.assumeTrue(mongoUri != null && !mongoUri.isBlank(),
                "TAPDATA_TEST_MONGO_URI must point to an isolated Mongo database for this integration test");
        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getenv("TAPDATA_TEST_MONGO_ALLOW_TEMP_DATABASE")),
                "TAPDATA_TEST_MONGO_ALLOW_TEMP_DATABASE=true is required because the test creates and drops a random database");

        MongoClient client = MongoClients.create(mongoUri);
        String databaseName = "tap12023_task_metric_summary_" + UUID.randomUUID().toString().replace("-", "");
        MongoTemplate mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(client, databaseName));
        AtomicBoolean writersTerminated = new AtomicBoolean(true);
        Throwable failure = null;
        try {
            MeasurementServiceV2Impl service = new MeasurementServiceV2Impl(CompletableFuture.completedFuture(mongoTemplate),
                    mock(MetadataInstancesService.class), mock(TaskService.class));
            String taskId = "task-summary-it";
            String physicalNodeId = "node-summary-it";
            Map<String, String> tags = Map.of(
                    MetricCons.Tags.F_TYPE, MetricCons.SampleType.TASK.code(),
                    MetricCons.Tags.F_TASK_ID, taskId,
                    MetricCons.Tags.F_TASK_RECORD_ID, "record-summary-it",
                    "engineId", "engine-summary-it",
                    "physicalNodeId", physicalNodeId);
            long receivedMinute = Instant.now().toEpochMilli();
            long priorMinuteStart = receivedMinute - receivedMinute % 60_000L - 60_000L;
            long sampleStart = priorMinuteStart + 1_000L;

            List<SampleRequest> samples = new ArrayList<>();
            for (int sampleNumber = 1; sampleNumber <= 201; sampleNumber++) {
                samples.add(sample(tags, sampleStart + sampleNumber * 100L, sampleNumber));
            }
            service.addAgentMeasurement(samples);

            MeasurementEntity minute = findMinute(mongoTemplate, taskId, physicalNodeId, priorMinuteStart);
            assertNotNull(minute);
            assertTrue(minute.getId().startsWith("tapdata-task-metric-v2:"));
            assertTrue(Boolean.TRUE.equals(minute.getDashboardRollupDirty()));
            assertEquals(200, minute.getSamples().size());
            assertEquals(200L, minute.getDashboardOutputQpsCount());
            assertEquals(20_300D, minute.getDashboardOutputQpsSum());
            assertEquals(40_600D, minute.getDashboardOutputSizeQpsSum());
            assertEquals(200L, minute.getDashboardOutputSizeQpsCount());

            // Same timestamp is a retry/update of one retained sample; it replaces rather than duplicates.
            service.addAgentMeasurement(List.of(sample(tags, sampleStart + 201L * 100L, 999)));
            // Another late sample replaces a retained timestamp while keeping the original 200-sample cap.
            service.addAgentMeasurement(List.of(sample(tags, sampleStart + 150L * 100L, 777)));
            minute = findMinute(mongoTemplate, taskId, physicalNodeId, priorMinuteStart);
            assertEquals(200, minute.getSamples().size());
            assertEquals(200L, minute.getDashboardOutputQpsCount());
            assertEquals(21_725D, minute.getDashboardOutputQpsSum());
            assertEquals(43_450D, minute.getDashboardOutputSizeQpsSum());

            long hourStart = priorMinuteStart - priorMinuteStart % 3_600_000L;
            service.aggregateMeasurementByGranularity(Map.of(), hourStart, hourStart + 3_600_000L,
                    Granularity.GRANULARITY_MINUTE);
            MeasurementEntity hourly = mongoTemplate.findOne(Query.query(Criteria.where(MetricCons.F_GRANULARITY)
                    .is(Granularity.GRANULARITY_HOUR)
                    .and(MetricCons.F_DATE).is(new Date(hourStart))
                    .and(MetricCons.Tags.path(MetricCons.Tags.F_TASK_ID)).is(taskId)),
                    MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
            assertNotNull(hourly);
            assertEquals(1, hourly.getDashboardMinuteSummaries().size());

            TaskMetricsTrendVo trend = service.aggregateTaskMetricsByTaskIdsV2(List.of(taskId),
                    hourStart, hourStart + 2L * 3_600_000L, physicalNodeId);

            assertEquals(21_725D / 200D, trend.getOutputQps().get(0), 0.000001D);
            assertEquals(43_450D / 200D, trend.getOutputSizeQps().get(0), 0.000001D);

            assertConcurrentFirstUpsertIsUnique(service, mongoTemplate, priorMinuteStart, sampleStart, writersTerminated);
        } catch (Exception | Error e) {
            failure = e;
            throw e;
        } finally {
            if (writersTerminated.get()) {
                failure = attemptCleanup(failure, () -> mongoTemplate.getDb().drop());
            }
            failure = attemptCleanup(failure, client::close);
            rethrowFailure(failure);
        }
    }

    private MeasurementEntity findMinute(MongoTemplate template, String taskId, String physicalNodeId, long date) {
        Query query = Query.query(Criteria.where(MetricCons.F_GRANULARITY).is(Granularity.GRANULARITY_MINUTE)
                .and(MetricCons.F_DATE).is(new Date(date))
                .and(MetricCons.Tags.path(MetricCons.Tags.F_TASK_ID)).is(taskId)
                .and(MetricCons.Tags.path("physicalNodeId")).is(physicalNodeId));
        return template.findOne(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
    }

    private SampleRequest sample(Map<String, String> tags, long timestamp, double outputQps) {
        Sample sample = new Sample();
        sample.setDate(new Date(timestamp));
        sample.setVs(Map.of(MetricCons.SS.VS.F_OUTPUT_QPS, outputQps,
                MetricCons.SS.VS.F_OUTPUT_SIZE_QPS, outputQps * 2D));
        SampleRequest request = new SampleRequest();
        request.setTags(tags);
        request.setSample(sample);
        return request;
    }

    private void assertConcurrentFirstUpsertIsUnique(MeasurementServiceV2Impl service, MongoTemplate template,
                                                     long minuteStart, long sampleStart,
                                                     AtomicBoolean writersTerminated) throws Exception {
        String taskId = "task-summary-concurrent";
        String physicalNodeId = "node-summary-it";
        Map<String, String> tags = Map.of(
                MetricCons.Tags.F_TYPE, MetricCons.SampleType.TASK.code(),
                MetricCons.Tags.F_TASK_ID, taskId,
                MetricCons.Tags.F_TASK_RECORD_ID, "record-summary-concurrent",
                "engineId", "engine-summary-it",
                "physicalNodeId", physicalNodeId);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Throwable failure = null;
        try {
            Future<?> first = executor.submit(() -> {
                await(start);
                service.addAgentMeasurement(List.of(sample(tags, sampleStart + 30_000L, 10D)));
            });
            Future<?> second = executor.submit(() -> {
                await(start);
                service.addAgentMeasurement(List.of(sample(tags, sampleStart + 30_100L, 20D)));
            });
            start.countDown();
            awaitWriter(first, "first");
            awaitWriter(second, "second");

            Query query = Query.query(Criteria.where(MetricCons.F_GRANULARITY).is(Granularity.GRANULARITY_MINUTE)
                    .and(MetricCons.F_DATE).is(new Date(minuteStart))
                    .and(MetricCons.Tags.path(MetricCons.Tags.F_TASK_ID)).is(taskId)
                    .and(MetricCons.Tags.path("physicalNodeId")).is(physicalNodeId));
            assertEquals(1L, template.count(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME));
            MeasurementEntity minute = template.findOne(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
            assertNotNull(minute);
            assertEquals(2L, minute.getDashboardOutputQpsCount());
            assertEquals(30D, minute.getDashboardOutputQpsSum());
        } catch (Exception | Error e) {
            failure = e;
            throw e;
        } finally {
            start.countDown();
            failure = attemptCleanup(failure, executor::shutdownNow);
            boolean terminated = false;
            try {
                terminated = executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failure = appendFailure(failure, e);
            }
            writersTerminated.set(terminated);
            if (!terminated) {
                failure = appendFailure(failure,
                        new TimeoutException("Concurrent metric writers did not terminate within 30 seconds"));
            }
            rethrowFailure(failure);
        }
    }

    private void awaitWriter(Future<?> writer, String name) throws Exception {
        try {
            writer.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            TimeoutException timeout = new TimeoutException(name + " metric writer timed out after 30 seconds");
            timeout.initCause(e);
            throw timeout;
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException(name + " metric writer failed", e.getCause());
        }
    }

    private void await(CountDownLatch start) {
        try {
            if (!start.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting to start concurrent metric writer");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while starting concurrent write", e);
        }
    }

    private Throwable attemptCleanup(Throwable failure, Runnable cleanup) {
        try {
            cleanup.run();
        } catch (RuntimeException | Error cleanupFailure) {
            return appendFailure(failure, cleanupFailure);
        }
        return failure;
    }

    private Throwable appendFailure(Throwable failure, Throwable nextFailure) {
        if (failure == null) {
            return nextFailure;
        }
        if (failure != nextFailure) {
            failure.addSuppressed(nextFailure);
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

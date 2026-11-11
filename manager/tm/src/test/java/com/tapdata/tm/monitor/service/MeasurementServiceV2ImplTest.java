package com.tapdata.tm.monitor.service;


import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.constant.Granularity;
import com.tapdata.tm.monitor.constant.KeyWords;
import com.tapdata.tm.monitor.dto.TableSyncStaticDto;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import com.tapdata.tm.monitor.param.SyncStatusStatisticsParam;
import com.tapdata.tm.monitor.vo.TableSyncStaticVo;
import com.tapdata.tm.monitor.vo.TaskMetricsTrendVo;
import com.tapdata.tm.task.bean.TableStatusInfoDto;
import com.tapdata.tm.task.res.CpuMemoryService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import io.github.openlg.graphlib.Graph;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.schema.partition.TapSubPartitionTableInfo;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.tapdata.tm.monitor.param.MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_CONTINUOUS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MeasurementServiceV2ImplTest {
    MeasurementServiceV2Impl measurementServiceV2;
    MongoTemplate mongoOperations;
    @BeforeEach
    void init() {
        measurementServiceV2 = mock(MeasurementServiceV2Impl.class);
        mongoOperations = mock(MongoTemplate.class);
        ReflectionTestUtils.setField(measurementServiceV2, "mongoOperations", mongoOperations);
    }
    @Test
    void testParams() {
        assertEquals("initial_sync", KeyWords.INITIAL_SYNC);
        assertEquals("DONE", KeyWords.DONE);
        assertEquals("complete", KeyWords.COMPLETE);
    }

    @Test
    void selectedNodeBucketIsNullWhenLegacySamplesShareTheBucket() {
        MeasurementServiceV2Impl.TaskMetricBucket bucket = new MeasurementServiceV2Impl.TaskMetricBucket();
        ReflectionTestUtils.setField(bucket, "sampleCount", 2L);
        ReflectionTestUtils.setField(bucket, "unattributedSampleCount", 1L);
        MeasurementServiceV2Impl.MetricAccumulator node = new MeasurementServiceV2Impl.MetricAccumulator();
        ReflectionTestUtils.setField(node, "sampleCount", 1L);
        ReflectionTestUtils.setField(node, "outputQpsSum", 100D);
        metricAccumulators(bucket).put("node-a", node);

        assertNull(MeasurementServiceV2Impl.nodeBucketContribution(bucket, "node-a"));
    }

    @Test
    void selectedNodeZeroSampleDiffersFromMissingSample() {
        MeasurementServiceV2Impl.TaskMetricBucket zeroBucket = new MeasurementServiceV2Impl.TaskMetricBucket();
        ReflectionTestUtils.setField(zeroBucket, "sampleCount", 1L);
        MeasurementServiceV2Impl.MetricAccumulator zeroNode = new MeasurementServiceV2Impl.MetricAccumulator();
        ReflectionTestUtils.setField(zeroNode, "sampleCount", 1L);
        ReflectionTestUtils.setField(zeroNode, "outputQpsSum", 0D);
        metricAccumulators(zeroBucket).put("node-a", zeroNode);

        assertEquals(0D, MeasurementServiceV2Impl.nodeBucketContribution(zeroBucket, "node-a"), 0D);
        assertNull(MeasurementServiceV2Impl.nodeBucketContribution(new MeasurementServiceV2Impl.TaskMetricBucket(), "node-a"));
    }

    @Test
    void selectedNodeMissingSampleDiffersFromAnotherNodesSample() {
        MeasurementServiceV2Impl.TaskMetricBucket bucket = new MeasurementServiceV2Impl.TaskMetricBucket();
        ReflectionTestUtils.setField(bucket, "sampleCount", 1L);
        MeasurementServiceV2Impl.MetricAccumulator otherNode = new MeasurementServiceV2Impl.MetricAccumulator();
        ReflectionTestUtils.setField(otherNode, "sampleCount", 1L);
        ReflectionTestUtils.setField(otherNode, "outputQpsSum", 100D);
        metricAccumulators(bucket).put("node-b", otherNode);

        assertNull(MeasurementServiceV2Impl.nodeBucketContribution(bucket, "node-a"));
    }

    @Test
    void aggregateV2UsesOneHourDenominatorAcrossMigrationNodes() throws Exception {
        long taskSampleStart = 0L;
        long endAt = 2L * 60L * 60L * 1000L;
        String taskId = "task-1";
        List<Sample> nodeASamples = new ArrayList<>();
        nodeASamples.add(metricSample(-60_000L, 999D));
        for (int minute = 0; minute < 30; minute++) {
            nodeASamples.add(metricSample(minute * 60_000L, 100D));
        }
        List<Sample> nodeBSamples = new ArrayList<>();
        for (int minute = 30; minute < 60; minute++) {
            nodeBSamples.add(metricSample(minute * 60_000L, 100D));
        }
        nodeBSamples.add(metricSample(endAt, 999D));
        List<MeasurementEntity> rows = List.of(
                taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR, nodeASamples),
                taskMetricsEntity(taskId, "node-b", Granularity.GRANULARITY_HOUR, nodeBSamples));
        List<String> queriedGranularities = new ArrayList<>();
        MeasurementServiceV2Impl service = measurementServiceWithRows(rows, queriedGranularities);

        TaskMetricsTrendVo nodeA = service.aggregateTaskMetricsByTaskIdsV2(List.of(taskId), taskSampleStart, endAt, "node-a");
        TaskMetricsTrendVo nodeB = service.aggregateTaskMetricsByTaskIdsV2(List.of(taskId), taskSampleStart, endAt, "node-b");
        TaskMetricsTrendVo allNodes = service.aggregateTaskMetricsByTaskIdsV2(List.of(taskId), taskSampleStart, endAt, null);

        assertEquals(Arrays.asList(50D, null), nodeA.getOutputQps());
        assertEquals(Arrays.asList(50D, null), nodeB.getOutputQps());
        assertEquals(Arrays.asList(100D, null), allNodes.getOutputQps());
        assertEquals(3600L, nodeA.getIntervalSeconds());
        assertTrue(queriedGranularities.contains(Granularity.GRANULARITY_HOUR));
        assertFalse(queriedGranularities.contains(Granularity.GRANULARITY_MINUTE));
    }

    @Test
    void selectedNodeWithoutSamplesInAPopulatedMinuteBucketReportsMissingInsteadOfFailing() throws Exception {
        long startAt = 0L;
        long endAt = 60L * 60L * 1000L;
        List<MeasurementEntity> rows = List.of(
                taskMetricsEntity("task-a", "node-a", Granularity.GRANULARITY_MINUTE,
                        List.of(metricSample(0L, 30D))));
        MeasurementServiceV2Impl service = measurementServiceWithRows(rows, new ArrayList<>());

        TaskMetricsTrendVo result = service.aggregateTaskMetricsByTaskIdsV2(
                List.of("task-a"), startAt, endAt, "node-b");

        assertEquals(60, result.getOutputQps().size());
        assertTrue(result.getOutputQps().stream().allMatch(Objects::isNull));
        assertEquals("no_data", result.getStatus());
    }

    @Test
    void selectedNodeAndAllUseTheSameComputableMinuteDenominator() throws Exception {
        String taskId = "task-common-minute-denominator";
        MeasurementEntity legacyUnattributedZero = taskMetricsEntity(taskId, null,
                Granularity.GRANULARITY_HOUR, List.of(metricSample(0L, 0D)));
        legacyUnattributedZero.getTags().remove("physicalNodeId");
        MeasurementEntity attributedNodeA = taskMetricsEntity(taskId, "node-a",
                Granularity.GRANULARITY_HOUR, List.of(metricSample(60_000L, 100D)));
        attributedNodeA.setDashboardMinuteSummaries(List.of(
                dashboardMinuteSummary(60_000L, 1_200D, 12L, 120D, 12L)));
        MeasurementServiceV2Impl service = measurementServiceWithRows(
                List.of(legacyUnattributedZero, attributedNodeA), new ArrayList<>());

        TaskMetricsTrendVo selected = service.aggregateTaskMetricsByTaskIdsV2(
                List.of(taskId), 0L, 2L * 60L * 60L * 1000L, "node-a");
        TaskMetricsTrendVo all = service.aggregateTaskMetricsByTaskIdsV2(
                List.of(taskId), 0L, 2L * 60L * 60L * 1000L, null);
        TaskMetricsTrendVo noSelectedEvidence = service.aggregateTaskMetricsByTaskIdsV2(
                List.of(taskId), 0L, 2L * 60L * 60L * 1000L, "node-b");

        assertEquals(Arrays.asList(50D, null), selected.getOutputQps());
        assertEquals(Arrays.asList(50D, null), all.getOutputQps());
        assertEquals("partial", selected.getCoverage());
        assertEquals(Arrays.asList(null, null), noSelectedEvidence.getOutputQps());
    }

    @Test
    void hourlyCoverageRequiresEveryExpectedMinuteButAllowsPartialEdgeHours() throws Exception {
        String taskId = "task-hourly-coverage";
        long oneHour = 3_600_000L;
        MeasurementEntity sparseHour0 = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                List.of(metricSample(0L, 10D)));
        MeasurementEntity sparseHour1 = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                List.of(metricSample(oneHour, 20D)));
        MeasurementServiceV2Impl sparseService = measurementServiceWithRows(
                List.of(sparseHour0, sparseHour1), new ArrayList<>());

        TaskMetricsTrendVo sparse = sparseService.aggregateTaskMetricsByTaskIdsV2(
                List.of(taskId), 0L, 2L * oneHour, null);

        assertEquals(Arrays.asList(10D, 20D), sparse.getOutputQps());
        assertEquals("partial", sparse.getCoverage());

        MeasurementEntity fullHour0 = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                samplesForMinuteRange(0L, 60, 10D));
        MeasurementEntity fullHour1 = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                samplesForMinuteRange(oneHour, 60, 10D));
        MeasurementServiceV2Impl fullService = measurementServiceWithRows(
                List.of(fullHour0, fullHour1), new ArrayList<>());

        TaskMetricsTrendVo full = fullService.aggregateTaskMetricsByTaskIdsV2(
                List.of(taskId), 0L, 2L * oneHour, null);

        assertEquals(Arrays.asList(10D, 10D), full.getOutputQps());
        assertEquals("complete", full.getCoverage());

        long partialStart = 30L * 60L * 1000L;
        long partialEnd = 2L * oneHour + 30L * 60L * 1000L;
        List<MeasurementEntity> partialRows = List.of(
                taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_MINUTE,
                        samplesForMinuteRange(partialStart, 30, 10D)),
                taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                        samplesForMinuteRange(oneHour, 60, 10D)),
                taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_MINUTE,
                        samplesForMinuteRange(2L * oneHour, 30, 10D)));
        MeasurementServiceV2Impl partialService = measurementServiceWithRows(partialRows, new ArrayList<>());

        TaskMetricsTrendVo partialEdges = partialService.aggregateTaskMetricsByTaskIdsV2(
                List.of(taskId), partialStart, partialEnd, null);

        assertEquals(Arrays.asList(10D, 10D, 10D), partialEdges.getOutputQps());
        assertEquals("complete", partialEdges.getCoverage());
    }

    @Test
    void hourlyRollupWeightsLegacyAndSummarizedMinutesEqually() throws Exception {
        String taskId = "task-hourly-weights";
        MeasurementEntity hourly = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                List.of(metricSample(0L, 200D), metricSample(60_000L, 100D)));
        hourly.setDashboardMinuteSummaries(List.of(dashboardMinuteSummary(60_000L, 1_200D, 12L,
                120D, 12L)));
        MeasurementServiceV2Impl service = measurementServiceWithRows(List.of(hourly), new ArrayList<>());

        TaskMetricsTrendVo result = service.aggregateTaskMetricsByTaskIdsV2(
                List.of(taskId), 0L, 2L * 60L * 60L * 1000L, null);

        assertEquals(Arrays.asList(150D, null), result.getOutputQps());
    }

    @Test
    void hourlyRollupSkipsLegacyAndWeightedSamplesMixedInTheSameMinute() throws Exception {
        String taskId = "task-hourly-mixed";
        MeasurementEntity oldNode = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                List.of(metricSample(0L, 200D)));
        MeasurementEntity newNode = taskMetricsEntity(taskId, "node-b", Granularity.GRANULARITY_HOUR,
                List.of(metricSample(0L, 100D), metricSample(60_000L, 60D)));
        newNode.setDashboardMinuteSummaries(List.of(
                dashboardMinuteSummary(0L, 1_200D, 12L, 120D, 12L),
                dashboardMinuteSummary(60_000L, 720D, 12L, 72D, 12L)));
        MeasurementServiceV2Impl service = measurementServiceWithRows(List.of(oldNode, newNode), new ArrayList<>());

        TaskMetricsTrendVo result = service.aggregateTaskMetricsByTaskIdsV2(
                List.of(taskId), 0L, 2L * 60L * 60L * 1000L, null);

        assertEquals(Arrays.asList(60D, null), result.getOutputQps());
        assertEquals("partial", result.getCoverage());
    }

    @Test
    void hourlyRollupMarksInconsistentDuplicateSamplesAsUntrusted() throws Exception {
        String taskId = "task-hourly-duplicate";
        MeasurementEntity first = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                List.of(metricSample(0L, 10D)));
        MeasurementEntity second = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                List.of(metricSample(0L, 20D)));
        MeasurementServiceV2Impl service = measurementServiceWithRows(List.of(first, second), new ArrayList<>());

        TaskMetricsTrendVo result = service.aggregateTaskMetricsByTaskIdsV2(
                List.of(taskId), 0L, 2L * 60L * 60L * 1000L, null);

        assertEquals(Arrays.asList(null, null), result.getOutputQps());
        assertEquals("unknown", result.getCoverage());
    }

    @Test
    void taskCounterRecoverySelectsLatestActualSampleAcrossNodesInTheNewestMinute() throws Exception {
        String taskId = "task-counter-recovery";
        String taskRecordId = "record-current";
        MeasurementEntity nodeB = taskMetricsEntity(taskId, "node-b", Granularity.GRANULARITY_MINUTE,
                List.of(counterSample(50_000L, 200L)));
        nodeB.getTags().put(MetricCons.Tags.F_TASK_RECORD_ID, taskRecordId);
        nodeB.setDate(new Date(0L));
        MeasurementEntity nodeA = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_MINUTE,
                List.of(counterSample(10_000L, 100L)));
        nodeA.getTags().put(MetricCons.Tags.F_TASK_RECORD_ID, taskRecordId);
        nodeA.getTags().remove("physicalNodeId");
        nodeA.setDate(new Date(0L));
        List<Query> queries = new ArrayList<>();
        MeasurementServiceV2Impl service = measurementServiceWithRows(
                List.of(nodeB, nodeA), new ArrayList<>(), queries);

        Object response = service.getSamples(taskCounterRecoveryQuery(taskId, taskRecordId, 0L, 60_000L), true);

        assertEquals(1, ((Number) ((Map<?, ?>) response).get("taskCounterRecoverySchemaVersion")).intValue());
        Map<String, Object> item = onlyRecoveryItem(response);
        assertEquals(200L, ((Number) item.get(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL)).longValue());
        assertEquals(taskId, ((Map<?, ?>) item.get("tags")).get(MetricCons.Tags.F_TASK_ID));
        assertEquals(-1, ((Number) queries.get(0).getSortObject().get(MetricCons.F_DATE)).intValue());
    }

    @Test
    void taskCounterRecoveryUsesNewestDocumentMinuteThenNewestSampleDate() throws Exception {
        String taskId = "task-counter-newest-minute";
        String taskRecordId = "record-current";
        MeasurementEntity olderMinute = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_MINUTE,
                List.of(counterSample(50_000L, 900L)));
        olderMinute.getTags().put(MetricCons.Tags.F_TASK_RECORD_ID, taskRecordId);
        olderMinute.setDate(new Date(0L));
        MeasurementEntity currentNodeA = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_MINUTE,
                List.of(counterSample(61_000L, 100L)));
        currentNodeA.getTags().put(MetricCons.Tags.F_TASK_RECORD_ID, taskRecordId);
        currentNodeA.setDate(new Date(60_000L));
        MeasurementEntity currentNodeB = taskMetricsEntity(taskId, "node-b", Granularity.GRANULARITY_MINUTE,
                List.of(counterSample(65_000L, 200L)));
        currentNodeB.getTags().put(MetricCons.Tags.F_TASK_RECORD_ID, taskRecordId);
        currentNodeB.setDate(new Date(60_000L));
        MeasurementServiceV2Impl service = measurementServiceWithRows(
                List.of(olderMinute, currentNodeB, currentNodeA), new ArrayList<>());

        Object response = service.getSamples(
                taskCounterRecoveryQuery(taskId, taskRecordId, 0L, 120_000L), true);

        assertEquals(200L, ((Number) onlyRecoveryItem(response)
                .get(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL)).longValue());
    }

    @Test
    void olderTimestampConflictDoesNotOverrideALaterCounterSample() throws Exception {
        String taskId = "task-counter-later-sample";
        String taskRecordId = "record-current";
        MeasurementEntity firstA = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_MINUTE,
                List.of(counterSample(10_000L, 100L)));
        MeasurementEntity firstB = taskMetricsEntity(taskId, "node-b", Granularity.GRANULARITY_MINUTE,
                List.of(counterSample(10_000L, 101L)));
        MeasurementEntity latestC = taskMetricsEntity(taskId, "node-c", Granularity.GRANULARITY_MINUTE,
                List.of(counterSample(50_000L, 200L)));
        for (MeasurementEntity row : List.of(firstA, firstB, latestC)) {
            row.getTags().put(MetricCons.Tags.F_TASK_RECORD_ID, taskRecordId);
            row.setDate(new Date(0L));
        }
        MeasurementServiceV2Impl service = measurementServiceWithRows(
                List.of(firstA, firstB, latestC), new ArrayList<>());

        Object response = service.getSamples(
                taskCounterRecoveryQuery(taskId, taskRecordId, 0L, 60_000L), true);

        assertEquals(200L, ((Number) onlyRecoveryItem(response)
                .get(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL)).longValue());
    }

    @Test
    void taskCounterRecoveryFiltersByRecordAndExcludesOutOfRangeSamples() throws Exception {
        String taskId = "task-counter-record-filter";
        MeasurementEntity oldRecord = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_MINUTE,
                List.of(counterSample(50_000L, 900L)));
        oldRecord.getTags().put(MetricCons.Tags.F_TASK_RECORD_ID, "record-old");
        oldRecord.setDate(new Date(0L));
        MeasurementEntity currentRecord = taskMetricsEntity(taskId, "node-b", Granularity.GRANULARITY_MINUTE,
                List.of(counterSample(-1L, 1L), counterSample(10_000L, 100L), counterSample(100_001L, 999L)));
        currentRecord.getTags().put(MetricCons.Tags.F_TASK_RECORD_ID, "record-current");
        currentRecord.setDate(new Date(0L));
        MeasurementServiceV2Impl service = measurementServiceWithRows(
                List.of(oldRecord, currentRecord), new ArrayList<>());

        Object response = service.getSamples(
                taskCounterRecoveryQuery(taskId, "record-current", 0L, 100_000L), true);

        assertEquals(100L, ((Number) onlyRecoveryItem(response)
                .get(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL)).longValue());
    }

    @Test
    void taskCounterRecoveryReturnsEmptyForNoSamplesAndFailsOnConflictingTimestamp() throws Exception {
        String taskId = "task-counter-empty";
        MeasurementServiceV2Impl emptyService = measurementServiceWithRows(List.of(), new ArrayList<>());
        Object emptyResponse = emptyService.getSamples(
                taskCounterRecoveryQuery(taskId, "record-current", 0L, 60_000L), true);
        assertEquals(1, ((Number) ((Map<?, ?>) emptyResponse).get("taskCounterRecoverySchemaVersion")).intValue());
        assertTrue(recoveryItems(emptyResponse).isEmpty());

        MeasurementEntity first = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_MINUTE,
                List.of(counterSample(10_000L, 100L)));
        first.getTags().put(MetricCons.Tags.F_TASK_RECORD_ID, "record-current");
        first.setDate(new Date(0L));
        MeasurementEntity conflicting = taskMetricsEntity(taskId, "node-b", Granularity.GRANULARITY_MINUTE,
                List.of(counterSample(10_000L, 200L)));
        conflicting.getTags().put(MetricCons.Tags.F_TASK_RECORD_ID, "record-current");
        conflicting.setDate(new Date(0L));
        MeasurementServiceV2Impl conflictService = measurementServiceWithRows(
                List.of(first, conflicting), new ArrayList<>());

        assertThrows(IllegalStateException.class, () -> conflictService.getSamples(
                taskCounterRecoveryQuery(taskId, "record-current", 0L, 60_000L), true));
    }

    @Test
    void taskCounterRecoveryRejectsNewServiceRowsWithPartialCoreCounters() throws Exception {
        String taskId = "task-counter-partial-core";
        Sample incomplete = new Sample();
        incomplete.setDate(new Date(10_000L));
        incomplete.setVs(Map.of(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL, 100L));
        MeasurementEntity row = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_MINUTE,
                List.of(incomplete));
        row.getTags().put(MetricCons.Tags.F_TASK_RECORD_ID, "record-current");
        row.setDate(new Date(0L));
        MeasurementServiceV2Impl service = measurementServiceWithRows(List.of(row), new ArrayList<>());

        assertThrows(IllegalStateException.class, () -> service.getSamples(
                taskCounterRecoveryQuery(taskId, "record-current", 0L, 60_000L), true));
    }

    @Test
    void taskCounterRecoveryStreamFailurePropagatesAndOrdinaryInstantUsesLegacyQuery() throws Exception {
        MongoTemplate mongo = mock(MongoTemplate.class);
        MeasurementServiceV2Impl service = new MeasurementServiceV2Impl(
                CompletableFuture.completedFuture(mongo), mock(MetadataInstancesService.class), mock(TaskService.class));
        ReflectionTestUtils.setField(service, "cpuMemoryService", mock(CpuMemoryService.class));
        MeasurementQueryParam query = taskCounterRecoveryQuery("task-counter-error", "record-current", 0L, 60_000L);
        when(mongo.stream(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                .thenThrow(new IllegalStateException("stream failed"));

        assertThrows(IllegalStateException.class, () -> service.getSamples(query, true));

        @SuppressWarnings("unchecked")
        AggregationResults<MeasurementEntity> emptyResults = mock(AggregationResults.class);
        when(emptyResults.getMappedResults()).thenReturn(Collections.emptyList());
        when(mongo.aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(MeasurementEntity.class)))
                .thenReturn(emptyResults);
        MeasurementQueryParam incomplete = taskCounterRecoveryQuery(
                "task-counter-error", "record-current", 0L, 60_000L);
        incomplete.getSamples().get("retriever").getTags().remove(MetricCons.Tags.F_TASK_RECORD_ID);
        assertTrue(recoveryItems(service.getSamples(incomplete, true)).isEmpty());
        assertTrue(recoveryItems(service.getSamples(query, false)).isEmpty());
        verify(mongo, times(2)).aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(MeasurementEntity.class));
    }

    @Test
    void attributedHourlyRollupUsesAtomicDateReplacementAndStableIdentity() {
        Map<String, String> tags = Map.of("type", "task", "taskId", "hourly-pipeline",
                "taskRecordId", "record-1", "physicalNodeId", "node-a");
        List<Map<String, Object>> samples = List.of(metricSample(0L, 200D).toMap(),
                metricSample(60_000L, 100D).toMap());
        MeasurementEntity.DashboardMinuteSummary summary = dashboardMinuteSummary(
                60_000L, 1_200D, 12L, 120D, 12L);

        List<org.bson.conversions.Bson> pipeline = MeasurementServiceV2Impl.attributedTaskHourRollupPipeline(
                tags, new Date(0L), samples, List.of(), List.of(summary), 0L, 60_000L);
        String pipelineText = pipeline.toString();

        assertEquals(1, pipeline.size());
        assertTrue(pipelineText.contains("$reduce"));
        assertTrue(pipelineText.contains("$filter"));
        assertTrue(pipelineText.contains("$concatArrays"));
        assertTrue(pipelineText.contains("$slice"));
        assertTrue(pipelineText.contains("dashboardMinuteSummaries"));
        assertEquals(MeasurementServiceV2Impl.attributedTaskHourDocumentId(tags, new Date(0L)),
                MeasurementServiceV2Impl.attributedTaskHourDocumentId(new TreeMap<>(tags), new Date(0L)));
    }

    @Test
    void selectedNodeKeepsKnownTaskContributionWhenAnotherTaskRunsOnlyOnAnotherNode() throws Exception {
        long startAt = 0L;
        long endAt = 2L * 60L * 60L * 1000L;
        List<MeasurementEntity> rows = List.of(
                taskMetricsEntity("task-a", "node-a", Granularity.GRANULARITY_HOUR,
                        List.of(metricSample(0L, 30D))),
                taskMetricsEntity("task-b", "node-b", Granularity.GRANULARITY_HOUR,
                        List.of(metricSample(0L, 60D))));
        MeasurementServiceV2Impl service = measurementServiceWithRows(rows, new ArrayList<>());

        TaskMetricsTrendVo result = service.aggregateTaskMetricsByTaskIdsV2(
                List.of("task-a", "task-b"), startAt, endAt, "node-a");

        assertEquals(Arrays.asList(30D, null), result.getOutputQps());
        assertEquals("partial", result.getCoverage());
    }

    @Test
    void dirtyMinuteSummaryReplacesDuplicateHourlySamplesWithoutChangingTheCommonDenominator() throws Exception {
        long startAt = 0L;
        long endAt = 2L * 60L * 60L * 1000L;
        Sample staleOne = metricSample(0L, 10D);
        Sample staleDuplicate = metricSample(0L, 10D);
        Sample nextMinute = metricSample(60_000L, 30D);
        MeasurementEntity hourly = taskMetricsEntity("task-dirty", "node-a", Granularity.GRANULARITY_HOUR,
                List.of(staleOne, staleDuplicate, nextMinute));
        hourly.setDashboardMinuteSummaries(List.of(
                dashboardMinuteSummary(0L, 120D, 12L, 12D, 12L),
                dashboardMinuteSummary(60_000L, 360D, 12L, 36D, 12L)));
        MeasurementEntity dirtyMinute = taskMetricsEntity("task-dirty", "node-a", Granularity.GRANULARITY_MINUTE,
                List.of(metricSample(0L, 20D)));
        dirtyMinute.setDashboardRollupDirty(true);
        dirtyMinute.setDashboardOutputQpsSum(240D);
        dirtyMinute.setDashboardOutputQpsCount(12L);
        dirtyMinute.setDashboardOutputSizeQpsSum(24D);
        dirtyMinute.setDashboardOutputSizeQpsCount(12L);
        MeasurementServiceV2Impl service = measurementServiceWithRows(List.of(hourly, dirtyMinute), new ArrayList<>());

        TaskMetricsTrendVo result = service.aggregateTaskMetricsByTaskIdsV2(
                List.of("task-dirty"), startAt, endAt, "node-a");

        assertEquals(Arrays.asList(25D, null), result.getOutputQps());
        assertEquals(Arrays.asList(2.5D, null), result.getOutputSizeQps());
        assertEquals("partial", result.getCoverage());
    }

    @Test
    void dirtyMinutesAcrossHourBoundaryUseBoundedWindowQueries() throws Exception {
        String taskId = "task-dirty-hour-boundary";
        long oneHour = 3_600_000L;
        MeasurementEntity hourly = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                List.of(metricSample(oneHour - 120_000L, 1D), metricSample(oneHour - 60_000L, 2D),
                        metricSample(oneHour, 3D)));
        hourly.setDashboardMinuteSummaries(List.of(
                dashboardMinuteSummary(oneHour - 120_000L, 12D, 12L, 1.2D, 12L),
                dashboardMinuteSummary(oneHour - 60_000L, 24D, 12L, 2.4D, 12L),
                dashboardMinuteSummary(oneHour, 36D, 12L, 3.6D, 12L)));
        MeasurementEntity lateBeforeBoundary = dirtyMinuteEntity(taskId, "node-a", oneHour - 120_000L, 480D, 12L);
        MeasurementEntity lateBeforeBoundaryEnd = dirtyMinuteEntity(taskId, "node-a", oneHour - 60_000L, 720D, 12L);
        MeasurementEntity lateAfterBoundary = dirtyMinuteEntity(taskId, "node-a", oneHour, 1_200D, 12L);
        List<Query> queries = new ArrayList<>();
        MeasurementServiceV2Impl service = measurementServiceWithRows(
                List.of(hourly, lateBeforeBoundary, lateBeforeBoundaryEnd, lateAfterBoundary), new ArrayList<>(), queries);

        TaskMetricsTrendVo result = service.aggregateTaskMetricsByTaskIdsV2(
                List.of(taskId), 0L, 2L * oneHour, "node-a");

        assertEquals(Arrays.asList(50D, 100D), result.getOutputQps());
        List<Query> dirtyQueries = queries.stream()
                .filter(query -> query.getQueryObject().containsKey(MeasurementEntity.FIELD_DASHBOARD_ROLLUP_DIRTY))
                .collect(Collectors.toList());
        assertEquals(1, dirtyQueries.size());
        for (Query query : dirtyQueries) {
            Document dateRange = (Document) query.getQueryObject().get(MetricCons.F_DATE);
            long lower = ((Date) dateRange.get("$gte")).getTime();
            long upper = ((Date) dateRange.get("$lt")).getTime();
            assertTrue(upper - lower <= MeasurementServiceV2Impl.HOURLY_TREND_WINDOW_MILLIS);
        }
    }

    @Test
    void sevenDayHourlyTrendUsesFourteenBoundedQueries() throws Exception {
        long sevenDays = 7L * 24L * 3_600_000L;
        List<Query> queries = new ArrayList<>();
        MeasurementServiceV2Impl service = measurementServiceWithRows(List.of(), new ArrayList<>(), queries);

        service.aggregateTaskMetricsByTaskIdsV2(List.of("task-seven-days"), 0L, sevenDays, "node-a");

        assertEquals(14, queries.size());
        assertEquals(7, queries.stream().filter(query ->
                query.getQueryObject().containsKey(MeasurementEntity.FIELD_DASHBOARD_ROLLUP_DIRTY)).count());
    }

    @Test
    void widerHourlyWindowsMatchOneHourWindowsAcrossDirtyAndConflictingMinutes() throws Exception {
        long hour = 3_600_000L;
        long boundary = 24L * hour;
        String taskId = "task-window-comparison";
        MeasurementEntity before = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                List.of(metricSample(boundary - 60_000L, 10D)));
        before.setDate(new Date(boundary - hour));
        MeasurementEntity after = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                List.of(metricSample(boundary, 20D), metricSample(boundary + 60_000L, 30D),
                        metricSample(boundary + hour - 60_000L, 40D)));
        after.setDate(new Date(boundary));
        MeasurementEntity conflict = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                List.of(metricSample(boundary + 60_000L, 31D)));
        conflict.setDate(new Date(boundary));
        MeasurementEntity next = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR,
                List.of(metricSample(boundary + hour, 50D)));
        next.setDate(new Date(boundary + hour));
        List<MeasurementEntity> rows = List.of(before, after, conflict, next,
                dirtyMinuteEntity(taskId, "node-a", boundary - 60_000L, 600D, 12L),
                dirtyMinuteEntity(taskId, "node-a", boundary, 840D, 12L),
                dirtyMinuteEntity(taskId, "node-a", boundary + hour, 1_080D, 12L));

        for (long startAt : List.of(0L, 30L * 60_000L)) {
            long endAt = 26L * hour + (startAt == 0L ? 0L : 30L * 60_000L);
            MeasurementServiceV2Impl oneHourService = spy(measurementServiceWithRows(rows, new ArrayList<>()));
            doReturn(hour).when(oneHourService).hourlyTrendWindowMillis();
            MeasurementServiceV2Impl wideService = measurementServiceWithRows(rows, new ArrayList<>());

            TaskMetricsTrendVo oneHourResult = oneHourService.aggregateTaskMetricsByTaskIdsV2(
                    List.of(taskId), startAt, endAt, "node-a");
            TaskMetricsTrendVo wideResult = wideService.aggregateTaskMetricsByTaskIdsV2(
                    List.of(taskId), startAt, endAt, "node-a");

            assertEquals(oneHourResult.getOutputQps(), wideResult.getOutputQps());
            assertEquals(oneHourResult.getOutputSizeQps(), wideResult.getOutputSizeQps());
            assertEquals(oneHourResult.getCoverage(), wideResult.getCoverage());
            assertEquals(oneHourResult, wideResult);
            assertTrue(wideResult.getOutputQps().stream().anyMatch(Objects::nonNull));
        }
    }

    @Test
    void attributedMinutePipelineReplacesSameTimestampAndRecomputesRetainedFieldCounts() {
        Map<String, String> tags = Map.of("type", "task", "taskId", "task-pipeline",
                "taskRecordId", "record-1", "physicalNodeId", "node-a");
        Date sampleDate = new Date(30_000L);
        Map<String, Object> sample = new HashMap<>();
        sample.put(Sample.FIELD_DATE, sampleDate);
        sample.put(Sample.FIELD_VALUES, Map.of(MetricCons.SS.VS.F_OUTPUT_QPS, 0D));

        List<org.bson.conversions.Bson> pipeline = MeasurementServiceV2Impl.attributedTaskMetricUpdatePipeline(
                tags, new Date(0L), sample, sampleDate, true);
        String pipelineText = pipeline.toString();

        assertEquals(2, pipeline.size());
        assertTrue(pipelineText.contains("$gt"));
        assertTrue(pipelineText.contains("$lt"));
        assertTrue(pipelineText.contains("$concatArrays"));
        assertTrue(pipelineText.contains("$slice"));
        assertTrue(pipelineText.contains("$literal"));
        assertTrue(pipelineText.contains(MeasurementEntity.FIELD_DASHBOARD_OUTPUT_QPS_COUNT));
        assertTrue(pipelineText.contains(MeasurementEntity.FIELD_DASHBOARD_OUTPUT_SIZE_QPS_COUNT));
        assertEquals(MeasurementServiceV2Impl.attributedTaskMinuteDocumentId(tags, new Date(0L)),
                MeasurementServiceV2Impl.attributedTaskMinuteDocumentId(tags, new Date(0L)));
        assertNotEquals(MeasurementServiceV2Impl.attributedTaskMinuteDocumentId(tags, new Date(0L)),
                MeasurementServiceV2Impl.attributedTaskMinuteDocumentId(Map.of("type", "task", "taskId", "task-pipeline",
                        "taskRecordId", "record-1", "physicalNodeId", "node-b"), new Date(0L)));
    }

    @Test
    void aggregateV2UsesMinuteEdgesAndExcludesSamplesOutsideTheRequestedWindow() throws Exception {
        long startAt = 30L * 60L * 1000L;
        long endAt = 2L * 60L * 60L * 1000L;
        String taskId = "task-2";
        MeasurementEntity edgeMinutes = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_MINUTE, List.of(
                metricSample(startAt - 60_000L, 999D),
                metricSample(startAt, 20D),
                metricSample(59L * 60L * 1000L, 30D),
                metricSample(60L * 60L * 1000L, 999D)));
        MeasurementEntity fullHour = taskMetricsEntity(taskId, "node-a", Granularity.GRANULARITY_HOUR, List.of(
                metricSample(60L * 60L * 1000L, 50D),
                metricSample(endAt, 999D)));
        List<String> queriedGranularities = new ArrayList<>();
        MeasurementServiceV2Impl service = measurementServiceWithRows(List.of(edgeMinutes, fullHour), queriedGranularities);

        TaskMetricsTrendVo result = service.aggregateTaskMetricsByTaskIdsV2(List.of(taskId), startAt, endAt, null);

        assertEquals(List.of(25D, 50D), result.getOutputQps());
        assertTrue(queriedGranularities.contains(Granularity.GRANULARITY_MINUTE));
        assertTrue(queriedGranularities.contains(Granularity.GRANULARITY_HOUR));
    }

    private MeasurementServiceV2Impl measurementServiceWithRows(List<MeasurementEntity> rows, List<String> queriedGranularities) throws Exception {
        return measurementServiceWithRows(rows, queriedGranularities, new ArrayList<>());
    }

    private MeasurementServiceV2Impl measurementServiceWithRows(List<MeasurementEntity> rows,
                                                                 List<String> queriedGranularities,
                                                                 List<Query> queries) throws Exception {
        MongoTemplate mongo = mock(MongoTemplate.class);
        when(mongo.stream(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                .thenAnswer(invocation -> {
                    Query query = invocation.getArgument(0);
                    queries.add(query);
                    String granularity = query.getQueryObject().getString(MetricCons.F_GRANULARITY);
                    // Dirty-minute overlay probes are bounded correction reads, not base sample reads.
                    if (!query.getQueryObject().containsKey(MeasurementEntity.FIELD_DASHBOARD_ROLLUP_DIRTY)) {
                        queriedGranularities.add(granularity);
                    }
                    List<MeasurementEntity> matches = rows.stream()
                            .filter(row -> queryMatchesMeasurement(row, query, granularity))
                            .collect(Collectors.toList());
                    Object sortDirection = query.getSortObject().get(MetricCons.F_DATE);
                    if (sortDirection instanceof Number direction && direction.intValue() < 0) {
                        matches.sort(Comparator.comparing(MeasurementEntity::getDate).reversed());
                    } else if (sortDirection instanceof Number ascendingDirection && ascendingDirection.intValue() > 0) {
                        matches.sort(Comparator.comparing(MeasurementEntity::getDate));
                    }
                    return matches.stream();
                });
        return new MeasurementServiceV2Impl(CompletableFuture.completedFuture(mongo),
                mock(MetadataInstancesService.class), mock(TaskService.class));
    }

    private boolean queryMatchesMeasurement(MeasurementEntity row, Query query, String granularity) {
        if (!granularity.equals(row.getGranularity()) || row.getDate() == null || row.getTags() == null) {
            return false;
        }
        Document queryObject = query.getQueryObject();
        Object taskFilter = queryObject.get(MeasurementServiceV2Impl.PATH_TAGS_TASK_ID);
        if (taskFilter instanceof Document condition && condition.get("$in") instanceof List<?> taskIds
                && !taskIds.contains(row.getTags().get(MetricCons.Tags.F_TASK_ID))) {
            return false;
        }
        Object taskRecordFilter = queryObject.get(MeasurementServiceV2Impl.PATH_TAGS_TASK_RECORD_ID);
        if (taskRecordFilter != null
                && !Objects.equals(taskRecordFilter, row.getTags().get(MetricCons.Tags.F_TASK_RECORD_ID))) {
            return false;
        }
        Object dateFilter = queryObject.get(MetricCons.F_DATE);
        if (!(dateFilter instanceof Document range)) {
            return true;
        }
        long timestamp = row.getDate().getTime();
        Object gte = range.get("$gte");
        Object gt = range.get("$gt");
        Object lt = range.get("$lt");
        Object lte = range.get("$lte");
        return (gte == null || timestamp >= ((Date) gte).getTime())
                && (gt == null || timestamp > ((Date) gt).getTime())
                && (lt == null || timestamp < ((Date) lt).getTime())
                && (lte == null || timestamp <= ((Date) lte).getTime());
    }

    private MeasurementQueryParam taskCounterRecoveryQuery(String taskId, String taskRecordId,
                                                           long startAt, long endAt) {
        MeasurementQueryParam query = new MeasurementQueryParam();
        query.setStartAt(startAt);
        query.setEndAt(endAt);
        MeasurementQueryParam.MeasurementQuerySample sample = new MeasurementQueryParam.MeasurementQuerySample();
        sample.setType(MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_INSTANT);
        sample.setFields(coreCounterFields());
        Map<String, String> tags = new HashMap<>();
        tags.put(MetricCons.Tags.F_TYPE, MetricCons.SampleType.TASK.code());
        tags.put(MetricCons.Tags.F_TASK_ID, taskId);
        tags.put(MetricCons.Tags.F_TASK_RECORD_ID, taskRecordId);
        sample.setTags(tags);
        query.setSamples(Map.of("retriever", sample));
        return query;
    }

    private Sample counterSample(long timestamp, long value) {
        Sample sample = new Sample();
        sample.setDate(new Date(timestamp));
        Map<String, Number> values = new HashMap<>();
        for (String field : coreCounterFields()) {
            values.put(field, 0L);
        }
        values.put(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL, value);
        sample.setVs(values);
        return sample;
    }

    private List<String> coreCounterFields() {
        return List.of(MetricCons.SS.VS.F_INPUT_DDL_TOTAL, MetricCons.SS.VS.F_INPUT_INSERT_TOTAL,
                MetricCons.SS.VS.F_INPUT_UPDATE_TOTAL, MetricCons.SS.VS.F_INPUT_DELETE_TOTAL,
                MetricCons.SS.VS.F_INPUT_OTHERS_TOTAL, MetricCons.SS.VS.F_OUTPUT_DDL_TOTAL,
                MetricCons.SS.VS.F_OUTPUT_INSERT_TOTAL, MetricCons.SS.VS.F_OUTPUT_UPDATE_TOTAL,
                MetricCons.SS.VS.F_OUTPUT_DELETE_TOTAL, MetricCons.SS.VS.F_OUTPUT_OTHERS_TOTAL);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> onlyRecoveryItem(Object response) {
        List<Map<String, Object>> items = recoveryItems(response);
        assertEquals(1, items.size());
        return items.get(0);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> recoveryItems(Object response) {
        Map<String, Object> envelope = (Map<String, Object>) response;
        Map<String, List<Map<String, Object>>> samples = (Map<String, List<Map<String, Object>>>) envelope.get("samples");
        return samples.get("retriever");
    }

    private MeasurementEntity dirtyMinuteEntity(String taskId, String physicalNodeId, long timestamp,
                                                double qpsSum, long qpsCount) {
        MeasurementEntity entity = taskMetricsEntity(taskId, physicalNodeId, Granularity.GRANULARITY_MINUTE,
                List.of(metricSample(timestamp, qpsSum / qpsCount)));
        entity.setDashboardRollupDirty(true);
        entity.setDashboardOutputQpsSum(qpsSum);
        entity.setDashboardOutputQpsCount(qpsCount);
        entity.setDashboardOutputSizeQpsSum(qpsSum / 10D);
        entity.setDashboardOutputSizeQpsCount(qpsCount);
        return entity;
    }

    private MeasurementEntity taskMetricsEntity(String taskId, String physicalNodeId, String granularity, List<Sample> samples) {
        MeasurementEntity entity = new MeasurementEntity();
        entity.setGranularity(granularity);
        entity.setDate(samples.isEmpty() ? new Date(0L) : samples.get(0).getDate());
        Map<String, String> tags = new HashMap<>();
        tags.put(MetricCons.Tags.F_TYPE, MetricCons.SampleType.TASK.code());
        tags.put(MetricCons.Tags.F_TASK_ID, taskId);
        tags.put("physicalNodeId", physicalNodeId);
        entity.setTags(tags);
        entity.setSamples(samples);
        return entity;
    }

    private Sample metricSample(long timestamp, double outputQps) {
        Sample sample = new Sample();
        sample.setDate(new Date(timestamp));
        sample.setVs(Map.of(MetricCons.SS.VS.F_OUTPUT_QPS, outputQps,
                MetricCons.SS.VS.F_OUTPUT_SIZE_QPS, outputQps / 10D));
        return sample;
    }

    private List<Sample> samplesForMinuteRange(long startAt, int minuteCount, double outputQps) {
        List<Sample> samples = new ArrayList<>();
        for (int minute = 0; minute < minuteCount; minute++) {
            samples.add(metricSample(startAt + minute * 60_000L, outputQps));
        }
        return samples;
    }

    private MeasurementEntity.DashboardMinuteSummary dashboardMinuteSummary(long timestamp, double qpsSum,
                                                                            long qpsCount, double sizeSum,
                                                                            long sizeCount) {
        MeasurementEntity.DashboardMinuteSummary summary = new MeasurementEntity.DashboardMinuteSummary();
        summary.setDate(new Date(timestamp));
        summary.setOutputQpsSum(qpsSum);
        summary.setOutputQpsCount(qpsCount);
        summary.setOutputSizeQpsSum(sizeSum);
        summary.setOutputSizeQpsCount(sizeCount);
        return summary;
    }

    @SuppressWarnings("unchecked")
    private Map<String, MeasurementServiceV2Impl.MetricAccumulator> metricAccumulators(
            MeasurementServiceV2Impl.TaskMetricBucket bucket) {
        return (Map<String, MeasurementServiceV2Impl.MetricAccumulator>) ReflectionTestUtils.getField(bucket, "byPhysicalNode");
    }

    @Nested
    class FixSyncRateTest {
        List<TableSyncStaticVo> tableSyncStaticVos;
        TableSyncStaticVo vo;
        TaskDto taskDto;
        @BeforeEach
        void init() {
            tableSyncStaticVos = new ArrayList<>();
            taskDto = mock(TaskDto.class);
            when(taskDto.getStatus()).thenReturn(KeyWords.COMPLETE);
            when(taskDto.getType()).thenReturn(KeyWords.INITIAL_SYNC);
            vo = mock(TableSyncStaticVo.class);
            tableSyncStaticVos.add(vo);
            doNothing().when(vo).setSyncRate(BigDecimal.ONE);
            doNothing().when(vo).setFullSyncStatus(KeyWords.DONE);

            doCallRealMethod().when(measurementServiceV2).fixSyncRate(tableSyncStaticVos, taskDto);
        }

        @Test
        void testNormal() {
            measurementServiceV2.fixSyncRate(tableSyncStaticVos, taskDto);
            verify(taskDto).getStatus();
            verify(taskDto).getType();
            verify(vo).setFullSyncStatus(KeyWords.DONE);
            verify(vo).setSyncRate(BigDecimal.ONE);
        }
        @Test
        void testNotINITIAL_SYNC() {
            when(taskDto.getType()).thenReturn("000");
            measurementServiceV2.fixSyncRate(tableSyncStaticVos, taskDto);
            verify(taskDto).getStatus();
            verify(taskDto).getType();
            verify(vo, times(0)).setFullSyncStatus(KeyWords.DONE);
            verify(vo, times(0)).setSyncRate(BigDecimal.ONE);
        }
        @Test
        void testNotFINSH() {
            when(taskDto.getStatus()).thenReturn("cdj");
            measurementServiceV2.fixSyncRate(tableSyncStaticVos, taskDto);
            verify(taskDto).getStatus();
            verify(taskDto).getType();
            verify(vo, times(0)).setFullSyncStatus(KeyWords.DONE);
            verify(vo, times(0)).setSyncRate(BigDecimal.ONE);
        }
    }

    @Nested
    class FindLastMinuteByTaskIdTest {
        MongoTemplate mongoOperations;

        @BeforeEach
        void init() {
            mongoOperations = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(measurementServiceV2, "mongoOperations", mongoOperations);
            when(measurementServiceV2.findLastMinuteByTaskId(anyString())).thenCallRealMethod();
            when(mongoOperations.findOne(any(Query.class), any(Class.class), anyString())).thenReturn(mock(MeasurementEntity.class));
        }

        @Test
        void testNormal() {
            measurementServiceV2.findLastMinuteByTaskId("");
            verify(mongoOperations).findOne(any(Query.class), any(Class.class), anyString());
        }

        @Test
        void testOne() {
            measurementServiceV2.findLastMinuteByTaskId(new ObjectId().toHexString());
            verify(mongoOperations).findOne(any(Query.class), any(Class.class), anyString());
        }
        @Test
        void testOneHasResult() {
            measurementServiceV2.findLastMinuteByTaskId(new ObjectId().toHexString());
            verify(mongoOperations).findOne(any(Query.class), any(Class.class), anyString());
        }
    }

    @Nested
    class FindLastMinuteSamplesByTaskIdsTest {
        private MongoTemplate mongoOperations;

        @BeforeEach
        void init() {
            mongoOperations = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(measurementServiceV2, "mongoOperations", mongoOperations);
            when(measurementServiceV2.findLastMinuteSamplesByTaskIds(any())).thenCallRealMethod();
        }

        @Test
        @DisplayName("空列表返回空Map")
        void testEmptyTaskIds() {
            Map<String, Sample> result = measurementServiceV2.findLastMinuteSamplesByTaskIds(new ArrayList<>());
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("null列表返回空Map")
        void testNullTaskIds() {
            Map<String, Sample> result = measurementServiceV2.findLastMinuteSamplesByTaskIds(null);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("跳过空白taskId")
        void testBlankTaskIdSkipped() {
            Map<String, Sample> result = measurementServiceV2.findLastMinuteSamplesByTaskIds(List.of("", "  "));
            assertTrue(result.isEmpty());
            verifyNoInteractions(mongoOperations);
        }

        @Test
        @DisplayName("无匹配实体时返回空Map")
        void testEntityNull() {
            mockAggregationResults(List.of());
            Map<String, Sample> result = measurementServiceV2.findLastMinuteSamplesByTaskIds(List.of("task-1"));
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("聚合查询限制最近五分钟并允许磁盘排序")
        void testAggregationLimitsTimeRangeAndAllowsDiskUse() {
            mockAggregationResults(List.of());
            long beforeQuery = System.currentTimeMillis();

            measurementServiceV2.findLastMinuteSamplesByTaskIds(List.of("task-1"));

            long afterQuery = System.currentTimeMillis();
            ArgumentCaptor<Aggregation> aggregationCaptor = ArgumentCaptor.forClass(Aggregation.class);
            verify(mongoOperations).aggregate(aggregationCaptor.capture(), eq(MeasurementEntity.COLLECTION_NAME),
                    eq(MeasurementEntity.class));
            Aggregation aggregation = aggregationCaptor.getValue();
            Document match = aggregation.toPipeline(Aggregation.DEFAULT_CONTEXT).get(0).get("$match", Document.class);
            Document dateRange = match.get(MetricCons.F_DATE, Document.class);
            Date queryStart = dateRange.getDate("$gte");
            Date queryEnd = dateRange.getDate("$lte");

            assertEquals(5L * 60L * 1000L, queryEnd.getTime() - queryStart.getTime());
            assertTrue(queryEnd.getTime() >= beforeQuery && queryEnd.getTime() <= afterQuery);
            assertTrue(aggregation.getOptions().isAllowDiskUse());
        }

        @Test
        @DisplayName("聚合查询去重并过滤空白taskId")
        void testTaskIdsAreNormalizedBeforeAggregation() {
            mockAggregationResults(List.of());

            measurementServiceV2.findLastMinuteSamplesByTaskIds(List.of("task-1", "task-1", " ", "task-2"));

            ArgumentCaptor<Aggregation> aggregationCaptor = ArgumentCaptor.forClass(Aggregation.class);
            verify(mongoOperations).aggregate(aggregationCaptor.capture(), eq(MeasurementEntity.COLLECTION_NAME),
                    eq(MeasurementEntity.class));
            Document match = aggregationCaptor.getValue().toPipeline(Aggregation.DEFAULT_CONTEXT)
                    .get(0).get("$match", Document.class);
            assertEquals(List.of("task-1", "task-2"),
                    match.get(MeasurementServiceV2Impl.PATH_TAGS_TASK_ID, Document.class).getList("$in", String.class));
        }

        @Test
        @DisplayName("entity的samples为空时跳过")
        void testEntityEmptySamples() {
            MeasurementEntity entity = new MeasurementEntity();
            entity.setTags(Map.of("taskId", "task-1"));
            entity.setSamples(new ArrayList<>());
            mockAggregationResults(List.of(entity));
            Map<String, Sample> result = measurementServiceV2.findLastMinuteSamplesByTaskIds(List.of("task-1"));
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("正常返回最新Sample")
        void testReturnLatestSample() {
            MeasurementEntity entity = new MeasurementEntity();
            Sample older = new Sample();
            older.setDate(new Date(1000L));
            older.setVs(new HashMap<>());
            Sample newer = new Sample();
            newer.setDate(new Date(2000L));
            newer.setVs(new HashMap<>());
            entity.setSamples(List.of(older, newer));
            entity.setTags(Map.of("taskId", "task-1"));
            mockAggregationResults(List.of(entity));

            Map<String, Sample> result = measurementServiceV2.findLastMinuteSamplesByTaskIds(List.of("task-1"));
            assertEquals(1, result.size());
            assertSame(newer, result.get("task-1"));
        }

        @Test
        @DisplayName("多个taskId正常聚合")
        void testMultipleTaskIds() {
            MeasurementEntity entity1 = new MeasurementEntity();
            Sample s1 = new Sample();
            s1.setDate(new Date(1000L));
            s1.setVs(new HashMap<>());
            entity1.setSamples(List.of(s1));
            entity1.setTags(Map.of("taskId", "t1"));

            MeasurementEntity entity3 = new MeasurementEntity();
            Sample s3 = new Sample();
            s3.setDate(new Date(3000L));
            s3.setVs(new HashMap<>());
            entity3.setSamples(List.of(s3));
            entity3.setTags(Map.of("taskId", "t3"));
            mockAggregationResults(List.of(entity1, entity3));

            Map<String, Sample> result = measurementServiceV2.findLastMinuteSamplesByTaskIds(List.of("t1", "t2", "t3"));
            assertEquals(2, result.size());
            assertSame(s1, result.get("t1"));
            assertSame(s3, result.get("t3"));
        }

        @Test
        @DisplayName("samples中有null和date为null的条目被过滤")
        void testFilterNullSamplesAndNullDate() {
            MeasurementEntity entity = new MeasurementEntity();
            Sample validSample = new Sample();
            validSample.setDate(new Date(5000L));
            validSample.setVs(new HashMap<>());
            Sample nullDateSample = new Sample();
            nullDateSample.setDate(null);
            List<Sample> samples = new ArrayList<>();
            samples.add(null);
            samples.add(nullDateSample);
            samples.add(validSample);
            entity.setSamples(samples);
            entity.setTags(Map.of("taskId", "task-1"));
            mockAggregationResults(List.of(entity));

            Map<String, Sample> result = measurementServiceV2.findLastMinuteSamplesByTaskIds(List.of("task-1"));
            assertEquals(1, result.size());
            assertSame(validSample, result.get("task-1"));
        }

        @Test
        @DisplayName("超过单批上限时分批聚合")
        void testTaskIdsAreBatched() {
            AtomicInteger batchCount = new AtomicInteger();
            when(mongoOperations.aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(MeasurementEntity.class)))
                    .thenAnswer(invocation -> {
                        int batch = batchCount.incrementAndGet();
                        return aggregationResults(List.of(entityWithSample("task-" + batch, batch)));
                    });

            List<String> taskIds = new ArrayList<>();
            for (int index = 1; index <= 101; index++) {
                taskIds.add("task-" + index);
            }

            Map<String, Sample> result = measurementServiceV2.findLastMinuteSamplesByTaskIds(taskIds);

            assertEquals(2, batchCount.get());
            assertEquals(2, result.size());
            verify(measurementServiceV2, never()).findLastMinuteByTaskId(anyString());
        }

        private void mockAggregationResults(List<MeasurementEntity> entities) {
            when(mongoOperations.aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(MeasurementEntity.class)))
                    .thenReturn(aggregationResults(entities));
        }

        private AggregationResults<MeasurementEntity> aggregationResults(List<MeasurementEntity> entities) {
            return new AggregationResults<>(entities, new org.bson.Document());
        }

        private MeasurementEntity entityWithSample(String taskId, long timestamp) {
            MeasurementEntity entity = new MeasurementEntity();
            entity.setTags(Map.of("taskId", taskId));
            Sample sample = new Sample();
            sample.setDate(new Date(timestamp));
            sample.setVs(new HashMap<>());
            entity.setSamples(List.of(sample));
            return entity;
        }
    }

    @Nested
    class AggregateTaskMetricsByTaskIdsTest {
        @BeforeEach
        void init() {
            mongoOperations = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(measurementServiceV2, "mongoOperations", mongoOperations);
            when(measurementServiceV2.aggregateTaskMetricsByTaskIds(any(), anyLong(), anyLong())).thenCallRealMethod();
        }

        @Test
        void testOneHourWindowUsesMinuteMeasurements() {
            long endAt = 1_710_000_000_000L;
            long startAt = endAt - 60L * 60L * 1000L;
            MeasurementEntity entity = new MeasurementEntity();
            entity.setGranularity(Granularity.GRANULARITY_MINUTE);
            entity.setTags(Map.of("taskId", "task-1"));
            entity.setSamples(List.of(sample(new Date(endAt - 60_000L), 120D, 12D)));
            when(mongoOperations.stream(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenAnswer(invocation -> {
                        Query query = invocation.getArgument(0);
                        return closeableIterator(query.getQueryObject().toJson().contains("\"grnty\": \"minute\"")
                                ? List.of(entity)
                                : new ArrayList<>());
                    });

            TaskMetricsTrendVo result = measurementServiceV2.aggregateTaskMetricsByTaskIds(List.of("task1"), startAt, endAt);

            assertTrue(result.getOutputQps().stream().anyMatch(value -> value != null && value > 0D));
            assertEquals(120D, result.getOutputQps().get(result.getOutputQps().size() - 1));
            assertEquals(12D, result.getOutputSizeQps().get(result.getOutputSizeQps().size() - 1));
        }

        @Test
        void testOneDayWindowUsesHourMeasurements() {
            long endAt = 1_710_000_000_000L;
            long startAt = endAt - 24L * 60L * 60L * 1000L;
            MeasurementEntity entity = new MeasurementEntity();
            entity.setGranularity(Granularity.GRANULARITY_HOUR);
            entity.setTags(Map.of("taskId", "task-1"));
            entity.setSamples(List.of(sample(new Date(endAt - 60L * 60L * 1000L), 300D, 30D)));
            when(mongoOperations.stream(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenAnswer(invocation -> {
                        Query query = invocation.getArgument(0);
                        return closeableIterator(query.getQueryObject().toJson().contains("\"grnty\": \"hour\"")
                                ? List.of(entity)
                                : new ArrayList<>());
                    });

            TaskMetricsTrendVo result = measurementServiceV2.aggregateTaskMetricsByTaskIds(List.of("task1"), startAt, endAt);

            assertTrue(result.getOutputQps().stream().anyMatch(value -> value != null && value > 0D));
            assertEquals(300D, result.getOutputQps().get(result.getOutputQps().size() - 1));
            assertEquals(30D, result.getOutputSizeQps().get(result.getOutputSizeQps().size() - 1));
        }

        @Test
        void testAggregateTaskMetricsFiltersByTaskRecordId() {
            long endAt = 1_710_000_000_000L;
            long startAt = endAt - 60L * 60L * 1000L;
            MeasurementEntity matched = new MeasurementEntity();
            matched.setGranularity(Granularity.GRANULARITY_MINUTE);
            matched.setTags(Map.of("taskId", "task-1", "taskRecordId", "record-1"));
            matched.setSamples(List.of(sample(new Date(endAt - 60_000L), 120D, 12D)));
            MeasurementEntity stale = new MeasurementEntity();
            stale.setGranularity(Granularity.GRANULARITY_MINUTE);
            stale.setTags(Map.of("taskId", "task-1", "taskRecordId", "record-old"));
            stale.setSamples(List.of(sample(new Date(endAt - 60_000L), 900D, 90D)));
            when(mongoOperations.stream(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenAnswer(invocation -> {
                        Query query = invocation.getArgument(0);
                        String json = query.getQueryObject().toJson();
                        assertTrue(json.contains("\"grnty\": \"minute\""));
                        return closeableIterator(List.of(matched));
                    });

            TaskMetricsTrendVo result = measurementServiceV2.aggregateTaskMetricsByTaskIds(List.of("task1"), startAt, endAt);

            assertTrue(result.getOutputQps().stream().anyMatch(value -> value != null && value > 0D));
            assertEquals(120D, result.getOutputQps().get(result.getOutputQps().size() - 1));
            assertEquals(12D, result.getOutputSizeQps().get(result.getOutputSizeQps().size() - 1));
        }

        @Test
        @DisplayName("空taskIds返回空结果")
        void testEmptyTaskIds() {
            TaskMetricsTrendVo result = measurementServiceV2.aggregateTaskMetricsByTaskIds(new ArrayList<>(), 0L, 100L);
            assertTrue(result.getTs().isEmpty());
            assertTrue(result.getOutputQps().isEmpty());
            assertTrue(result.getOutputSizeQps().isEmpty());
        }

        @Test
        @DisplayName("null taskIds返回空结果")
        void testNullTaskIds() {
            TaskMetricsTrendVo result = measurementServiceV2.aggregateTaskMetricsByTaskIds(null, 0L, 100L);
            assertTrue(result.getTs().isEmpty());
        }

        @Test
        @DisplayName("startAt >= endAt返回空结果")
        void testStartGreaterOrEqualEnd() {
            TaskMetricsTrendVo result1 = measurementServiceV2.aggregateTaskMetricsByTaskIds(List.of("task-1"), 100L, 100L);
            assertTrue(result1.getTs().isEmpty());

            TaskMetricsTrendVo result2 = measurementServiceV2.aggregateTaskMetricsByTaskIds(List.of("task-1"), 200L, 100L);
            assertTrue(result2.getTs().isEmpty());
        }

        @Test
        @DisplayName("entity为null时跳过")
        void testNullEntitySkipped() {
            long endAt = 1_710_000_000_000L;
            long startAt = endAt - 60L * 60L * 1000L;
            List<MeasurementEntity> entities = new ArrayList<>();
            entities.add(null);
            when(mongoOperations.stream(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(closeableIterator(entities));

            TaskMetricsTrendVo result = measurementServiceV2.aggregateTaskMetricsByTaskIds(List.of("task-1"), startAt, endAt);
            assertFalse(result.getTs().isEmpty());
            assertTrue(result.getOutputQps().stream().allMatch(v -> v == 0D));
        }

        @Test
        @DisplayName("entity的tags为null时跳过")
        void testNullTagsSkipped() {
            long endAt = 1_710_000_000_000L;
            long startAt = endAt - 60L * 60L * 1000L;
            MeasurementEntity entity = new MeasurementEntity();
            entity.setTags(null);
            entity.setSamples(List.of(sample(new Date(endAt - 60_000L), 100D, 10D)));
            when(mongoOperations.stream(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(closeableIterator(List.of(entity)));

            TaskMetricsTrendVo result = measurementServiceV2.aggregateTaskMetricsByTaskIds(List.of("task-1"), startAt, endAt);
            assertTrue(result.getOutputQps().stream().allMatch(v -> v == 0D));
        }

        @Test
        @DisplayName("entity的taskId为空白时跳过")
        void testBlankTaskIdInTagsSkipped() {
            long endAt = 1_710_000_000_000L;
            long startAt = endAt - 60L * 60L * 1000L;
            MeasurementEntity entity = new MeasurementEntity();
            entity.setTags(Map.of("taskId", ""));
            entity.setSamples(List.of(sample(new Date(endAt - 60_000L), 100D, 10D)));
            when(mongoOperations.stream(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(closeableIterator(List.of(entity)));

            TaskMetricsTrendVo result = measurementServiceV2.aggregateTaskMetricsByTaskIds(List.of("task-1"), startAt, endAt);
            assertTrue(result.getOutputQps().stream().allMatch(v -> v == 0D));
        }

        @Test
        @DisplayName("无数据时所有bucket值为0")
        void testNoEntitiesAllZero() {
            long endAt = 1_710_000_000_000L;
            long startAt = endAt - 60L * 60L * 1000L;
            when(mongoOperations.stream(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(closeableIterator(new ArrayList<>()));

            TaskMetricsTrendVo result = measurementServiceV2.aggregateTaskMetricsByTaskIds(List.of("task-1"), startAt, endAt);
            assertFalse(result.getTs().isEmpty());
            assertTrue(result.getOutputQps().stream().allMatch(v -> v == 0D));
            assertTrue(result.getOutputSizeQps().stream().allMatch(v -> v == 0D));
        }

        @Test
        @DisplayName("多个taskId的值跨taskId求和")
        void testMultipleTaskIdsSummed() {
            long endAt = 1_710_000_000_000L;
            long startAt = endAt - 60L * 60L * 1000L;
            long sampleTs = endAt - 60_000L;

            MeasurementEntity entity1 = new MeasurementEntity();
            entity1.setTags(Map.of("taskId", "task-1"));
            entity1.setSamples(List.of(sample(new Date(sampleTs), 100D, 10D)));

            MeasurementEntity entity2 = new MeasurementEntity();
            entity2.setTags(Map.of("taskId", "task-2"));
            entity2.setSamples(List.of(sample(new Date(sampleTs), 200D, 20D)));

            when(mongoOperations.stream(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(closeableIterator(List.of(entity1, entity2)));

            TaskMetricsTrendVo result = measurementServiceV2.aggregateTaskMetricsByTaskIds(List.of("task-1", "task-2"), startAt, endAt);
            // 最后一个bucket应该是两个task的和: 100+200=300, 10+20=30
            Double lastQps = result.getOutputQps().get(result.getOutputQps().size() - 1);
            Double lastSizeQps = result.getOutputSizeQps().get(result.getOutputSizeQps().size() - 1);
            assertEquals(300D, lastQps);
            assertEquals(30D, lastSizeQps);
        }

        @Test
        @DisplayName("超过单批上限时分批查询并合并指标")
        void testTaskIdsAreBatched() {
            long endAt = 1_710_000_000_000L;
            long startAt = endAt - 60L * 60L * 1000L;
            AtomicInteger batchCount = new AtomicInteger();
            when(mongoOperations.stream(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenAnswer(invocation -> {
                        int batch = batchCount.incrementAndGet();
                        MeasurementEntity entity = new MeasurementEntity();
                        entity.setTags(Map.of("taskId", "task-" + batch));
                        entity.setSamples(List.of(sample(new Date(endAt - 60_000L), batch * 100D, batch * 10D)));
                        return closeableIterator(List.of(entity));
                    });
            List<String> taskIds = new ArrayList<>();
            for (int index = 1; index <= 101; index++) {
                taskIds.add("task-" + index);
            }

            TaskMetricsTrendVo result = measurementServiceV2.aggregateTaskMetricsByTaskIds(taskIds, startAt, endAt);

            assertEquals(2, batchCount.get());
            assertEquals(300D, result.getOutputQps().get(result.getOutputQps().size() - 1));
            assertEquals(30D, result.getOutputSizeQps().get(result.getOutputSizeQps().size() - 1));
        }

        private Sample sample(Date date, double outputQps, double outputSizeQps) {
            Sample sample = new Sample();
            sample.setDate(date);
            Map<String, Number> vs = new HashMap<>();
            vs.put("outputQps", outputQps);
            vs.put("outputSizeQps", outputSizeQps);
            sample.setVs(vs);
            return sample;
        }

        private Stream<MeasurementEntity> closeableIterator(List<MeasurementEntity> entities) {
            return entities.stream();
        }
    }

    @Nested
    class AddBulkAgentMeasurementTest{
        private MongoTemplate mongoOperations;
        private TaskService taskService;
        private Map<String, Long> taskDelayTimeMap;
        private SampleRequest sampleRequest;
        private List<SampleRequest> samples;
        @BeforeEach
        void beforeEach(){
            mongoOperations = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(measurementServiceV2,"mongoOperations",mongoOperations);
            when(mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME)).thenReturn(mock(BulkOperations.class));
            taskService = mock(TaskService.class);
            ReflectionTestUtils.setField(measurementServiceV2,"taskService",taskService);
            taskDelayTimeMap = new HashMap<>();
            ReflectionTestUtils.setField(measurementServiceV2,"taskDelayTimeMap",taskDelayTimeMap);
            samples = new ArrayList<>();
            sampleRequest = mock(SampleRequest.class);
            samples.add(sampleRequest);
            Map<String, String> tags = new HashMap<>();
            String id = "665d2b9b889245e73373cf49";
            tags.put("type","task");
            tags.put("taskId",id);
            when(sampleRequest.getTags()).thenReturn(tags);
        }
        @Test
        @DisplayName("test addBulkAgentMeasurement method normal")
        void test1(){
            Sample sample = new Sample();
            Date date = new Date();
            sample.setDate(date);
            Map vs = new HashMap();
            vs.put("replicateLag",111);
            sample.setVs(vs);
            when(sampleRequest.getSample()).thenReturn(sample);
            doCallRealMethod().when(measurementServiceV2).addAgentMeasurement(samples);
            measurementServiceV2.addAgentMeasurement(samples);
            verify(taskService).updateDelayTime(new ObjectId("665d2b9b889245e73373cf49"), 111L);
        }
        @Test
        @DisplayName("future attributed sample is clamped to the receive time instead of being dropped")
        void futureAttributedSampleIsClamped() {
            Map<String, String> attributedTags = new HashMap<>();
            attributedTags.put("type", "task");
            attributedTags.put("taskId", "665d2b9b889245e73373cf49");
            attributedTags.put("physicalNodeId", "node-a");
            when(sampleRequest.getTags()).thenReturn(attributedTags);
            Sample sample = new Sample();
            sample.setDate(new Date(System.currentTimeMillis() + 600_000L));
            sample.setVs(Map.of("outputQps", 5));
            when(sampleRequest.getSample()).thenReturn(sample);
            when(mongoOperations.getCollection(MeasurementEntity.COLLECTION_NAME))
                    .thenReturn(mock(com.mongodb.client.MongoCollection.class));
            doCallRealMethod().when(measurementServiceV2).addAgentMeasurement(samples);
            Date before = new Date();
            measurementServiceV2.addAgentMeasurement(samples);

            assertFalse(sample.getDate().after(new Date()), "future sample must be clamped");
            assertFalse(sample.getDate().before(new Date(before.getTime() - 2_000L)), "sample must keep near-current time");
        }

        @Test
        @DisplayName("test addBulkAgentMeasurement method when replicateLag is null")
        void test2(){
            Sample sample = new Sample();
            Date date = new Date();
            sample.setDate(date);
            Map vs = new HashMap();
            vs.put("replicateLag",null);
            sample.setVs(vs);
            when(sampleRequest.getSample()).thenReturn(sample);
            doCallRealMethod().when(measurementServiceV2).addAgentMeasurement(samples);
            measurementServiceV2.addAgentMeasurement(samples);
            verify(taskService, never()).updateDelayTime(any(ObjectId.class), anyLong());
        }
        @Test
        @DisplayName("test addBulkAgentMeasurement method when taskDelayTimeMap contains taskId")
        void test3(){
            Sample sample = new Sample();
            Date date = new Date();
            sample.setDate(date);
            Map vs = new HashMap();
            vs.put("replicateLag",111);
            sample.setVs(vs);
            when(sampleRequest.getSample()).thenReturn(sample);
            samples.add(sampleRequest);
            doCallRealMethod().when(measurementServiceV2).addAgentMeasurement(samples);
            measurementServiceV2.addAgentMeasurement(samples);
            verify(taskService, new Times(1)).updateDelayTime(new ObjectId("665d2b9b889245e73373cf49"), 111L);
        }
        @Test
        @DisplayName("test addBulkAgentMeasurement method when taskDelayTimeMap not contains taskId")
        void test4(){
            Sample sample = new Sample();
            Date date = new Date();
            sample.setDate(date);
            Map vs = new HashMap();
            vs.put("replicateLag",0);
            sample.setVs(vs);
            when(sampleRequest.getSample()).thenReturn(sample);
            SampleRequest sampleRequest1 = mock(SampleRequest.class);
            Map<String, String> tags = new HashMap<>();
            String id = "665d2b9b889245e73373cf50";
            tags.put("type","task");
            tags.put("taskId",id);
            when(sampleRequest1.getTags()).thenReturn(tags);
            Sample sample1 = new Sample();
            Date date1 = new Date();
            sample1.setDate(date1);
            Map vs1 = new HashMap();
            vs1.put("replicateLag",111);
            sample1.setVs(vs1);
            when(sampleRequest1.getSample()).thenReturn(sample1);
            samples.add(sampleRequest1);
            SampleRequest sampleRequest2 = mock(SampleRequest.class);
            when(sampleRequest2.getTags()).thenReturn(null);
            samples.add(sampleRequest2);
            doCallRealMethod().when(measurementServiceV2).addAgentMeasurement(samples);
            measurementServiceV2.addAgentMeasurement(samples);
            verify(taskService, new Times(1)).updateDelayTime(new ObjectId("665d2b9b889245e73373cf49"), 0L);
            verify(taskService, new Times(1)).updateDelayTime(new ObjectId("665d2b9b889245e73373cf50"), 111L);
            verify(taskService, new Times(2)).updateDelayTime(any(ObjectId.class),anyLong());
        }

        @Test
        @DisplayName("test addBulkAgentMeasurement reuses share cdc task delay when task is idle")
        void testReuseShareCdcDelayWhenTaskIdle(){
            String taskId = "665d2b9b889245e73373cf49";
            String logCollectorTaskId = "665d2b9b889245e73373cf51";
            long taskEventTime = System.currentTimeMillis() - 300_000L;
            long logCollectorEventTime = System.currentTimeMillis() - 10_000L;

            Sample sample = new Sample();
            sample.setDate(new Date());
            Map vs = new HashMap();
            vs.put("replicateLag", 300_000L);
            vs.put("currentEventTimestamp", taskEventTime);
            vs.put("inputQps", 0);
            vs.put("outputQps", 0);
            sample.setVs(vs);
            when(sampleRequest.getSample()).thenReturn(sample);

            Map<String, String> shareCdcTaskId = new HashMap<>();
            shareCdcTaskId.put("connectionId", logCollectorTaskId);
            TaskDto taskDto = new TaskDto();
            taskDto.setShareCdcEnable(true);
            taskDto.setSyncType(TaskDto.SYNC_TYPE_SYNC);
            taskDto.setShareCdcTaskId(shareCdcTaskId);
            TaskDto logCollectorTask = new TaskDto();
            logCollectorTask.setStatus(TaskDto.STATUS_RUNNING);
            logCollectorTask.setCurrentEventTimestamp(logCollectorEventTime);
            logCollectorTask.setDelayTime(10_000L);
            when(taskService.findByTaskId(eq(new ObjectId(taskId)), any(String[].class))).thenReturn(taskDto);
            when(taskService.findByTaskId(eq(new ObjectId(logCollectorTaskId)), any(String[].class))).thenReturn(logCollectorTask);

            doCallRealMethod().when(measurementServiceV2).addAgentMeasurement(samples);
            measurementServiceV2.addAgentMeasurement(samples);

            verify(taskService).updateDelayTime(new ObjectId(taskId), 10_000L);
            assertEquals(10_000L, sample.getVs().get("replicateLag"));
        }
        @Test
        @DisplayName("test addBulkAgentMeasurement method when type is table")
        void test5(){
            Map<String, String> tags = new HashMap<>();
            String id = "665d2b9b889245e73373cf49";
            tags.put("type","table");
            tags.put("taskId",id);
            when(sampleRequest.getTags()).thenReturn(tags);
            Sample sample = new Sample();
            Date date = new Date();
            sample.setDate(date);
            Map vs = new HashMap();
            vs.put("replicateLag",null);
            sample.setVs(vs);
            when(sampleRequest.getSample()).thenReturn(sample);
            doCallRealMethod().when(measurementServiceV2).addAgentMeasurement(samples);
            measurementServiceV2.addAgentMeasurement(samples);
            verify(taskService,new Times(0)).updateDelayTime(new ObjectId("665d2b9b889245e73373cf49"), 0L);
        }
    }
    @Nested
    class QueryTableMeasurementTest{
        private String taskId;
        private TableStatusInfoDto tableStatusInfoDto;
        private MongoTemplate mongoOperations;
        @BeforeEach
        void beforeEach(){
            taskId = "111";
            tableStatusInfoDto = new TableStatusInfoDto();
            mongoOperations = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(measurementServiceV2,"mongoOperations",mongoOperations);
        }
        @Test
        void testQueryTableMeasurement(){
            MeasurementEntity measurementEntity = new MeasurementEntity();
            List<Sample> samples = new ArrayList<>();
            Sample sample = new Sample();
            Map<String, Number> vs = new HashMap<>();
            vs.put("replicateLag",111);
            vs.put("currentEventTimestamp",1717413352);
            sample.setVs(vs);
            samples.add(sample);
            measurementEntity.setSamples(samples);
            when(mongoOperations.findOne(any(Query.class),any(Class.class),anyString())).thenReturn(measurementEntity);
            doCallRealMethod().when(measurementServiceV2).queryTableMeasurement(taskId,tableStatusInfoDto);
            measurementServiceV2.queryTableMeasurement(taskId,tableStatusInfoDto);
            assertEquals(111,tableStatusInfoDto.getCdcDelayTime());
            assertEquals(new Date(1717413352L),tableStatusInfoDto.getLastDataChangeTime());
        }
    }

    @Nested
    class GetTaskLastFiveMinutesQpsTest {
        String type = "task";
        String taskId = "test-task-id";
        String taskRecordId = "test-task-record-id";
        Map<String, String> tags;
        MongoTemplate mongoOperations;

        @BeforeEach
        void beforeEach() {
            tags = new HashMap<>();
            tags.put("type", type);
            tags.put("taskId", taskId);
            tags.put("taskRecordId", taskRecordId);

            mongoOperations = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(measurementServiceV2, "mongoOperations", mongoOperations);
        }

        @Test
        void testNotfound() {
            AggregationResults<Map> results = mock(AggregationResults.class);
            when(mongoOperations.aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(Map.class))).thenReturn(results);
            doCallRealMethod().when(measurementServiceV2).getTaskLastFiveMinutesQps(any());

            assertEquals(0D, measurementServiceV2.getTaskLastFiveMinutesQps(tags));
        }

        @Test
        void testNotParse() {
            Map<String, String> values = new HashMap<>();
            values.put("qps", "error");
            AggregationResults<Map> results = mock(AggregationResults.class);
            when(results.getMappedResults()).thenReturn(Arrays.asList(values));
            when(mongoOperations.aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(Map.class))).thenReturn(results);
            doCallRealMethod().when(measurementServiceV2).getTaskLastFiveMinutesQps(any());

            assertEquals(0D, measurementServiceV2.getTaskLastFiveMinutesQps(tags));
        }

        @Test
        void testSuccess() {
            Map<String, String> values = new HashMap<>();
            values.put("qps", "100");
            AggregationResults<Map> results = mock(AggregationResults.class);
            when(results.getMappedResults()).thenReturn(Arrays.asList(values));
            when(mongoOperations.aggregate(any(Aggregation.class), eq(MeasurementEntity.COLLECTION_NAME), eq(Map.class))).thenReturn(results);
            doCallRealMethod().when(measurementServiceV2).getTaskLastFiveMinutesQps(any());

            assertEquals(100, measurementServiceV2.getTaskLastFiveMinutesQps(tags));
        }
    }

    @Nested
    class Parse2SampleDataTest {
        String type = "task";
        String taskId = "test-task-id";
        String taskRecordId = "test-task-record-id";
        List<MeasurementEntity> entities;
        Map<String, Sample> data;
        long time;

        @BeforeEach
        void beforeEach() {
            entities = new ArrayList<>();
            data = new HashMap<>();
            time = System.currentTimeMillis();
        }

        private Sample addSample(List<Sample> samples, Consumer<Sample> consumer) {
            Sample sample = new Sample();
            sample.setDate(new Date());
            consumer.accept(sample);
            samples.add(sample);
            return sample;
        }

        private MeasurementEntity mockMeasurementEntity(String type, String taskId, String taskRecordId) {
            Map<String, String> tags = new HashMap<>();
            tags.put("type", type);
            tags.put("taskId", taskId);
            tags.put("taskRecordId", taskRecordId);
            MeasurementEntity entity = new MeasurementEntity();
            entity.setTags(tags);
            entity.setSamples(new ArrayList<>());
            return entity;
        }

        @Test
        void testDoubleSampleSameDate() {
            Date nowTime = new Date();
            MeasurementEntity entity = mockMeasurementEntity(type, taskId, taskRecordId);
            addSample(entity.getSamples(), sample -> sample.setDate(nowTime));
            Sample expected = addSample(entity.getSamples(), sample -> sample.setDate(nowTime));
            entities.add(entity);

            doCallRealMethod().when(measurementServiceV2).parse2SampleData(any(), eq(data), eq(time), eq(false));
            measurementServiceV2.parse2SampleData(entities, data, time, false);
            assertTrue(data.containsValue(expected));
        }

        @Test
        void testDoubleSampleUseLast() {
            long nowTime = System.currentTimeMillis();
            MeasurementEntity entity = mockMeasurementEntity(type, taskId, taskRecordId);
            Sample expected = addSample(entity.getSamples(), sample -> sample.setDate(new Date(nowTime)));
            addSample(entity.getSamples(), sample -> sample.setDate(new Date(nowTime + 1000)));
            entities.add(entity);

            doCallRealMethod().when(measurementServiceV2).parse2SampleData(any(), eq(data), eq(time), eq(false));
            measurementServiceV2.parse2SampleData(entities, data, time, false);
            assertTrue(data.containsValue(expected));
        }

        @Test
        void testOneSample() {
            MeasurementEntity entity = mockMeasurementEntity(type, taskId, taskRecordId);
            Sample expected = addSample(entity.getSamples(), sample -> {});
            entities.add(entity);

            doCallRealMethod().when(measurementServiceV2).parse2SampleData(any(), eq(data), eq(time), eq(false));
            measurementServiceV2.parse2SampleData(entities, data, time, false);
            assertTrue(data.containsValue(expected));
        }
        @Test
        void testOneSampleAsc() {
            MeasurementEntity entity = mockMeasurementEntity(type, taskId, taskRecordId);
            Sample expected = addSample(entity.getSamples(), sample -> {});
            entities.add(entity);

            doCallRealMethod().when(measurementServiceV2).parse2SampleData(any(), eq(data), eq(time), eq(true));
            measurementServiceV2.parse2SampleData(entities, data, time, true);
            assertTrue(data.containsValue(expected));
        }

    }

    @Nested
    class TestQuerySyncStatic {

        private MetadataInstancesService metadataInstancesService;
        private TaskService taskService;
        private MongoTemplate mongoTemplate;
        private MeasurementServiceV2Impl measurementService;

        @BeforeEach
        public void beforeEach() throws ExecutionException, InterruptedException {
            this.metadataInstancesService = mock(MetadataInstancesService.class);
            taskService = mock(TaskService.class);
            mongoTemplate = mock(MongoTemplate.class);
            CompletableFuture<MongoTemplate> future = new CompletableFuture<>();
            future.complete(mongoTemplate);
            measurementService = new MeasurementServiceV2Impl(future, metadataInstancesService, taskService);
        }

        @Test
        public void testNonData() {

            TableSyncStaticDto dto = new TableSyncStaticDto("taskRecordId", 1, 10, "test", null);
            UserDetail userDetail = mock(UserDetail.class);

            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(null);

            Page<TableSyncStaticVo> result = measurementService.querySyncStatic(dto, userDetail);

            Assertions.assertNotNull(result);
            Assertions.assertEquals(0, result.getTotal());
        }

        private TaskDto makeTaskDto() {
            TaskDto taskDto = new TaskDto();
            Graph<Node, Edge> graph = new Graph<>();

            graph.setNode("1", new DatabaseNode(){{
                setId("1");
            }});
            graph.setNode("2", new TableRenameProcessNode(){{
                setId("2");
            }});
            graph.setNode("3", new DatabaseNode(){{
                setId("3");
            }});
            graph.setEdge(new io.github.openlg.graphlib.Edge("1", "2"));
            graph.setEdge(new io.github.openlg.graphlib.Edge("2", "3"));

            DAG dag = new DAG(graph);
            taskDto.setDag(dag);
            taskDto.setId(new ObjectId());
            return taskDto;
        }

        @Test
        public void testNoDataInDB() {

            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(makeTaskDto());

            when(mongoTemplate.count(any(Query.class), anyString())).then(new Answer<Long>() {
                @Override
                public Long answer(InvocationOnMock invocation) throws Throwable {
                    Assertions.assertTrue(invocation.getArgument(0) instanceof Query);
                    Assertions.assertEquals(MeasurementEntity.COLLECTION_NAME, invocation.getArgument(1));
                    Query query = invocation.getArgument(0);
                    Assertions.assertTrue(query.getQueryObject().containsKey("tags.taskRecordId"));
                    Assertions.assertEquals("taskRecordId", query.getQueryObject().get("tags.taskRecordId"));
                    return 0L;
                }
            });

            TableSyncStaticDto dto = new TableSyncStaticDto("taskRecordId", 1, 10, "test", null);
            UserDetail userDetail = mock(UserDetail.class);

            Page<TableSyncStaticVo> result = measurementService.querySyncStatic(dto, userDetail);

            Assertions.assertNotNull(result);
            Assertions.assertEquals(0, result.getTotal());
        }

        @Test
        public void testNormalData() {
            TaskDto taskDto = makeTaskDto();
            taskDto.setSyncType("mi");
            taskDto.getDag().getTargetNode().getLast().setConnectionId("connectionId");

            AtomicInteger counter = new AtomicInteger();
            List<MetadataInstancesDto> metas = Stream.generate(() -> {
                int count = counter.incrementAndGet();
                MetadataInstancesDto metadata = new MetadataInstancesDto();
                if (count == 1) {
                    metadata.setName("test_target");
                    metadata.setAncestorsName("test");
                    metadata.setNodeId("3");
                } else {
                    metadata.setName(String.format("test_%d_target", count));
                    metadata.setAncestorsName(String.format("test_%d", count));
                    metadata.setNodeId("3");
                }
                return metadata;
            }).limit(5).collect(Collectors.toList());

            counter.set(0);
            List<TapSubPartitionTableInfo> subPartitionTableInfo = Stream.generate(() -> {
                TapSubPartitionTableInfo info = new TapSubPartitionTableInfo();
                info.setTableName(String.format("test_%d", counter.incrementAndGet()));
                return info;
            }).limit(4).collect(Collectors.toList());

            metas.get(0).setPartitionMasterTableId("test_1");
            metas.get(0).setPartitionInfo(new TapPartition());
            metas.get(0).getPartitionInfo().setSubPartitionTableInfo(subPartitionTableInfo);

            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(taskDto);
            when(mongoTemplate.count(any(Query.class), anyString())).thenReturn(10L);
            when(metadataInstancesService.findBySourceIdAndTableNameList(
                    eq("connectionId"), eq(null), any(UserDetail.class), eq(taskDto.getId().toHexString())))
                    .thenReturn(metas);

            List<MeasurementEntity> measurementEntities = new ArrayList<>();
            MeasurementEntity measure = new MeasurementEntity();
            measure.setGranularity("minute");
            measure.setTags(new HashMap<>());
            measure.getTags().put("table", "test_1");
            measure.getTags().put("taskId", taskDto.getId().toHexString());
            measure.getTags().put("taskRecordId", "657bb5dab37b9461bfc53eb2");
            measure.getTags().put("type", "table");
            measure.setDate(new Date());
            measure.setFirst(new Date(System.currentTimeMillis() - 1000 * 60));
            measure.setLast(new Date());
            measure.setSamples(Stream.generate(() -> {
                Sample sample = new Sample();
                sample.setDate(new Date());
                sample.setVs(new HashMap<>());
                sample.getVs().put("snapshotSyncRate", 1);
                sample.getVs().put("snapshotInsertRowTotal", 10);
                sample.getVs().put("snapshotRowTotal", 10);
                return sample;
            }).limit(2).collect(Collectors.toList()));

            measurementEntities.add(measure);

            when(mongoTemplate.find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(measurementEntities);

            TableSyncStaticDto dto = new TableSyncStaticDto("taskRecordId", 1, 10, "test", null);
            UserDetail userDetail = mock(UserDetail.class);

            Page<TableSyncStaticVo> result = measurementService.querySyncStatic(dto, userDetail);

            Assertions.assertNotNull(result);
            Assertions.assertEquals(10, result.getTotal());
        }
    }

    @Nested
    class QuerySyncStatic{
        MeasurementServiceV2Impl measurementServiceV2 = mock(MeasurementServiceV2Impl.class);
        MongoTemplate mongoTemplate;
        TaskService taskService;
        TableSyncStaticDto tableSyncStaticDto;
        @BeforeEach
        void before(){
            mongoTemplate = mock(MongoTemplate.class);
            taskService = mock(TaskService.class);
            ReflectionTestUtils.setField(measurementServiceV2,"mongoOperations",mongoTemplate);
            ReflectionTestUtils.setField(measurementServiceV2,"taskService",taskService);
            tableSyncStaticDto = new TableSyncStaticDto("test",1,20,"test", null);
        }
        @Test
        void test(){
            when(mongoTemplate.count(any(Query.class),anyString())).thenAnswer(invocationOnMock -> {
                Query query = invocationOnMock.getArgument(0);
                Assertions.assertTrue(query.getQueryObject().containsKey("tags.table"));
                return 0L;
            });
            doCallRealMethod().when(measurementServiceV2).querySyncStatic(any(),any());
            measurementServiceV2.querySyncStatic(tableSyncStaticDto,mock(UserDetail.class));
        }

        @Test
        void testQuerySyncStatic_SyncRate() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId("615c9f48f1d842b8b78bf9c8"));
            taskDto.setDag(mock(DAG.class));

            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(taskDto);
            when(mongoTemplate.count(any(Query.class), eq(MeasurementEntity.COLLECTION_NAME))).thenReturn(5L);
            MeasurementEntity measurementEntity = new MeasurementEntity();
            Map<String, String> tags = new HashMap<>();
            tags.put("table","table1");
            measurementEntity.setTags(tags);
            List<Sample> samples = new ArrayList<>();
            Sample sample = mock(Sample.class);
            samples.add(sample);
            Map<String, Number> vs = new HashMap<>();
            vs.put("snapshotRowTotal", -1);
            vs.put("snapshotInsertRowTotal", 100);
            when(sample.getVs()).thenReturn(vs);
            measurementEntity.setSamples(samples);
            when(mongoTemplate.find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(Collections.singletonList(measurementEntity));

            doReturn(mock(Criteria.class)).when(measurementServiceV2).createMinuteCriteria(anyString(), anyString(), anyString());
            doCallRealMethod().when(measurementServiceV2).querySyncStatic(any(),any());
            Page<TableSyncStaticVo> result = measurementServiceV2.querySyncStatic(tableSyncStaticDto,mock(UserDetail.class));

            assertEquals(5, result.getTotal());
            verify(taskService, times(1)).findOne(any(Query.class), any(UserDetail.class));
            verify(mongoTemplate, times(1)).count(any(Query.class), eq(MeasurementEntity.COLLECTION_NAME));
            verify(mongoTemplate, times(1)).find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME));
        }

        @Test
        void testQuerySyncStatic_SyncRateIsZero() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId("615c9f48f1d842b8b78bf9c8"));
            taskDto.setDag(mock(DAG.class));

            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(taskDto);
            when(mongoTemplate.count(any(Query.class), eq(MeasurementEntity.COLLECTION_NAME))).thenReturn(5L);
            MeasurementEntity measurementEntity = new MeasurementEntity();
            Map<String, String> tags = new HashMap<>();
            tags.put("table","table1");
            measurementEntity.setTags(tags);
            List<Sample> samples = new ArrayList<>();
            Sample sample = mock(Sample.class);
            samples.add(sample);
            Map<String, Number> vs = new HashMap<>();
            vs.put("snapshotRowTotal", 1);
            vs.put("snapshotInsertRowTotal", 0);
            when(sample.getVs()).thenReturn(vs);
            measurementEntity.setSamples(samples);
            when(mongoTemplate.find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(Collections.singletonList(measurementEntity));

            doReturn(mock(Criteria.class)).when(measurementServiceV2).createMinuteCriteria(anyString(), anyString(), anyString());
            doCallRealMethod().when(measurementServiceV2).querySyncStatic(any(),any());
            Page<TableSyncStaticVo> result = measurementServiceV2.querySyncStatic(tableSyncStaticDto,mock(UserDetail.class));

            assertEquals(5, result.getTotal());
        }

        @Test
        void testQuerySyncStatic_SyncRateIsOne() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId("615c9f48f1d842b8b78bf9c8"));
            taskDto.setDag(mock(DAG.class));

            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(taskDto);
            when(mongoTemplate.count(any(Query.class), eq(MeasurementEntity.COLLECTION_NAME))).thenReturn(5L);
            MeasurementEntity measurementEntity = new MeasurementEntity();
            Map<String, String> tags = new HashMap<>();
            tags.put("table","table1");
            measurementEntity.setTags(tags);
            List<Sample> samples = new ArrayList<>();
            Sample sample = mock(Sample.class);
            samples.add(sample);
            Map<String, Number> vs = new HashMap<>();
            vs.put("snapshotRowTotal", 1);
            vs.put("snapshotInsertRowTotal", 1);
            when(sample.getVs()).thenReturn(vs);
            measurementEntity.setSamples(samples);
            when(mongoTemplate.find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(Collections.singletonList(measurementEntity));

            doReturn(mock(Criteria.class)).when(measurementServiceV2).createMinuteCriteria(anyString(), anyString(), anyString());
            doCallRealMethod().when(measurementServiceV2).querySyncStatic(any(),any());
            Page<TableSyncStaticVo> result = measurementServiceV2.querySyncStatic(tableSyncStaticDto,mock(UserDetail.class));

            assertEquals(5, result.getTotal());
        }

        @Test
        void testQuerySyncStatic_Sync() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId("615c9f48f1d842b8b78bf9c8"));
            taskDto.setDag(mock(DAG.class));

            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(taskDto);
            when(mongoTemplate.count(any(Query.class), eq(MeasurementEntity.COLLECTION_NAME))).thenReturn(5L);
            MeasurementEntity measurementEntity = new MeasurementEntity();
            Map<String, String> tags = new HashMap<>();
            tags.put("table","table1");
            measurementEntity.setTags(tags);
            List<Sample> samples = new ArrayList<>();
            Sample sample = mock(Sample.class);
            samples.add(sample);
            Map<String, Number> vs = new HashMap<>();
            vs.put("snapshotSyncRate", 0.5);
            vs.put("snapshotRowTotal", 1);
            vs.put("snapshotInsertRowTotal", 1);
            when(sample.getVs()).thenReturn(vs);
            measurementEntity.setSamples(samples);
            when(mongoTemplate.find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(Collections.singletonList(measurementEntity));

            doReturn(mock(Criteria.class)).when(measurementServiceV2).createMinuteCriteria(anyString(), anyString(), anyString());
            doCallRealMethod().when(measurementServiceV2).querySyncStatic(any(),any());
            Page<TableSyncStaticVo> result = measurementServiceV2.querySyncStatic(tableSyncStaticDto,mock(UserDetail.class));

            assertEquals(5, result.getTotal());
        }
        @Test
        void testQuerySyncStatic() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId("615c9f48f1d842b8b78bf9c8"));
            taskDto.setDag(mock(DAG.class));

            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(taskDto);
            when(mongoTemplate.count(any(Query.class), eq(MeasurementEntity.COLLECTION_NAME))).thenReturn(5L);
            MeasurementEntity measurementEntity = new MeasurementEntity();
            Map<String, String> tags = new HashMap<>();
            tags.put("table","table1");
            measurementEntity.setTags(tags);
            List<Sample> samples = new ArrayList<>();
            Sample sample = mock(Sample.class);
            samples.add(sample);
            Map<String, Number> vs = new HashMap<>();
            vs.put("snapshotRowTotal", 0);
            vs.put("snapshotInsertRowTotal", 1);
            when(sample.getVs()).thenReturn(vs);
            measurementEntity.setSamples(samples);
            when(mongoTemplate.find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(Collections.singletonList(measurementEntity));

            doReturn(mock(Criteria.class)).when(measurementServiceV2).createMinuteCriteria(anyString(), anyString(), anyString());
            doCallRealMethod().when(measurementServiceV2).querySyncStatic(any(),any());
            Page<TableSyncStaticVo> result = measurementServiceV2.querySyncStatic(tableSyncStaticDto,mock(UserDetail.class));

            assertEquals(5, result.getTotal());
        }
        @Test
        void testQuerySyncStatic_snapshotRowTotal() {
            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId("615c9f48f1d842b8b78bf9c8"));
            taskDto.setDag(mock(DAG.class));

            when(taskService.findOne(any(Query.class), any(UserDetail.class))).thenReturn(taskDto);
            when(mongoTemplate.count(any(Query.class), eq(MeasurementEntity.COLLECTION_NAME))).thenReturn(5L);
            MeasurementEntity measurementEntity = new MeasurementEntity();
            Map<String, String> tags = new HashMap<>();
            tags.put("table","table1");
            measurementEntity.setTags(tags);
            List<Sample> samples = new ArrayList<>();
            Sample sample = mock(Sample.class);
            samples.add(sample);
            Map<String, Number> vs = new HashMap<>();
            vs.put("snapshotRowTotal", -1);
            vs.put("snapshotInsertRowTotal", 0);
            when(sample.getVs()).thenReturn(vs);
            measurementEntity.setSamples(samples);
            when(mongoTemplate.find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(Collections.singletonList(measurementEntity));

            doReturn(mock(Criteria.class)).when(measurementServiceV2).createMinuteCriteria(anyString(), anyString(), anyString());
            doCallRealMethod().when(measurementServiceV2).querySyncStatic(any(),any());
            Page<TableSyncStaticVo> result = measurementServiceV2.querySyncStatic(tableSyncStaticDto,mock(UserDetail.class));

            assertEquals(5, result.getTotal());
        }
    }

    @Nested
    class cleanRemovedTableMeasurementTest {
        @Test
        void testCleanRemovedTableMeasurementNormal() {
            String taskId = "111";
            String taskRecordId = "222";
            String tableName = "table1";
            MongoTemplate mongoOperations = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(measurementServiceV2, "mongoOperations", mongoOperations);
            doReturn(new Criteria()).when(measurementServiceV2).createMinuteCriteria(anyString(), anyString(), anyString());
            doCallRealMethod().when(measurementServiceV2).cleanRemovedTableMeasurement(taskId, taskRecordId, tableName);
            measurementServiceV2.cleanRemovedTableMeasurement(taskId, taskRecordId, tableName);
            verify(mongoOperations, new Times(1)).remove(any(Query.class), any(Class.class), anyString());
        }
    }

    @Nested
    class findRunTableTest {
        @Test
        void testFindRunTableNormal() {
            String taskId = "111";
            String taskRecordId = "222";
            MongoTemplate mongoOperations = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(measurementServiceV2, "mongoOperations", mongoOperations);
            List<MeasurementEntity> measurementEntities = new ArrayList<>();
            MeasurementEntity measurementEntity = mock(MeasurementEntity.class);
            measurementEntities.add(measurementEntity);
            Map<String, String> tags = new HashMap<>();
            tags.put("table", "test");
            when(measurementEntity.getTags()).thenReturn(tags);
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(measurementEntities);
            doReturn(mock(Criteria.class)).when(measurementServiceV2).createMinuteCriteria(anyString(), anyString(), anyString());
            doCallRealMethod().when(measurementServiceV2).findRunTable(taskId, taskRecordId);
            List<String> actual = measurementServiceV2.findRunTable(taskId, taskRecordId);
            verify(mongoOperations, new Times(1)).find(any(Query.class), any(Class.class), anyString());
            assertEquals("test", actual.get(0));
        }
    }

    @Nested
    class queryTableSyncStatusStatisticsTest{
        @Test
        void testQueryTableSyncStatusStatisticsNormal(){
            MongoTemplate mongoOperations = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(measurementServiceV2, "mongoOperations", mongoOperations);
            when(mongoOperations.aggregate(any(Aggregation.class), anyString(), any(Class.class))).thenReturn(mock(AggregationResults.class));
            SyncStatusStatisticsParam param = mock(SyncStatusStatisticsParam.class);
            doReturn("test-task-id").when(param).getTaskId();
            doReturn("test-task-record-id").when(param).getTaskRecordId();
            doReturn(mock(Criteria.class)).when(measurementServiceV2).createMinuteCriteria(anyString(), anyString(), anyString());
            doCallRealMethod().when(measurementServiceV2).queryTableSyncStatusStatistics(param);
            measurementServiceV2.queryTableSyncStatusStatistics(param);
            verify(mongoOperations, new Times(1)).aggregate(any(Aggregation.class), anyString(), any(Class.class));
        }
    }

    @Nested
    class getContinuousSamplesTest{
        @Test
        void testGetContinuousSamplesNormal(){
            MeasurementQueryParam.MeasurementQuerySample querySample = new MeasurementQueryParam.MeasurementQuerySample();
            querySample.setType(MEASUREMENT_QUERY_SAMPLE_TYPE_CONTINUOUS);
            querySample.setTags(new HashMap<>());
            querySample.setFields(new ArrayList<>());
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(new ArrayList<>());
            doCallRealMethod().when(measurementServiceV2).getContinuousSamples(any(), anyLong(), anyLong(), any());
            measurementServiceV2.getContinuousSamples(querySample, new Date().getTime(), new Date().getTime() + 1000 * 60 * 5, Granularity.GRANULARITY_HOUR);
            verify(mongoOperations, new Times(1)).find(any(Query.class), any(Class.class), anyString());
            assertEquals(4, querySample.getFields().size());
        }

    }

    @Nested
    class AggregateMeasurementByGranularityTest {
        private Map<String, String> queryTags;
        private MongoTemplate mongoOperations;
        private BulkOperations bulkOperations;

        @BeforeEach
        void setUp() {
            queryTags = new HashMap<>();
            queryTags.put("taskId", "test-task-id");
            queryTags.put("type", "task");

            mongoOperations = mock(MongoTemplate.class);
            bulkOperations = mock(BulkOperations.class);
            ReflectionTestUtils.setField(measurementServiceV2, "mongoOperations", mongoOperations);
        }

        @Test
        @DisplayName("test aggregateMeasurementByGranularity - skip when interval not fully completed")
        void testSkipWhenIntervalNotFullyCompleted() {
            long start = 1000L;
            long end = 1500L; // Less than one minute interval (60000ms)
            String granularity = Granularity.GRANULARITY_MINUTE;

            doCallRealMethod().when(measurementServiceV2).aggregateMeasurementByGranularity(queryTags, start, end, granularity);

            measurementServiceV2.aggregateMeasurementByGranularity(queryTags, start, end, granularity);

            // Should not query database when interval is not fully completed
            verify(mongoOperations, times(0)).find(any(Query.class), any(Class.class), anyString());
        }

        @Test
        @DisplayName("test aggregateMeasurementByGranularity - no data to aggregate")
        void testNoDataToAggregate() {
            long start = 0L;
            long end = 120000L; // 2 minutes
            String granularity = Granularity.GRANULARITY_MINUTE;

            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(new ArrayList<>());
            doCallRealMethod().when(measurementServiceV2).aggregateMeasurementByGranularity(queryTags, start, end, granularity);

            measurementServiceV2.aggregateMeasurementByGranularity(queryTags, start, end, granularity);

            verify(mongoOperations, times(1)).find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME));
            // Should not execute bulk operations when no data
            verify(mongoOperations, times(0)).bulkOps(any(), any(), anyString());
        }

        @Test
        @DisplayName("test aggregateMeasurementByGranularity - aggregate single entity")
        void testAggregateSingleEntity() {
            long start = 0L;
            long end = 120000L; // 2 minutes
            String granularity = Granularity.GRANULARITY_MINUTE;

            // Create test measurement entity
            MeasurementEntity entity = new MeasurementEntity();
            entity.setDate(new Date(60000L)); // 1 minute
            entity.setGranularity(granularity);
            Map<String, String> tags = new HashMap<>();
            tags.put("taskId", "test-task-id");
            tags.put("type", "task");
            entity.setTags(tags);

            // Create sample data
            List<Sample> samples = new ArrayList<>();
            Sample sample = new Sample();
            sample.setDate(new Date(60000L));
            Map<String, Number> vs = new HashMap<>();
            vs.put("inputQps", 100);
            vs.put("outputQps", 90);
            sample.setVs(vs);
            samples.add(sample);
            entity.setSamples(samples);

            // Mock averageValues
            Map<String, Number> avgValues = new HashMap<>();
            avgValues.put("inputQps", 100);
            avgValues.put("outputQps", 90);
            when(mongoOperations.find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(Collections.singletonList(entity));
            when(mongoOperations.bulkOps(any(), any(), anyString())).thenReturn(bulkOperations);
            doCallRealMethod().when(measurementServiceV2).aggregateMeasurementByGranularity(queryTags, start, end, granularity);

            measurementServiceV2.aggregateMeasurementByGranularity(queryTags, start, end, granularity);

            verify(mongoOperations, times(1)).find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME));
            verify(mongoOperations, times(1)).bulkOps(eq(BulkOperations.BulkMode.UNORDERED), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME));
            verify(bulkOperations, times(1)).execute();
            ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
            verify(bulkOperations).upsert(any(Query.class), updateCaptor.capture());
            Document updateDocument = updateCaptor.getValue().getUpdateObject();
            Document pushFields = (Document) updateDocument.get("$push");
            assertTrue(pushFields.containsKey(MetricCons.F_SAMPLES));
            assertTrue(pushFields.containsKey(MetricCons.F_DIGEST));
            assertFalse(updateDocument.containsKey("$set"));
        }

        @Test
        @DisplayName("test aggregateMeasurementByGranularity - aggregate multiple entities with same tags")
        void testAggregateMultipleEntitiesWithSameTags() {
            long start = 0L;
            long end = 180000L; // 3 minutes
            String granularity = Granularity.GRANULARITY_MINUTE;

            Map<String, String> tags = new HashMap<>();
            tags.put("taskId", "test-task-id");
            tags.put("type", "task");

            // Create multiple entities
            List<MeasurementEntity> entities = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                MeasurementEntity entity = new MeasurementEntity();
                entity.setDate(new Date(60000L * i));
                entity.setGranularity(granularity);
                entity.setTags(tags);

                List<Sample> samples = new ArrayList<>();
                Sample sample = new Sample();
                sample.setDate(new Date(60000L * i));
                Map<String, Number> vs = new HashMap<>();
                vs.put("inputQps", 100 + i * 10);
                sample.setVs(vs);
                samples.add(sample);
                entity.setSamples(samples);

                entities.add(entity);
            }

            when(mongoOperations.find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(entities);
            when(mongoOperations.bulkOps(any(), any(), anyString())).thenReturn(bulkOperations);
            doCallRealMethod().when(measurementServiceV2).aggregateMeasurementByGranularity(queryTags, start, end, granularity);

            measurementServiceV2.aggregateMeasurementByGranularity(queryTags, start, end, granularity);

            verify(mongoOperations, times(1)).find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME));
            verify(bulkOperations, times(1)).execute();
        }

        @Test
        @DisplayName("test aggregateMeasurementByGranularity - aggregate entities with different tags")
        void testAggregateEntitiesWithDifferentTags() {
            long start = 0L;
            long end = 120000L; // 2 minutes
            String granularity = Granularity.GRANULARITY_MINUTE;

            // Create entities with different tags
            List<MeasurementEntity> entities = new ArrayList<>();

            // Entity 1
            MeasurementEntity entity1 = new MeasurementEntity();
            entity1.setDate(new Date(60000L));
            entity1.setGranularity(granularity);
            Map<String, String> tags1 = new HashMap<>();
            tags1.put("taskId", "task-1");
            tags1.put("type", "task");
            entity1.setTags(tags1);
            List<Sample> samples1 = new ArrayList<>();
            Sample sample1 = new Sample();
            sample1.setDate(new Date(60000L));
            Map<String, Number> vs1 = new HashMap<>();
            vs1.put("inputQps", 100);
            sample1.setVs(vs1);
            samples1.add(sample1);
            entity1.setSamples(samples1);
            entities.add(entity1);

            // Entity 2
            MeasurementEntity entity2 = new MeasurementEntity();
            entity2.setDate(new Date(60000L));
            entity2.setGranularity(granularity);
            Map<String, String> tags2 = new HashMap<>();
            tags2.put("taskId", "task-2");
            tags2.put("type", "task");
            entity2.setTags(tags2);
            List<Sample> samples2 = new ArrayList<>();
            Sample sample2 = new Sample();
            sample2.setDate(new Date(60000L));
            Map<String, Number> vs2 = new HashMap<>();
            vs2.put("inputQps", 200);
            sample2.setVs(vs2);
            samples2.add(sample2);
            entity2.setSamples(samples2);
            entities.add(entity2);

            when(mongoOperations.find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(entities);
            when(mongoOperations.bulkOps(any(), any(), anyString())).thenReturn(bulkOperations);
            doCallRealMethod().when(measurementServiceV2).aggregateMeasurementByGranularity(queryTags, start, end, granularity);

            measurementServiceV2.aggregateMeasurementByGranularity(queryTags, start, end, granularity);

            verify(mongoOperations, times(1)).find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME));
            // Should create upsert operations for both tag groups
            verify(bulkOperations, atLeast(2)).upsert(any(Query.class), any());
            verify(bulkOperations, times(1)).execute();
        }

        @Test
        void attributedHourRollupBatchesLookupAndPrefersMatchingExistingIds() {
            Map<String, String> legacyTags = attributedTags("task-legacy");
            Map<String, String> deterministicTags = attributedTags("task-deterministic");
            Map<String, String> missingTags = attributedTags("task-missing");
            List<MeasurementEntity> entities = List.of(
                    attributedMinuteEntity(legacyTags),
                    attributedMinuteEntity(deterministicTags),
                    attributedMinuteEntity(missingTags));
            ObjectId legacyId = new ObjectId();
            String deterministicId = MeasurementServiceV2Impl.attributedTaskHourDocumentId(deterministicTags, new Date(0L));
            Map<String, String> reorderedLegacyTags = new LinkedHashMap<>();
            reorderedLegacyTags.put("physicalNodeId", legacyTags.get("physicalNodeId"));
            reorderedLegacyTags.put(MetricCons.Tags.F_TASK_ID, legacyTags.get(MetricCons.Tags.F_TASK_ID));
            reorderedLegacyTags.put(MetricCons.Tags.F_TYPE, legacyTags.get(MetricCons.Tags.F_TYPE));
            List<Document> existing = List.of(
                    new Document("_id", legacyId).append(MetricCons.F_TAGS, reorderedLegacyTags)
                            .append(MetricCons.F_DATE, new Date(0L)),
                    new Document("_id", new ObjectId()).append(MetricCons.F_TAGS, deterministicTags)
                            .append(MetricCons.F_DATE, new Date(0L)),
                    new Document("_id", deterministicId).append(MetricCons.F_TAGS, deterministicTags)
                            .append(MetricCons.F_DATE, new Date(0L)));

            MongoCollection<Document> collection = mock(MongoCollection.class);
            @SuppressWarnings("unchecked")
            FindIterable<Document> found = mock(FindIterable.class);
            when(mongoOperations.find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(entities);
            when(mongoOperations.getCollection(MeasurementEntity.COLLECTION_NAME)).thenReturn(collection);
            when(collection.find(any(Bson.class))).thenReturn(found);
            when(found.projection(any(Bson.class))).thenReturn(found);
            when(found.sort(any(Bson.class))).thenReturn(found);
            @SuppressWarnings("unchecked")
            MongoCursor<Document> cursor = mock(MongoCursor.class);
            Iterator<Document> iterator = existing.iterator();
            when(found.iterator()).thenReturn(cursor);
            when(cursor.hasNext()).thenAnswer(invocation -> iterator.hasNext());
            when(cursor.next()).thenAnswer(invocation -> iterator.next());
            doCallRealMethod().when(measurementServiceV2).aggregateMeasurementByGranularity(
                    anyMap(), anyLong(), anyLong(), eq(Granularity.GRANULARITY_MINUTE));

            measurementServiceV2.aggregateMeasurementByGranularity(queryTags, 0L, 180_000L,
                    Granularity.GRANULARITY_MINUTE);

            ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
            verify(collection, times(1)).find(queryCaptor.capture());
            Document lookup = Document.parse(queryCaptor.getValue().toBsonDocument(Document.class,
                    com.mongodb.MongoClientSettings.getDefaultCodecRegistry()).toJson());
            assertEquals(3, ((List<?>) ((Document) lookup.get("tags.taskId")).get("$in")).size());
            assertEquals(List.of(new Date(0L)), ((Document) lookup.get(MetricCons.F_DATE)).get("$in"));

            @SuppressWarnings({"unchecked", "rawtypes"})
            ArgumentCaptor<List<WriteModel<Document>>> writesCaptor = (ArgumentCaptor) ArgumentCaptor.forClass(List.class);
            verify(collection).bulkWrite(writesCaptor.capture(), any(BulkWriteOptions.class));
            Map<String, Object> idsByTask = new HashMap<>();
            for (WriteModel<Document> write : writesCaptor.getValue()) {
                UpdateOneModel<Document> update = (UpdateOneModel<Document>) write;
                Document stage = (Document) update.getUpdatePipeline().get(0);
                Document set = stage.get("$set", Document.class);
                Document tagsExpression = set.get(MetricCons.F_TAGS, Document.class);
                List<?> tagOptions = (List<?>) tagsExpression.get("$ifNull");
                Document literal = (Document) tagOptions.get(1);
                Map<?, ?> tags = (Map<?, ?>) literal.get("$literal");
                idsByTask.put((String) tags.get(MetricCons.Tags.F_TASK_ID),
                        ((Document) update.getFilter()).get("_id"));
                assertTrue(update.getOptions().isUpsert());
            }
            assertEquals(legacyId, idsByTask.get("task-legacy"));
            assertEquals(deterministicId, idsByTask.get("task-deterministic"));
            assertEquals(MeasurementServiceV2Impl.attributedTaskHourDocumentId(missingTags, new Date(0L)),
                    idsByTask.get("task-missing"));
        }

        @Test
        void attributedHourRollupLookupBatchesTaskIdsByOneHundred() {
            List<MeasurementEntity> entities = new ArrayList<>();
            for (int i = 0; i < 101; i++) {
                entities.add(attributedMinuteEntity(attributedTags("task-" + i)));
            }
            MongoCollection<Document> collection = mock(MongoCollection.class);
            @SuppressWarnings("unchecked")
            FindIterable<Document> found = mock(FindIterable.class);
            when(mongoOperations.find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(entities);
            when(mongoOperations.getCollection(MeasurementEntity.COLLECTION_NAME)).thenReturn(collection);
            when(collection.find(any(Bson.class))).thenReturn(found);
            when(found.projection(any(Bson.class))).thenReturn(found);
            when(found.sort(any(Bson.class))).thenReturn(found);
            @SuppressWarnings("unchecked")
            MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(found.iterator()).thenReturn(cursor);
            doCallRealMethod().when(measurementServiceV2).aggregateMeasurementByGranularity(
                    anyMap(), anyLong(), anyLong(), eq(Granularity.GRANULARITY_MINUTE));

            measurementServiceV2.aggregateMeasurementByGranularity(queryTags, 0L, 60_000L,
                    Granularity.GRANULARITY_MINUTE);

            ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
            verify(collection, times(2)).find(queryCaptor.capture());
            assertEquals(List.of(1, 100), queryCaptor.getAllValues().stream()
                    .map(bson -> {
                        Document query = Document.parse(bson.toBsonDocument(Document.class,
                                com.mongodb.MongoClientSettings.getDefaultCodecRegistry()).toJson());
                        return ((List<?>) ((Document) query.get("tags.taskId")).get("$in")).size();
                    }).sorted().toList());
            @SuppressWarnings({"unchecked", "rawtypes"})
            ArgumentCaptor<List<WriteModel<Document>>> writesCaptor = (ArgumentCaptor) ArgumentCaptor.forClass(List.class);
            verify(collection).bulkWrite(writesCaptor.capture(), any(BulkWriteOptions.class));
            assertEquals(101, writesCaptor.getValue().size());
        }

        private MeasurementEntity attributedMinuteEntity(Map<String, String> tags) {
            MeasurementEntity entity = taskMetricsEntity(tags.get(MetricCons.Tags.F_TASK_ID),
                    tags.get("physicalNodeId"), Granularity.GRANULARITY_MINUTE, List.of(metricSample(0L, 10D)));
            entity.setTags(tags);
            entity.setDate(new Date(0L));
            return entity;
        }

        private Map<String, String> attributedTags(String taskId) {
            Map<String, String> tags = new LinkedHashMap<>();
            tags.put(MetricCons.Tags.F_TYPE, MetricCons.SampleType.TASK.code());
            tags.put(MetricCons.Tags.F_TASK_ID, taskId);
            tags.put("physicalNodeId", "node-a");
            return tags;
        }

        @Test
        @DisplayName("test aggregateMeasurementByGranularity - hour granularity")
        void testHourGranularity() {
            long start = 0L;
            long end = 7200000L; // 2 hours
            String granularity = Granularity.GRANULARITY_HOUR;

            when(mongoOperations.find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME)))
                    .thenReturn(new ArrayList<>());
            doCallRealMethod().when(measurementServiceV2).aggregateMeasurementByGranularity(queryTags, start, end, granularity);

            measurementServiceV2.aggregateMeasurementByGranularity(queryTags, start, end, granularity);

            verify(mongoOperations, times(1)).find(any(Query.class), eq(MeasurementEntity.class), eq(MeasurementEntity.COLLECTION_NAME));
        }
    }

    @Nested
    class FillNodeCurrentEventTimestampIfNeedTest {
        private TaskService taskService;

        @BeforeEach
        void setup() {
            taskService = mock(TaskService.class);
            ReflectionTestUtils.setField(measurementServiceV2, "taskService", taskService);
        }

        @Test
        @DisplayName("test fillNodeCurrentEventTimestampIfNeed - isNode, migrate task")
        void testIsNodeMigrateTask() {
            Map<String, List<Map<String, Object>>> data = new HashMap<>();
            List<Map<String, Object>> uniqueData = new ArrayList<>();
            Map<String, Object> sample1 = new HashMap<>();
            Map<String, String> tags1 = new HashMap<>();
            tags1.put("type", "node");
            tags1.put("taskId", "6a46322e5b75c32ed96ac543");
            tags1.put("nodeId", "node1");
            sample1.put("tags", tags1);
            sample1.put("currentEventTimestamp", null);
            sample1.put("snapshotDoneAt", 0L);
            uniqueData.add(sample1);
            data.put("uniqueKey", uniqueData);

            MeasurementQueryParam param = new MeasurementQueryParam();
            Map<String, MeasurementQueryParam.MeasurementQuerySample> samples = new HashMap<>();
            MeasurementQueryParam.MeasurementQuerySample querySample = new MeasurementQueryParam.MeasurementQuerySample();
            Map<String, String> qtags = new HashMap<>();
            qtags.put("type", "node");
            qtags.put("taskId", "6a46322e5b75c32ed96ac543");
            querySample.setTags(qtags);
            querySample.setType("instant");
            samples.put("uniqueKey", querySample);
            param.setSamples(samples);

            TaskDto taskDto = new TaskDto();
            taskDto.setSyncType("migrate");
            taskDto.setSnapshotDoneAt(123456789L);
            Map<String, Long> nodeTs = new HashMap<>();
            nodeTs.put("node1", 987654321L);
            nodeTs.put("node2", 888888888L);
            taskDto.setNodeCurrentEventTimestamp(nodeTs);
            Map<String, Object> attrs = new HashMap<>();
            Map<String, Object> milestone = new HashMap<>();
            Map<String, Object> snapshot = new HashMap<>();
            snapshot.put("progress", 2);
            milestone.put("SNAPSHOT", snapshot);
            attrs.put("milestone", milestone);
            taskDto.setAttrs(attrs);

            when(taskService.findByTaskId(any(ObjectId.class), any(String[].class))).thenReturn(taskDto);

            ReflectionTestUtils.invokeMethod(measurementServiceV2, "fillNodeCurrentEventTimestampIfNeed", data, param);

            assertEquals(987654321L, sample1.get("currentEventTimestamp"));
            assertEquals(123456789L, sample1.get("snapshotDoneAt"));

            List<Map<String, Object>> resultList = data.get("uniqueKey");
            assertEquals(2, resultList.size());
            Map<String, Object> paddedSample = resultList.stream()
                    .filter(s -> "node2".equals(((Map<String, String>) s.get("tags")).get("nodeId")))
                    .findFirst()
                    .orElse(null);
            assertNotNull(paddedSample);
            assertEquals(888888888L, paddedSample.get("currentEventTimestamp"));
            assertEquals(123456789L, paddedSample.get("snapshotDoneAt"));
            assertEquals(2L, paddedSample.get("snapshotTableTotal"));
            assertEquals("database", ((Map<String, String>) paddedSample.get("tags")).get("nodeType"));
        }

        @Test
        @DisplayName("test fillNodeCurrentEventTimestampIfNeed - isTask, sync task")
        void testIsTaskSyncTask() {
            Map<String, List<Map<String, Object>>> data = new HashMap<>();
            List<Map<String, Object>> uniqueData = new ArrayList<>();
            data.put("uniqueKey", uniqueData);

            MeasurementQueryParam param = new MeasurementQueryParam();
            Map<String, MeasurementQueryParam.MeasurementQuerySample> samples = new HashMap<>();
            MeasurementQueryParam.MeasurementQuerySample querySample = new MeasurementQueryParam.MeasurementQuerySample();
            Map<String, String> qtags = new HashMap<>();
            qtags.put("type", "task");
            qtags.put("taskId", "69feb7076daae8c9545dad1c");
            querySample.setTags(qtags);
            querySample.setType("instant");
            samples.put("uniqueKey", querySample);
            param.setSamples(samples);

            TaskDto taskDto = new TaskDto();
            taskDto.setSyncType("sync");
            taskDto.setSnapshotDoneAt(555555L);
            taskDto.setCurrentEventTimestamp(999999L);
            Map<String, Object> attrs = new HashMap<>();
            Map<String, Object> milestone = new HashMap<>();
            Map<String, Object> snapshot = new HashMap<>();
            snapshot.put("progress", 2);
            milestone.put("SNAPSHOT", snapshot);
            attrs.put("milestone", milestone);
            taskDto.setAttrs(attrs);

            when(taskService.findByTaskId(any(ObjectId.class), any(String[].class))).thenReturn(taskDto);

            ReflectionTestUtils.invokeMethod(measurementServiceV2, "fillNodeCurrentEventTimestampIfNeed", data, param);

            List<Map<String, Object>> resultList = data.get("uniqueKey");
            assertEquals(1, resultList.size());
            Map<String, Object> padded = resultList.get(0);
            assertEquals(999999L, padded.get("currentEventTimestamp"));
            assertEquals(555555L, padded.get("snapshotDoneAt"));
            assertEquals(2L, padded.get("snapshotTableTotal"));
            assertEquals("task", ((Map<String, String>) padded.get("tags")).get("type"));
        }
    }

}

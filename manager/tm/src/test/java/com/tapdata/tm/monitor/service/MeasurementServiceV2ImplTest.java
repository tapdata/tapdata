package com.tapdata.tm.monitor.service;


import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.constant.KeyWords;
import com.tapdata.tm.monitor.dto.TableSyncStaticDto;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.vo.TableSyncStaticVo;
import com.tapdata.tm.task.bean.TableStatusInfoDto;
import com.tapdata.tm.task.service.TaskService;
import io.github.openlg.graphlib.Graph;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.schema.partition.TapSubPartitionTableInfo;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.internal.verification.Times;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MeasurementServiceV2ImplTest {
    MeasurementServiceV2Impl measurementServiceV2;
    @BeforeEach
    void init() {
        measurementServiceV2 = mock(MeasurementServiceV2Impl.class);
    }
    @Test
    void testParams() {
        assertEquals("initial_sync", KeyWords.INITIAL_SYNC);
        assertEquals("DONE", KeyWords.DONE);
        assertEquals("complete", KeyWords.COMPLETE);
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
            verify(taskService).updateDelayTime(new ObjectId("665d2b9b889245e73373cf49"), 0L);
        }
        @Test
        @DisplayName("test addBulkAgentMeasurement method when taskDelayTimeMap contains taskId")
        void test3(){
            Sample sample = new Sample();
            Date date = new Date();
            sample.setDate(date);
            Map vs = new HashMap();
            vs.put("replicateLag",null);
            sample.setVs(vs);
            when(sampleRequest.getSample()).thenReturn(sample);
            samples.add(sampleRequest);
            doCallRealMethod().when(measurementServiceV2).addAgentMeasurement(samples);
            measurementServiceV2.addAgentMeasurement(samples);
            verify(taskService, new Times(1)).updateDelayTime(new ObjectId("665d2b9b889245e73373cf49"), 0L);
        }
        @Test
        @DisplayName("test addBulkAgentMeasurement method when taskDelayTimeMap not contains taskId")
        void test4(){
            Sample sample = new Sample();
            Date date = new Date();
            sample.setDate(date);
            Map vs = new HashMap();
            vs.put("replicateLag",null);
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

            TableSyncStaticDto dto = new TableSyncStaticDto("taskRecordId", 1, 10, "test");
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

            TableSyncStaticDto dto = new TableSyncStaticDto("taskRecordId", 1, 10, "test");
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

            TableSyncStaticDto dto = new TableSyncStaticDto("taskRecordId", 1, 10, "test");
            UserDetail userDetail = mock(UserDetail.class);

            Page<TableSyncStaticVo> result = measurementService.querySyncStatic(dto, userDetail);

            Assertions.assertNotNull(result);
            Assertions.assertEquals(10, result.getTotal());
        }
    }
}

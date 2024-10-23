package com.tapdata.tm.monitor.service;


import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.constant.KeyWords;
import com.tapdata.tm.monitor.dto.TableSyncStaticDto;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.vo.TableSyncStaticVo;
import com.tapdata.tm.task.bean.TableStatusInfoDto;
import com.tapdata.tm.task.service.TaskService;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.internal.verification.Times;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
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
            tableSyncStaticDto = new TableSyncStaticDto("test",1,20,"test");
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

            doCallRealMethod().when(measurementServiceV2).querySyncStatic(any(),any());
            Page<TableSyncStaticVo> result = measurementServiceV2.querySyncStatic(tableSyncStaticDto,mock(UserDetail.class));

            assertEquals(5, result.getTotal());
        }
    }
}

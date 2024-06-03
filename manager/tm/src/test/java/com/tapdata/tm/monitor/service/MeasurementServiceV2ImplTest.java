package com.tapdata.tm.monitor.service;


import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.monitor.constant.KeyWords;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.vo.TableSyncStaticVo;
import com.tapdata.tm.task.service.TaskService;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
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
        Assertions.assertEquals("initial_sync", KeyWords.INITIAL_SYNC);
        Assertions.assertEquals("DONE", KeyWords.DONE);
        Assertions.assertEquals("complete", KeyWords.COMPLETE);
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
        @BeforeEach
        void beforeEach(){
            mongoOperations = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(measurementServiceV2,"mongoOperations",mongoOperations);
            when(mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME)).thenReturn(mock(BulkOperations.class));
            taskService = mock(TaskService.class);
            ReflectionTestUtils.setField(measurementServiceV2,"taskService",taskService);
        }
        @Test
        @DisplayName("test addBulkAgentMeasurement method normal")
        void test1(){
            List<SampleRequest> samples = new ArrayList<>();
            SampleRequest sampleRequest = mock(SampleRequest.class);
            samples.add(sampleRequest);
            Map<String, String> tags = new HashMap<>();
            String id = "665d2b9b889245e73373cf49";
            tags.put("type","task");
            tags.put("taskId",id);
            when(sampleRequest.getTags()).thenReturn(tags);
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
            List<SampleRequest> samples = new ArrayList<>();
            SampleRequest sampleRequest = mock(SampleRequest.class);
            samples.add(sampleRequest);
            Map<String, String> tags = new HashMap<>();
            String id = "665d2b9b889245e73373cf49";
            tags.put("type","task");
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
            verify(taskService).updateDelayTime(new ObjectId("665d2b9b889245e73373cf49"), 0L);
        }
    }
}
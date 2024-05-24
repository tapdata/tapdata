package com.tapdata.tm.monitor.service;


import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.monitor.constant.KeyWords;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.vo.TableSyncStaticVo;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

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
        List<MeasurementEntity> entities;

        @BeforeEach
        void init() {
            mongoOperations = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(measurementServiceV2, "mongoOperations", mongoOperations);
            entities = new ArrayList<>();
            when(measurementServiceV2.findLastMinuteByTaskId(anyString())).thenCallRealMethod();
            when(measurementServiceV2.findLastMinuteByTaskId(anyList())).thenCallRealMethod();
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(entities);
        }

        @Test
        void testNormal() {
            measurementServiceV2.findLastMinuteByTaskId(new ArrayList<>());
            verify(mongoOperations).find(any(Query.class), any(Class.class), anyString());
        }

        @Test
        void testOne() {
            measurementServiceV2.findLastMinuteByTaskId(new ObjectId().toHexString());
            verify(mongoOperations).find(any(Query.class), any(Class.class), anyString());
        }
        @Test
        void testOneHasResult() {
            entities.add(new MeasurementEntity());
            measurementServiceV2.findLastMinuteByTaskId(new ObjectId().toHexString());
            verify(mongoOperations).find(any(Query.class), any(Class.class), anyString());
        }
    }
}
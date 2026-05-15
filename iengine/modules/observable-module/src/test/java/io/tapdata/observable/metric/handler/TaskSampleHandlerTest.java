package io.tapdata.observable.metric.handler;

import io.tapdata.common.sample.sampler.CounterSampler;
import org.junit.Assert;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class TaskSampleHandlerTest {

    @Test
    void handleTableCountAcceptTest() {
        TaskSampleHandler handler = mock(TaskSampleHandler.class);
        Map<String, Long> currentSnapshotTableRowTotalMap = new HashMap<>();
        ReflectionTestUtils.setField(handler, "currentSnapshotTableRowTotalMap", currentSnapshotTableRowTotalMap);
        CounterSampler snapshotRowTotal = mock(CounterSampler.class);
        ReflectionTestUtils.setField(handler, "snapshotRowTotal", snapshotRowTotal);
        String table = "table1";
        doCallRealMethod().when(handler).handleTableCountAccept(anyString(), anyLong());
        handler.handleTableCountAccept(table, -1L);
        handler.handleTableCountAccept(table, 10L);
        assertEquals(10L, currentSnapshotTableRowTotalMap.get(table));
    }

    @Nested
    class GetTaskMemTest {
        @Test
        void testGetTaskMem() {
            TaskSampleHandler handler = mock(TaskSampleHandler.class);
            AtomicReference<Long> taskMemUsage = new AtomicReference<>(-1L);
            ReflectionTestUtils.setField(handler, "taskMemUsage", taskMemUsage);
            doCallRealMethod().when(handler).getTaskMem();
            Long mem = handler.getTaskMem();
            assertEquals(null, mem);
            taskMemUsage.set(100L);
            mem = handler.getTaskMem();
            assertEquals(100L, mem);
        }
        @Test
        void testGetTaskMem3() {
            TaskSampleHandler handler = mock(TaskSampleHandler.class);
            AtomicReference<Long> taskMemUsage = new AtomicReference<>(0L);
            ReflectionTestUtils.setField(handler, "taskMemUsage", taskMemUsage);
            doCallRealMethod().when(handler).getTaskMem();
            Long mem = handler.getTaskMem();
            assertEquals(0L, mem);
        }
        @Test
        void testGetTaskMem4() {
            TaskSampleHandler handler = mock(TaskSampleHandler.class);
            AtomicReference<Long> taskMemUsage = new AtomicReference<>(100L);
            ReflectionTestUtils.setField(handler, "taskMemUsage", taskMemUsage);
            doCallRealMethod().when(handler).getTaskMem();
            Long mem = handler.getTaskMem();
            assertEquals(100L, mem);
        }
    }
}

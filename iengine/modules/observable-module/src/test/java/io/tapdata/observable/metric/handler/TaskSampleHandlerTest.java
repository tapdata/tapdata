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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class TaskSampleHandlerTest {

    @Test
    public void testStaticFields() {
        testStaticFields("SAMPLE_TYPE_TASK", "task");
        testStaticFields("TABLE_TOTAL", "tableTotal");
        testStaticFields("CREATE_TABLE_TOTAL", "createTableTotal");
        testStaticFields("SNAPSHOT_TABLE_TOTAL", "snapshotTableTotal");
        testStaticFields("SNAPSHOT_ROW_TOTAL", "snapshotRowTotal");
        testStaticFields("SNAPSHOT_INSERT_ROW_TOTAL", "snapshotInsertRowTotal");
        testStaticFields("SNAPSHOT_START_AT", "snapshotStartAt");
        testStaticFields("SNAPSHOT_DONE_AT", "snapshotDoneAt");
        testStaticFields("SNAPSHOT_DONE_COST", "snapshotDoneCost");
        testStaticFields("CURR_SNAPSHOT_TABLE", "currentSnapshotTable");
        testStaticFields("CURR_SNAPSHOT_TABLE_ROW_TOTAL", "currentSnapshotTableRowTotal");
        testStaticFields("CURR_SNAPSHOT_TABLE_INSERT_ROW_TOTAL", "currentSnapshotTableInsertRowTotal");
        testStaticFields("OUTPUT_QPS_MAX", "outputQpsMax");
        testStaticFields("OUTPUT_QPS_AVG", "outputQpsAvg");
    }

    private void testStaticFields(String fieldName, String value) {
        Object v = null;
        try {
            Field field = TaskSampleHandler.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            v = field.get(null);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        Assert.assertNotNull(v);
        Assert.assertEquals(value, v);
    }
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
            AtomicLong taskMemUsage = new AtomicLong(-1);
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
            AtomicLong taskMemUsage = new AtomicLong(0);
            ReflectionTestUtils.setField(handler, "taskMemUsage", taskMemUsage);
            doCallRealMethod().when(handler).getTaskMem();
            Long mem = handler.getTaskMem();
            assertEquals(0L, mem);
        }
        @Test
        void testGetTaskMem4() {
            TaskSampleHandler handler = mock(TaskSampleHandler.class);
            AtomicLong taskMemUsage = new AtomicLong(100L);
            ReflectionTestUtils.setField(handler, "taskMemUsage", taskMemUsage);
            doCallRealMethod().when(handler).getTaskMem();
            Long mem = handler.getTaskMem();
            assertEquals(100L, mem);
        }
    }
}

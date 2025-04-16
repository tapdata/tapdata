package com.tapdata.taskinspect;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.taskinspect.vo.TaskInspectCdcEvent;
import com.tapdata.tm.taskinspect.TaskInspectConfig;
import com.tapdata.tm.taskinspect.TaskInspectMode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.schema.TapTableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

/**
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/15 23:50 Create
 */
@ExtendWith(MockitoExtension.class)
class IModeTest {

    TapTableMap<String, TapTable> mockTapTableMap(String key, TapTable value) {
        TapTableMap<String, TapTable> map = mock(TapTableMap.class);
        if (null != key) {
            doReturn(value).when(map).get(eq(key));
        }
        return map;
    }

    TapdataEvent createTapdataEvent(TapEvent event) {
        TapdataEvent ins = new TapdataEvent();
        ins.setTapEvent(event);
        return ins;
    }

    IMode iMode;

    @Mock
    DataProcessorContext dataProcessorContext;
    @Mock
    TaskInspectConfig config;
    @Mock
    TapInsertRecordEvent insertEvent;
    @Mock
    TapUpdateRecordEvent updateEvent;
    @Mock
    TapDeleteRecordEvent deleteEvent;
    @Mock
    TapTable tapTable;

    @BeforeEach
    void setUp() {
        iMode = new IMode() {};
        reset(dataProcessorContext, config, insertEvent, updateEvent, deleteEvent, tapTable);
    }

    @Nested
    class getModeTest {

        @Test
        void testGetMode() {
            // 逻辑预设：默认实现返回 TaskInspectMode.CLOSE
            TaskInspectMode mode = iMode.getMode();

            // 期望：返回 TaskInspectMode.CLOSE
            assertEquals(TaskInspectMode.CLOSE, mode);
        }
    }

    @Nested
    class refreshTest {

        @Test
        void testRefresh() throws InterruptedException {
            // 逻辑预设：默认实现不执行任何操作
            iMode.refresh(config);

            // 期望：不抛出异常
            // 这里不需要额外的验证，因为默认实现是空的
        }
    }

    @Nested
    class acceptCdcEventTest {

        @Test
        void testAcceptCdcEvent_TaskInspectCdcEvent() {
            // 逻辑预设：默认实现不执行任何操作
            TaskInspectCdcEvent event = mock(TaskInspectCdcEvent.class);
            iMode.acceptCdcEvent(event);

            // 期望：不抛出异常
            // 这里不需要额外的验证，因为默认实现是空的
        }

        @Test
        void testAcceptCdcEvent_DataProcessorContext_TapInsertRecordEvent() {
            // 逻辑预设：插入事件处理
            when(insertEvent.getTableId()).thenReturn("table1");
            when(insertEvent.getAfter()).thenReturn(Collections.singletonMap("id", 1));
            TapTableMap<String, TapTable> tapTableMap = mockTapTableMap("table1", tapTable);
            when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
            when(tapTable.primaryKeys(true)).thenReturn(Arrays.asList("id"));

            IMode spy = spy(iMode);
            spy.acceptCdcEvent(dataProcessorContext, createTapdataEvent(insertEvent));

            // 期望：调用 acceptCdcEvent(TaskInspectCdcEvent)
            verify(spy).acceptCdcEvent(any(TaskInspectCdcEvent.class));
        }

        @Test
        void testAcceptCdcEvent_DataProcessorContext_TapUpdateRecordEvent() {
            // 逻辑预设：更新事件处理
            when(updateEvent.getTableId()).thenReturn("table1");
            when(updateEvent.getAfter()).thenReturn(Collections.singletonMap("id", 1));
            when(updateEvent.getBefore()).thenReturn(Collections.singletonMap("id", 2));
            TapTableMap<String, TapTable> tapTableMap = mockTapTableMap("table1", tapTable);
            when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
            when(tapTable.primaryKeys(true)).thenReturn(Arrays.asList("id"));

            IMode spy = spy(iMode);
            spy.acceptCdcEvent(dataProcessorContext, createTapdataEvent(updateEvent));

            // 期望：调用 acceptCdcEvent(TaskInspectCdcEvent) 两次
            verify(spy, times(2)).acceptCdcEvent(any(TaskInspectCdcEvent.class));
        }

        @Test
        void testAcceptCdcEvent_DataProcessorContext_TapDeleteRecordEvent() {
            // 逻辑预设：删除事件处理
            when(deleteEvent.getTableId()).thenReturn("table1");
            when(deleteEvent.getBefore()).thenReturn(Collections.singletonMap("id", 1));
            TapTableMap<String, TapTable> tapTableMap = mockTapTableMap("table1", tapTable);
            when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
            when(tapTable.primaryKeys(true)).thenReturn(Arrays.asList("id"));

            IMode spy = spy(iMode);
            spy.acceptCdcEvent(dataProcessorContext, createTapdataEvent(deleteEvent));

            // 期望：调用 acceptCdcEvent(TaskInspectCdcEvent)
            verify(spy).acceptCdcEvent(any(TaskInspectCdcEvent.class));
        }
    }

    @Nested
    class syncDelayTest {

        @Test
        void testSyncDelay() {
            // 逻辑预设：默认实现不执行任何操作
            iMode.syncDelay(1000);

            // 期望：不抛出异常
            // 这里不需要额外的验证，因为默认实现是空的
        }
    }

    @Nested
    class getKeysTest {

        @Test
        void testGetKeys_WithPrimaryKeys() {
            // 逻辑预设：有主键
            TapTableMap<String, TapTable> tapTableMap = mockTapTableMap("table1", tapTable);
            when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
            when(tapTable.primaryKeys(true)).thenReturn(Arrays.asList("id"));
            Map<String, Object> data = Collections.singletonMap("id", 1);

            LinkedHashMap<String, Object> keys = iMode.getKeys(dataProcessorContext, "table1", data);

            // 期望：返回包含主键的 Map
            assertEquals(1, keys.size());
            assertEquals(1, keys.get("id"));
        }

        @Test
        void testGetKeys_NoPrimaryKeys() {
            // 逻辑预设：没有主键
            TapTableMap<String, TapTable> tapTableMap = mockTapTableMap("table1", tapTable);
            when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
            when(tapTable.primaryKeys(true)).thenReturn(Collections.emptyList());
            Map<String, Object> data = Collections.singletonMap("id", 1);

            LinkedHashMap<String, Object> keys = iMode.getKeys(dataProcessorContext, "table1", data);

            // 期望：返回空 Map
            assertTrue(keys.isEmpty());
        }

        @Test
        void testGetKeys_NoTapTable() {
            // 逻辑预设：没有 TapTable
            TapTableMap<String, TapTable> tapTableMap = mockTapTableMap(null, null);
            when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
            Map<String, Object> data = Collections.singletonMap("id", 1);

            LinkedHashMap<String, Object> keys = iMode.getKeys(dataProcessorContext, "table1", data);

            // 期望：返回空 Map
            assertTrue(keys.isEmpty());
        }
    }

    @Nested
    class stopTest {

        @Test
        void testStop() {
            // 逻辑预设：默认实现返回 true
            boolean stopped = iMode.stop();

            // 期望：返回 true
            assertTrue(stopped);
        }
    }
}

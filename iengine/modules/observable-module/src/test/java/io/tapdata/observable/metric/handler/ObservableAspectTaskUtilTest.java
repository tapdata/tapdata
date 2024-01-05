package io.tapdata.observable.metric.handler;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.observable.metric.util.SyncGetMemorySizeHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ObservableAspectTaskUtilTest {
    CompletableFuture<Void> future;
    List<TapEvent> events;
    SyncGetMemorySizeHandler syncGetMemorySizeHandler;
    String nodeId;
    Map<String, DataNodeSampleHandler> dataNodeSampleHandlers;
    TaskSampleHandler taskSampleHandler;
    List<TapdataEvent> es;
    DataNodeSampleHandler dataNodeSampleHandler;
    long mockTimestamp = 1000L;
    HandlerUtil.EventTypeRecorder record;

    @BeforeEach
    void init() {
        nodeId = UUID.randomUUID().toString();
        future = CompletableFuture.runAsync(() -> {});
        syncGetMemorySizeHandler = new SyncGetMemorySizeHandler(new AtomicLong(-1));
        dataNodeSampleHandlers = new HashMap<>();
        dataNodeSampleHandler = mock(DataNodeSampleHandler.class);
        dataNodeSampleHandlers.put(nodeId, dataNodeSampleHandler);
    }

    @Nested
    class StreamReadCompleteTest {
        @BeforeEach
        void init() {
            events = new ArrayList<>();
            taskSampleHandler = mock(TaskSampleHandler.class);
        }
        @Test
        void testStreamReadCompleteNormal() {
            TapUpdateRecordEvent tapEvent = new  TapUpdateRecordEvent();
            HashMap<String, Object> after = new HashMap<>();
            after.put("id", "ddd");
            tapEvent.after(after);
            HashMap<String, Object> before = new HashMap<>();
            before.put("id", "ddd");
            tapEvent.before(before);
            events.add(tapEvent);
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doNothing().when(taskSampleHandler).handleStreamReadAccept(record);
            doNothing().when(dataNodeSampleHandler).handleStreamReadReadComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.streamReadComplete(future,
                    events,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler, mockTimestamp);
            verify(dataNodeSampleHandler, times(1)).handleStreamReadReadComplete(mockTimestamp, record);
            verify(taskSampleHandler, times(1)).handleStreamReadAccept(record);
        }
        @Test
        void testStreamReadCompleteWithNullDataNodeSampleHandler() {
            dataNodeSampleHandlers.remove(nodeId);
            TapUpdateRecordEvent tapEvent = new  TapUpdateRecordEvent();
            HashMap<String, Object> after = new HashMap<>();
            after.put("id", "ddd");
            tapEvent.after(after);
            HashMap<String, Object> before = new HashMap<>();
            before.put("id", "ddd");
            tapEvent.before(before);
            events.add(tapEvent);
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doNothing().when(taskSampleHandler).handleStreamReadAccept(record);
            doNothing().when(dataNodeSampleHandler).handleStreamReadReadComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.streamReadComplete(future,
                    events,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler, mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleStreamReadReadComplete(mockTimestamp, record);
            verify(taskSampleHandler, times(1)).handleStreamReadAccept(record);
        }
        @Test
        void testStreamReadCompleteEmptyEvent() {
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doNothing().when(taskSampleHandler).handleStreamReadAccept(record);
            doNothing().when(dataNodeSampleHandler).handleStreamReadReadComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.streamReadComplete(future,
                    events,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleStreamReadReadComplete(mockTimestamp, record);
            verify(taskSampleHandler, times(0)).handleStreamReadAccept(record);
        }
        @Test
        void testStreamReadCompleteNullEvent() {
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doNothing().when(taskSampleHandler).handleStreamReadAccept(record);
            doNothing().when(dataNodeSampleHandler).handleStreamReadReadComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.streamReadComplete(future,
                    null,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleStreamReadReadComplete(mockTimestamp, record);
            verify(taskSampleHandler, times(0)).handleStreamReadAccept(record);
        }
    }

    @Nested
    class StreamReadProcessCompleteTest {
        @BeforeEach
        void init() {
            es = new ArrayList<>();
            taskSampleHandler = mock(TaskSampleHandler.class);
        }
        @Test
        void testStreamReadProcessCompleteNormal() {
            TapdataEvent e = new TapdataEvent();
            TapUpdateRecordEvent tapEvent = new  TapUpdateRecordEvent();
            HashMap<String, Object> after = new HashMap<>();
            after.put("id", "ddd");
            tapEvent.after(after);
            HashMap<String, Object> before = new HashMap<>();
            before.put("id", "ddd");
            tapEvent.before(before);
            e.setTapEvent(tapEvent);
            es.add(e);
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doCallRealMethod().when(dataNodeSampleHandler).handleStreamReadProcessComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.streamReadProcessComplete(future,
                    es,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,mockTimestamp);
            verify(dataNodeSampleHandler, times(1)).handleStreamReadProcessComplete(mockTimestamp, record);
        }
        @Test
        void testStreamReadProcessCompleteEmptyEvent() {
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doCallRealMethod().when(dataNodeSampleHandler).handleStreamReadProcessComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.streamReadProcessComplete(future,
                    es,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleStreamReadProcessComplete(mockTimestamp, record);
        }
        @Test
        void testStreamReadProcessCompleteNullEvent() {
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doCallRealMethod().when(dataNodeSampleHandler).handleStreamReadProcessComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.streamReadProcessComplete(future,
                    null,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleStreamReadProcessComplete(mockTimestamp, record);
        }
    }

    @Nested
    class BatchReadCompleteTest {
        @BeforeEach
        void init() {
            events = new ArrayList<>();
            taskSampleHandler = mock(TaskSampleHandler.class);
        }
        @Test
        void testBatchReadCompleteNormal() {
            TapUpdateRecordEvent tapEvent = new  TapUpdateRecordEvent();
            HashMap<String, Object> after = new HashMap<>();
            after.put("id", "ddd");
            tapEvent.after(after);
            HashMap<String, Object> before = new HashMap<>();
            before.put("id", "ddd");
            tapEvent.before(before);
            events.add(tapEvent);
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doNothing().when(taskSampleHandler).handleBatchReadAccept(record);
            doNothing().when(dataNodeSampleHandler).handleBatchReadReadComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.batchReadComplete(future,
                    events,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler,mockTimestamp);
            verify(taskSampleHandler, times(1)).handleBatchReadAccept(record);
            verify(dataNodeSampleHandler, times(1)).handleBatchReadReadComplete(mockTimestamp, record);
        }

        @Test
        void testBatchReadCompleteWithNullDataNodeSampleHandler() {
            dataNodeSampleHandlers.remove(nodeId);
            TapUpdateRecordEvent tapEvent = new  TapUpdateRecordEvent();
            HashMap<String, Object> after = new HashMap<>();
            after.put("id", "ddd");
            tapEvent.after(after);
            HashMap<String, Object> before = new HashMap<>();
            before.put("id", "ddd");
            tapEvent.before(before);
            events.add(tapEvent);
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doNothing().when(taskSampleHandler).handleBatchReadAccept(record);
            doNothing().when(dataNodeSampleHandler).handleBatchReadReadComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.batchReadComplete(future,
                    events,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler,mockTimestamp);
            verify(taskSampleHandler, times(1)).handleBatchReadAccept(record);
            verify(dataNodeSampleHandler, times(0)).handleBatchReadReadComplete(mockTimestamp, record);
        }
        @Test
        void testBatchReadCompleteEmptyEvent() {
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doNothing().when(taskSampleHandler).handleBatchReadAccept(record);
            doNothing().when(dataNodeSampleHandler).handleBatchReadReadComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.batchReadComplete(future,
                    events,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleBatchReadReadComplete(mockTimestamp, record);
            verify(taskSampleHandler, times(0)).handleStreamReadAccept(record);
        }
        @Test
        void testBatchReadCompleteNullEvent() {
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doNothing().when(taskSampleHandler).handleBatchReadAccept(record);
            doNothing().when(dataNodeSampleHandler).handleBatchReadReadComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.batchReadComplete(future,
                    null,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleBatchReadReadComplete(mockTimestamp, record);
            verify(taskSampleHandler, times(0)).handleStreamReadAccept(record);
        }
    }

    @Nested
    class BatchReadProcessCompleteTest {
        @BeforeEach
        void init() {
            es = new ArrayList<>();
            taskSampleHandler = mock(TaskSampleHandler.class);
        }

        @Test
        void testBatchReadProcessCompleteNormal() {
            TapdataEvent e = new TapdataEvent();
            TapUpdateRecordEvent tapEvent = new  TapUpdateRecordEvent();
            HashMap<String, Object> after = new HashMap<>();
            after.put("id", "ddd");
            tapEvent.after(after);
            HashMap<String, Object> before = new HashMap<>();
            before.put("id", "ddd");
            tapEvent.before(before);
            e.setTapEvent(tapEvent);
            es.add(e);
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doCallRealMethod().when(dataNodeSampleHandler).handleBatchReadProcessComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.batchReadProcessComplete(future,
                    es,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,mockTimestamp);
            verify(dataNodeSampleHandler, times(1)).handleBatchReadProcessComplete(mockTimestamp, record);
        }
        @Test
        void testBatchReadProcessCompleteEmptyEvent() {
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doCallRealMethod().when(dataNodeSampleHandler).handleBatchReadProcessComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.batchReadProcessComplete(future,
                    es,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleBatchReadProcessComplete(mockTimestamp, record);
        }
        @Test
        void testBatchReadProcessCompleteNullEvent() {
            record = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            doCallRealMethod().when(dataNodeSampleHandler).handleBatchReadProcessComplete(mockTimestamp, record);
            ObservableAspectTaskUtil.batchReadProcessComplete(future,
                    null,
                    record,
                    nodeId,
                    dataNodeSampleHandlers,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleBatchReadProcessComplete(mockTimestamp, record);
        }
    }
}

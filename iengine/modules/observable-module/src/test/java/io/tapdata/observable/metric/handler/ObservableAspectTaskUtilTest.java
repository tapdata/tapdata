package io.tapdata.observable.metric.handler;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.observable.metric.util.SyncGetMemorySizeHandler;
import org.junit.jupiter.api.Assertions;
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
    HandlerUtil.EventTypeRecorder event;

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
            doCallRealMethod().when(taskSampleHandler).handleStreamReadAccept(any(HandlerUtil.EventTypeRecorder.class));
            doCallRealMethod().when(dataNodeSampleHandler).handleStreamReadReadComplete(anyLong(), any(HandlerUtil.EventTypeRecorder.class));
        }
        @Test
        void testStreamReadCompleteNormal() throws InterruptedException {
            TapUpdateRecordEvent tapEvent = new  TapUpdateRecordEvent();
            HashMap<String, Object> after = new HashMap<>();
            after.put("id", "ddd");
            tapEvent.after(after);
            HashMap<String, Object> before = new HashMap<>();
            before.put("id", "ddd");
            tapEvent.before(before);
            events.add(tapEvent);
            event = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            ObservableAspectTaskUtil.streamReadComplete(future,
                    events,
                    event,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler,mockTimestamp);
            verify(dataNodeSampleHandler, times(1)).handleStreamReadReadComplete(mockTimestamp, event);
            verify(taskSampleHandler, times(1)).handleStreamReadAccept(event);
        }
        @Test
        void testStreamReadCompleteEmptyEvent() throws InterruptedException {
            event = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            ObservableAspectTaskUtil.streamReadComplete(future,
                    events,
                    event,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleStreamReadReadComplete(mockTimestamp, event);
            verify(taskSampleHandler, times(0)).handleStreamReadAccept(event);
        }
        @Test
        void testStreamReadCompleteNullEvent() throws InterruptedException {
            event = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            ObservableAspectTaskUtil.streamReadComplete(future,
                    null,
                    event,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleStreamReadReadComplete(mockTimestamp, event);
            verify(taskSampleHandler, times(0)).handleStreamReadAccept(event);
        }
    }

    @Nested
    class StreamReadProcessCompleteTest {
        @BeforeEach
        void init() {
            es = new ArrayList<>();
            taskSampleHandler = mock(TaskSampleHandler.class);
            doCallRealMethod().when(dataNodeSampleHandler).handleStreamReadProcessComplete(anyLong(), any(HandlerUtil.EventTypeRecorder.class));
        }
        @Test
        void testStreamReadProcessCompleteNormal() throws InterruptedException {
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
            event = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            ObservableAspectTaskUtil.streamReadProcessComplete(future,
                    es,
                    event,
                    nodeId,
                    dataNodeSampleHandlers,mockTimestamp);
            verify(dataNodeSampleHandler, times(1)).handleStreamReadProcessComplete(mockTimestamp, event);
        }
        @Test
        void testStreamReadProcessCompleteEmptyEvent() throws InterruptedException {
            event = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            ObservableAspectTaskUtil.streamReadProcessComplete(future,
                    es,
                    event,
                    nodeId,
                    dataNodeSampleHandlers,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleStreamReadProcessComplete(mockTimestamp, event);
        }
        @Test
        void testStreamReadProcessCompleteNullEvent() throws InterruptedException {
            event = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            ObservableAspectTaskUtil.streamReadProcessComplete(future,
                    null,
                    event,
                    nodeId,
                    dataNodeSampleHandlers,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleStreamReadProcessComplete(mockTimestamp, event);
        }
    }

    @Nested
    class BatchReadCompleteTest {
        @BeforeEach
        void init() {
            events = new ArrayList<>();
            taskSampleHandler = mock(TaskSampleHandler.class);
            doCallRealMethod().when(taskSampleHandler).handleBatchReadAccept(any(HandlerUtil.EventTypeRecorder.class));
            doCallRealMethod().when(dataNodeSampleHandler).handleBatchReadReadComplete(anyLong(), any(HandlerUtil.EventTypeRecorder.class));
        }
        @Test
        void testBatchReadCompleteNormal() throws InterruptedException {
            TapUpdateRecordEvent tapEvent = new  TapUpdateRecordEvent();
            HashMap<String, Object> after = new HashMap<>();
            after.put("id", "ddd");
            tapEvent.after(after);
            HashMap<String, Object> before = new HashMap<>();
            before.put("id", "ddd");
            tapEvent.before(before);
            events.add(tapEvent);
            event = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            ObservableAspectTaskUtil.batchReadComplete(future,
                    events,
                    event,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler,mockTimestamp);
            verify(dataNodeSampleHandler, times(1)).handleBatchReadReadComplete(mockTimestamp, event);
            verify(taskSampleHandler, times(0)).handleBatchReadAccept(event);
        }
        @Test
        void testBatchReadCompleteEmptyEvent() throws InterruptedException {
            event = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            ObservableAspectTaskUtil.batchReadComplete(future,
                    events,
                    event,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleBatchReadReadComplete(mockTimestamp, event);
            verify(taskSampleHandler, times(0)).handleStreamReadAccept(event);
        }
        @Test
        void testBatchReadCompleteNullEvent() throws InterruptedException {
            event = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            ObservableAspectTaskUtil.batchReadComplete(future,
                    null,
                    event,
                    nodeId,
                    dataNodeSampleHandlers,
                    taskSampleHandler,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleBatchReadReadComplete(mockTimestamp, event);
            verify(taskSampleHandler, times(0)).handleStreamReadAccept(event);
        }
    }

    @Nested
    class BatchReadProcessCompleteTest {
        @BeforeEach
        void init() {
            es = new ArrayList<>();
            taskSampleHandler = mock(TaskSampleHandler.class);
            doCallRealMethod().when(dataNodeSampleHandler).handleBatchReadProcessComplete(anyLong(), any(HandlerUtil.EventTypeRecorder.class));
        }

        @Test
        void testBatchReadProcessCompleteNormal() throws InterruptedException {
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
            event = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            ObservableAspectTaskUtil.batchReadProcessComplete(future,
                    es,
                    event,
                    nodeId,
                    dataNodeSampleHandlers,mockTimestamp);
            verify(dataNodeSampleHandler, times(1)).handleBatchReadProcessComplete(mockTimestamp, event);
        }
        @Test
        void testBatchReadProcessCompleteEmptyEvent() throws InterruptedException {
            event = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            ObservableAspectTaskUtil.batchReadProcessComplete(future,
                    es,
                    event,
                    nodeId,
                    dataNodeSampleHandlers,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleBatchReadProcessComplete(mockTimestamp, event);
        }
        @Test
        void testBatchReadProcessCompleteNullEvent() throws InterruptedException {
            event = syncGetMemorySizeHandler.getEventTypeRecorderSyncTapEvent(events);
            ObservableAspectTaskUtil.batchReadProcessComplete(future,
                    null,
                    event,
                    nodeId,
                    dataNodeSampleHandlers,mockTimestamp);
            verify(dataNodeSampleHandler, times(0)).handleBatchReadProcessComplete(mockTimestamp, event);
        }
    }
}

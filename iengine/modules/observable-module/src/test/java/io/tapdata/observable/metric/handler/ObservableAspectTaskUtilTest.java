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

import static org.mockito.Mockito.mock;

public class ObservableAspectTaskUtilTest {
    CompletableFuture<Void> future;
    List<TapEvent> events;
    SyncGetMemorySizeHandler syncGetMemorySizeHandler;
    String nodeId;
    Map<String, DataNodeSampleHandler> dataNodeSampleHandlers;
    TaskSampleHandler taskSampleHandler;
    List<TapdataEvent> es;
    DataNodeSampleHandler dataNodeSampleHandler;

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
            ObservableAspectTaskUtil.streamReadComplete(future, events, syncGetMemorySizeHandler, nodeId, dataNodeSampleHandlers, taskSampleHandler);
        }
        @Test
        void testStreamReadCompleteEmptyEvent() {
            ObservableAspectTaskUtil.streamReadComplete(future, events, syncGetMemorySizeHandler, nodeId, dataNodeSampleHandlers, taskSampleHandler);
        }
        @Test
        void testStreamReadCompleteNullEvent() {
            ObservableAspectTaskUtil.streamReadComplete(future, null, syncGetMemorySizeHandler, nodeId, dataNodeSampleHandlers, taskSampleHandler);
        }
    }

    @Nested
    class StreamReadProcessCompleteTest {
        @BeforeEach
        void init() {
            es = new ArrayList<>();
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
            ObservableAspectTaskUtil.streamReadProcessComplete(future, es, syncGetMemorySizeHandler, nodeId, dataNodeSampleHandlers);
        }
        @Test
        void testStreamReadProcessCompleteEmptyEvent() {
            ObservableAspectTaskUtil.streamReadProcessComplete(future, es, syncGetMemorySizeHandler, nodeId, dataNodeSampleHandlers);
        }
        @Test
        void testStreamReadProcessCompleteNullEvent() {
            ObservableAspectTaskUtil.streamReadProcessComplete(future, null, syncGetMemorySizeHandler, nodeId, dataNodeSampleHandlers);
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
            ObservableAspectTaskUtil.batchReadComplete(future, events, syncGetMemorySizeHandler, nodeId, dataNodeSampleHandlers, taskSampleHandler);
        }
        @Test
        void testBatchReadCompleteEmptyEvent() {
            ObservableAspectTaskUtil.batchReadComplete(future, events, syncGetMemorySizeHandler, nodeId, dataNodeSampleHandlers, taskSampleHandler);
        }
        @Test
        void testBatchReadCompleteNullEvent() {
            ObservableAspectTaskUtil.batchReadComplete(future, null, syncGetMemorySizeHandler, nodeId, dataNodeSampleHandlers, taskSampleHandler);
        }
    }

    @Nested
    class BatchReadProcessCompleteTest {
        @BeforeEach
        void init() {
            es = new ArrayList<>();
        }

        @Test
        void testBatchReadProcessCompleteNormal(){
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
            ObservableAspectTaskUtil.batchReadProcessComplete(future, es, syncGetMemorySizeHandler, nodeId, dataNodeSampleHandlers);
        }
        @Test
        void testBatchReadProcessCompleteEmptyEvent(){
            ObservableAspectTaskUtil.batchReadProcessComplete(future, es, syncGetMemorySizeHandler, nodeId, dataNodeSampleHandlers);
        }
        @Test
        void testBatchReadProcessCompleteNullEvent(){
            ObservableAspectTaskUtil.batchReadProcessComplete(future, null, syncGetMemorySizeHandler, nodeId, dataNodeSampleHandlers);
        }
    }
}

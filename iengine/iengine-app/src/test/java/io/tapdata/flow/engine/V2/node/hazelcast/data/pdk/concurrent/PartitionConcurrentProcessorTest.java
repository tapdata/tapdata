package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent;

import com.tapdata.entity.*;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.KeysPartitioner;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.Partitioner;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector.PartitionKeySelector;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector.TapEventPartitionKeySelector;
import io.tapdata.utils.UnitTestUtils;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.internal.verification.Times;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/21 10:31 Create
 */
class PartitionConcurrentProcessorTest {
    private static final Logger logger = LogManager.getLogger(PartitionConcurrentProcessorTest.class);

    String taskId = "65fbaa3b064b2664cb59c424";
    Partitioner<TapdataEvent, List<Object>> partitioner;
    PartitionKeySelector<TapEvent, Object, Map<String, Object>> keySelector;
    Consumer<List<TapdataEvent>> eventProcessor;
    Consumer<TapdataEvent> flushOffset;
    PartitionConcurrentProcessor.ErrorHandler<Throwable, String> errorHandler;
    Supplier<Boolean> nodeRunning;
    TaskDto taskDto;

    @BeforeEach
    void setUp() {
        partitioner = new KeysPartitioner();
        keySelector = new TapEventPartitionKeySelector(tapEvent -> Collections.singletonList("id"));
        eventProcessor = mock(Consumer.class);
        flushOffset = mock(Consumer.class);
        errorHandler = mock(PartitionConcurrentProcessor.ErrorHandler.class);
        nodeRunning = mock(Supplier.class);
        taskDto = new TaskDto();
        taskDto.setId(new ObjectId(taskId));
    }

    @Test
    void testPartitionerNotNull() throws Exception {
        assertThrows(RuntimeException.class, () -> new PartitionConcurrentProcessor(1, 500, null, keySelector, eventProcessor, flushOffset, errorHandler, nodeRunning, taskDto));
    }

    @Test
    void testPartitionKeySelectorNotNull() throws Exception {
        assertThrows(RuntimeException.class, () -> new PartitionConcurrentProcessor(1, 500, partitioner, null, eventProcessor, flushOffset, errorHandler, nodeRunning, taskDto));
    }

    @Test
    void testEmptyEvents() throws Exception {
        PartitionConcurrentProcessor processor = new PartitionConcurrentProcessor(1, 500, partitioner, keySelector, eventProcessor, flushOffset, errorHandler, nodeRunning, taskDto);
        when(nodeRunning.get()).thenReturn(true);

        try {
            processor.start();
            processor.process(Collections.singletonList(generateInsertEvent("i", 1, null)), false);
            processor.process(Collections.emptyList(), false);
        } finally {
            processor.forceStop();
        }
    }

    @Test
    void testEventOrders() throws Exception {
        Map<String, Boolean> syncMap = new ConcurrentHashMap<>(); // executed to set true
        Map<String, Boolean> assertList = new LinkedHashMap<>(); // executed to assert results

        List<TapdataEvent> list = new ArrayList<>();
        list.add(generateInsertEvent("i", 1, null)); // need before ddl1
        list.add(generateInsertEvent("u", 2, 2)); // need before ddl1
        list.add(generateInsertEvent("d", 3, 3)); // need before ddl1
        list.add(generateDDLEvent(new TapField("ddl1", "string"))); // need after 1,2,3
        list.add(generateInsertEvent("u", 4, 40)); // need after ddl1
        list.add(generateInsertEvent("u", 5, null)); // need after ddl1
        list.add(generateInsertEvent("u", 6, null)); // need after ddl1
        list.add(generateDDLEvent(new TapField("ddl2", "string"))); // need after ddl1,1,2,3,4,5,6
        list.add(generateDDLEvent(new TapField("ddl3", "string"))); // need after ddl1,ddl2,1,2,3,4,5,6
        list.add(generateInsertEvent("i", 7, null));
        list.add(new TapdataCompleteSnapshotEvent());
        list.add(new TapdataCompleteTableSnapshotEvent());
        list.add(new TapdataStartingCdcEvent());
        list.add(TapdataStartedCdcEvent.create());
        list.add(generateInsertEvent("u", 8, null));

        PartitionConcurrentProcessor processor = new PartitionConcurrentProcessor(2, 500, partitioner, keySelector, eventProcessor, flushOffset, errorHandler, nodeRunning, taskDto);

        when(nodeRunning.get()).thenReturn(true);

        doAnswer(invocationOnMock -> {
            logger.error(invocationOnMock.getArgument(1, String.class), invocationOnMock.getArgument(0, Throwable.class));
            return null;
        }).when(errorHandler).accept(any(), any());

        CountDownLatch countDownLatch = new CountDownLatch(list.size() + 2);
        doAnswer(invocationOnMock -> {
            List<TapdataEvent> events = invocationOnMock.getArgument(0);
            for (TapdataEvent e : events) {
                countDownLatch.countDown();
                if (e.getTapEvent() instanceof TapInsertRecordEvent) {
                    String id = ((TapInsertRecordEvent) e.getTapEvent()).getAfter().get("id").toString();
                    syncMap.put(id, true);
                    assertDMLEvents(id, syncMap, assertList);
                } else if (e.getTapEvent() instanceof TapUpdateRecordEvent) {
                    String id = ((TapUpdateRecordEvent) e.getTapEvent()).getAfter().get("id").toString();
                    syncMap.put(id, true);
                    assertDMLEvents(id, syncMap, assertList);
                } else if (e.getTapEvent() instanceof TapDeleteRecordEvent) {
                    String id = ((TapDeleteRecordEvent) e.getTapEvent()).getBefore().get("id").toString();
                    syncMap.put(id, true);
                    assertDMLEvents(id, syncMap, assertList);
                } else if (e.getTapEvent() instanceof TapNewFieldEvent) {
                    String id = ((TapNewFieldEvent) e.getTapEvent()).getNewFields().get(0).getName();
                    syncMap.put(id, true);
                    assertDDLEvents(id, syncMap, assertList);
                } else if (e instanceof TapdataCompleteSnapshotEvent) {
                    syncMap.put(e.getClass().getName(), true);
                    assertCompleteSnapshot(e, assertList, syncMap);
                } else if (e instanceof TapdataCompleteTableSnapshotEvent) {
                    syncMap.put(e.getClass().getName(), true);
                    assertCompleteTableSnapshot(e, assertList, syncMap);
                } else if (e instanceof TapdataStartingCdcEvent) {
                    syncMap.put(e.getClass().getName(), true);
                    assertStartingCdc(e, assertList, syncMap);
                } else if (e instanceof TapdataStartedCdcEvent) {
                    syncMap.put(e.getClass().getName(), true);
                    assertStartedCdc(e, assertList, syncMap);
                }
            }
            return null;
        }).when(eventProcessor).accept(anyList());

        try {
            processor.start();
            processor.process(list, false);
            processor.process(Collections.singletonList(generateInsertEvent("i", 8, null)), false);
            processor.process(Collections.singletonList(generateDDLEvent(new TapField("ddl2", "string"))), true);
            assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        } finally {
            processor.stop();
        }

        assertFalse(processor.isRunning());
        logger.info("assertList: {}", String.join(" >> ", assertList.keySet()));
        for (Map.Entry<String, Boolean> entry : assertList.entrySet()) {
            assertTrue(entry.getValue(), "key: " + entry.getKey());
        }
    }

    private static void assertCompleteSnapshot(TapdataEvent e, Map<String, Boolean> assertList, Map<String, Boolean> syncMap) {
        assertList.put(e.getClass().getName(),
                syncMap.containsKey("1")
                && syncMap.containsKey("2")
                && syncMap.containsKey("3")
                && syncMap.containsKey("ddl1")
                && syncMap.containsKey("4")
                && syncMap.containsKey("5")
                && syncMap.containsKey("6")
                && syncMap.containsKey("ddl2")
                && syncMap.containsKey("ddl3")
                && syncMap.containsKey("7")
                && !syncMap.containsKey(TapdataCompleteTableSnapshotEvent.class.getName())
                && !syncMap.containsKey(TapdataStartingCdcEvent.class.getName())
                && !syncMap.containsKey(TapdataStartedCdcEvent.class.getName())
                && !syncMap.containsKey("8")
        );
    }

    private static void assertCompleteTableSnapshot(TapdataEvent e, Map<String, Boolean> assertList, Map<String, Boolean> syncMap) {
        assertList.put(e.getClass().getName(),
                syncMap.containsKey("1")
                        && syncMap.containsKey("2")
                        && syncMap.containsKey("3")
                        && syncMap.containsKey("ddl1")
                        && syncMap.containsKey("4")
                        && syncMap.containsKey("5")
                        && syncMap.containsKey("6")
                        && syncMap.containsKey("ddl2")
                        && syncMap.containsKey("ddl3")
                        && syncMap.containsKey("7")
                        && syncMap.containsKey(TapdataCompleteSnapshotEvent.class.getName())
                        && !syncMap.containsKey(TapdataStartingCdcEvent.class.getName())
                        && !syncMap.containsKey(TapdataStartedCdcEvent.class.getName())
                        && !syncMap.containsKey("8")
        );
    }

    private static void assertStartingCdc(TapdataEvent e, Map<String, Boolean> assertList, Map<String, Boolean> syncMap) {
        assertList.put(e.getClass().getName(),
                syncMap.containsKey("1")
                        && syncMap.containsKey("2")
                        && syncMap.containsKey("3")
                        && syncMap.containsKey("ddl1")
                        && syncMap.containsKey("4")
                        && syncMap.containsKey("5")
                        && syncMap.containsKey("6")
                        && syncMap.containsKey("ddl2")
                        && syncMap.containsKey("ddl3")
                        && syncMap.containsKey("7")
                        && syncMap.containsKey(TapdataCompleteSnapshotEvent.class.getName())
                        && syncMap.containsKey(TapdataCompleteTableSnapshotEvent.class.getName())
                        && !syncMap.containsKey(TapdataStartedCdcEvent.class.getName())
                        && !syncMap.containsKey("8")
        );
    }

    private static void assertStartedCdc(TapdataEvent e, Map<String, Boolean> assertList, Map<String, Boolean> syncMap) {
        assertList.put(e.getClass().getName(),
                syncMap.containsKey("1")
                        && syncMap.containsKey("2")
                        && syncMap.containsKey("3")
                        && syncMap.containsKey("ddl1")
                        && syncMap.containsKey("4")
                        && syncMap.containsKey("5")
                        && syncMap.containsKey("6")
                        && syncMap.containsKey("ddl2")
                        && syncMap.containsKey("ddl3")
                        && syncMap.containsKey("7")
                        && syncMap.containsKey(TapdataCompleteSnapshotEvent.class.getName())
                        && syncMap.containsKey(TapdataCompleteTableSnapshotEvent.class.getName())
                        && syncMap.containsKey(TapdataStartingCdcEvent.class.getName())
                        && !syncMap.containsKey("8")
        );
    }

    private void assertDMLEvents(String id, Map<String, Boolean> syncMap, Map<String, Boolean> assertList) {
        switch (id) {
            case "1":
            case "2":
            case "3":
                assertList.put(id, !Boolean.TRUE.equals(syncMap.get("ddl1")));
                break;
            case "4":
            case "5":
            case "6":
                assertList.put(id, Boolean.TRUE.equals(syncMap.get("ddl1")));
                break;
            case "7":
                assertList.put(id, Boolean.TRUE.equals(syncMap.get("ddl1"))
                        && Boolean.TRUE.equals(syncMap.get("ddl2"))
                        && Boolean.TRUE.equals(syncMap.get("ddl3"))
                );
                break;
            case "8":
                assertList.put(id, Boolean.TRUE.equals(syncMap.get(TapdataStartedCdcEvent.class.getName())));
                break;
            default:
                break;
        }
    }

    private void assertDDLEvents(String id, Map<String, Boolean> syncMap, Map<String, Boolean> assertList) {
        switch (id) {
            case "ddl1":
                assertList.put(id, Boolean.TRUE.equals(syncMap.get("1"))
                        && Boolean.TRUE.equals(syncMap.get("2"))
                        && Boolean.TRUE.equals(syncMap.get("3"))
                );
                break;
            case "ddl2":
                assertList.put(id, Boolean.TRUE.equals(syncMap.get("1"))
                        && Boolean.TRUE.equals(syncMap.get("2"))
                        && Boolean.TRUE.equals(syncMap.get("3"))
                        && Boolean.TRUE.equals(syncMap.get("ddl1"))
                        && Boolean.TRUE.equals(syncMap.get("4"))
                        && Boolean.TRUE.equals(syncMap.get("5"))
                        && Boolean.TRUE.equals(syncMap.get("6"))
                );
                break;
            case "ddl3":
                assertList.put(id, Boolean.TRUE.equals(syncMap.get("1"))
                        && Boolean.TRUE.equals(syncMap.get("2"))
                        && Boolean.TRUE.equals(syncMap.get("3"))
                        && Boolean.TRUE.equals(syncMap.get("ddl1"))
                        && Boolean.TRUE.equals(syncMap.get("4"))
                        && Boolean.TRUE.equals(syncMap.get("5"))
                        && Boolean.TRUE.equals(syncMap.get("6"))
                        && Boolean.TRUE.equals(syncMap.get("ddl2"))
                        && !Boolean.TRUE.equals(syncMap.get("7"))
                );
                break;
        }
    }

    private TapdataEvent generateInsertEvent(String type, Integer afterId, Integer beforeId) {
        TapdataEvent tapdataEvent = new TapdataEvent();
        switch (type) {
            case "i":
                tapdataEvent.setTapEvent(TapInsertRecordEvent.create().after(new HashMap<String, Object>() {{
                    put("id", afterId);
                }}));
                break;
            case "u":
                TapUpdateRecordEvent updateRecordEvent = TapUpdateRecordEvent.create();
                if (null != afterId) updateRecordEvent.after(new HashMap<String, Object>() {{
                    put("id", afterId);
                }});
                if (null != beforeId) updateRecordEvent.before(new HashMap<String, Object>() {{
                    put("id", beforeId);
                }});
                tapdataEvent.setTapEvent(updateRecordEvent);
                break;
            case "d":
                tapdataEvent.setTapEvent(TapDeleteRecordEvent.create().before(new HashMap<String, Object>() {{
                    put("id", afterId);
                }}));
                break;
        }
        tapdataEvent.setStreamOffset(afterId);
        return tapdataEvent;
    }

    private TapdataEvent generateDDLEvent(TapField tapField) {
        TapNewFieldEvent event = new TapNewFieldEvent().field(tapField);
        TapdataEvent tapdataEvent = new TapdataEvent();
        tapdataEvent.setTapEvent(event);
        return tapdataEvent;
    }

    @Nested
    class UpdatePartitionValueEventTest {
        PartitionConcurrentProcessor processor;

        @BeforeEach
        void setUp() {
            processor = mock(PartitionConcurrentProcessor.class, CALLS_REAL_METHODS);
            UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "keySelector", new TapEventPartitionKeySelector(tapEvent -> Collections.singletonList("id")));
        }

        @Test
        void testUpdatePartitionValueEvent() {
            assertFalse(processor.updatePartitionValueEvent(TapUpdateRecordEvent.create().after(new HashMap<String, Object>() {{
                put("id", 1);
            }})), "testBeforeEmpty");
            assertFalse(processor.updatePartitionValueEvent(TapUpdateRecordEvent.create().before(new HashMap<String, Object>() {{
                put("id", 1);
            }})), "testAfterEmpty");
            assertTrue(processor.updatePartitionValueEvent(TapUpdateRecordEvent.create().after(new HashMap<String, Object>() {{
                put("id", 1);
            }}).before(new HashMap<String, Object>() {{
                put("id", 2);
            }})), "testModify");
            assertFalse(processor.updatePartitionValueEvent(TapUpdateRecordEvent.create().after(new HashMap<String, Object>() {{
                put("id", 1);
            }}).before(new HashMap<String, Object>() {{
                put("id", 1);
            }})), "testNotModify");
        }
    }

    @Test
    @Timeout(2)
    void testWatermarkEventRunner() throws Exception {
        PartitionConcurrentProcessor processor = mock(PartitionConcurrentProcessor.class, CALLS_REAL_METHODS);
        UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "logger", logger);
        UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "taskDto", taskDto);
        UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "errorHandler", errorHandler);
        UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "flushOffset", mock(Consumer.class));
        UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "currentRunning", new AtomicBoolean(false));
        UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "nodeRunning", (Supplier<Boolean>) () -> true);

        String mockErrorMsg = "test-ex";
        AtomicInteger callIndex = new AtomicInteger(0);
        AtomicBoolean hasError = new AtomicBoolean(false);
        doAnswer(invocationOnMock -> {
            Runnable runnable = invocationOnMock.getArgument(1, Runnable.class);
            if (null != runnable) {
                runnable.run();
            }
            return true;
        }).when(processor).waitCountDownLath(any(), any());
        when(processor.isRunning()).thenReturn(true);
        doAnswer(invocationOnMock -> {
            switch (callIndex.addAndGet(1)) {
                case 1:
                    return null;
                case 2:
                    throw new RuntimeException(mockErrorMsg);
                case 3:
                    TapdataEvent event = new TapdataEvent();
                    event.setSourceTime(System.currentTimeMillis());
                    return new WatermarkEvent(1, event);
                case 4:
                    throw new InterruptedException();
                default:
                    return null;
            }
        }).when(processor).pollWatermarkEvent();
        doAnswer(invocationOnMock -> {
            hasError.compareAndSet(false, true);
            Throwable e = invocationOnMock.getArgument(0, Throwable.class);
            if (null != e && !mockErrorMsg.equals(e.getMessage())) {
                logger.error(invocationOnMock.getArgument(1, String.class), invocationOnMock.getArgument(0, Throwable.class));
            }
            return null;
        }).when(errorHandler).accept(any(), anyString());

        // while 1: watermarkQueue poll null
        // while 2: watermarkQueue poll exception
        // while 3: watermarkQueue poll normal event
        // while 4: watermarkQueue poll interrupted
        processor.watermarkEventRunner();
        assertEquals(true, hasError.get());
        assertEquals(4, callIndex.get());

        // if waitCountDownLath false break while
        when(processor.isRunning()).thenReturn(true);
        when(processor.pollWatermarkEvent()).thenReturn(new WatermarkEvent(1, null));
        when(processor.waitCountDownLath(any(), any())).thenReturn(false);
        processor.watermarkEventRunner();

        // if isRunning=false break while
        when(processor.isRunning()).thenReturn(false);
        when(processor.waitCountDownLath(any(), any())).thenReturn(false);
        processor.watermarkEventRunner();
    }

    @Nested
    class Offer2QueueIfRunningTest {
        PartitionConcurrentProcessor processor;

        @BeforeEach
        void setUp() {
            processor = mock(PartitionConcurrentProcessor.class, CALLS_REAL_METHODS);
            UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "currentRunning", new AtomicBoolean(false));
            UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "nodeRunning", (Supplier<Boolean>) () -> false);
        }

        @Test
        void testNotRunning() throws InterruptedException {
            LinkedBlockingQueue<PartitionEvent<TapdataEvent>> queue = mock(LinkedBlockingQueue.class);
            processor.offer2QueueIfRunning(queue, null, null);
            verify(queue, new Times(0)).offer(any(), anyLong(), any());
        }

        @Test
        void testTraceEnabled() throws InterruptedException {
            boolean offsetFalse2Log = false;
            boolean offsetFalseNotLog = false;
            boolean offsetTrue = true;
            LinkedBlockingQueue<PartitionEvent<TapdataEvent>> queue = mock(LinkedBlockingQueue.class);
            when(queue.offer(any(), anyLong(), any())).thenReturn(offsetFalse2Log, offsetFalseNotLog, offsetTrue);

            Logger logger = mock(Logger.class);
            when(logger.isTraceEnabled()).thenReturn(true, false);
            UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "logger", logger);
            when(processor.isRunning()).thenReturn(true);

            // 1. first to call trace log
            // 2. second not trace log
            // 3. third to offer success
            processor.offer2QueueIfRunning(queue, null, null);
        }
    }

    @Nested
    class WaitCountDownLathTest {
        PartitionConcurrentProcessor processor;

        @BeforeEach
        void setUp() {
            processor = mock(PartitionConcurrentProcessor.class, CALLS_REAL_METHODS);
            UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "currentRunning", new AtomicBoolean(false));
            UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "nodeRunning", (Supplier<Boolean>) () -> false);
        }

        @Test
        void testNotRunning() throws InterruptedException {
            CountDownLatch countDownLatch = mock(CountDownLatch.class);
            when(countDownLatch.await(anyLong(), any())).thenReturn(false, false, true);

            assertFalse(processor.waitCountDownLath(countDownLatch, () -> {
            }));
        }

        @Test
        void testTraceEnabled() throws InterruptedException {
            CountDownLatch countDownLatch = mock(CountDownLatch.class);
            when(countDownLatch.await(anyLong(), any())).thenReturn(false, false, true);

            Logger logger = mock(Logger.class);
            when(logger.isTraceEnabled()).thenReturn(true, false);
            UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "logger", logger);

            when(processor.isRunning()).thenReturn(true);

            // 1. call wait 3 times
            // 2. first callback true
            // 3. second callback false
            // 4. third wait=ture
            AtomicInteger callCounter = new AtomicInteger(0);
            assertTrue(processor.waitCountDownLath(countDownLatch, () -> callCounter.addAndGet(1)));
            assertEquals(1, callCounter.get());
        }
    }

    @Nested
    class ToSingleModeTest {
        PartitionConcurrentProcessor processor;

        @BeforeEach
        void setUp() throws InterruptedException {
            processor = mock(PartitionConcurrentProcessor.class);
            doCallRealMethod().when(processor).toSingleMode(any(), any(), any());
        }

        @Test
        void testEventNullAndNotSingleMode() throws InterruptedException {
            AtomicBoolean singleMode = new AtomicBoolean(false);
            assertFalse(processor.toSingleMode(null, null, singleMode));
            assertFalse(singleMode.get());
            verify(processor, new Times(0)).generateBarrierEvent();
        }

        @Test
        void testEventNullAndSingleMode() throws InterruptedException {
            AtomicBoolean singleMode = new AtomicBoolean(true);
            assertFalse(processor.toSingleMode(null, null, singleMode));
            assertFalse(singleMode.get());
            verify(processor, new Times(1)).generateBarrierEvent();
        }

        @Test
        void testSingleModeAndDeleteEvent() throws InterruptedException {
            AtomicBoolean singleMode = new AtomicBoolean(true);
            TapDeleteRecordEvent tapEvent = new TapDeleteRecordEvent();
            assertTrue(processor.toSingleMode(tapEvent, Arrays.asList("1", null), singleMode));
            assertTrue(singleMode.get());
            verify(processor, new Times(0)).generateBarrierEvent();
        }

        @Test
        void testNotSingleModeAndDeleteEvent() throws InterruptedException {
            AtomicBoolean singleMode = new AtomicBoolean(false);
            TapDeleteRecordEvent tapEvent = new TapDeleteRecordEvent();
            assertTrue(processor.toSingleMode(tapEvent, Collections.singletonList(null), singleMode));
            assertTrue(singleMode.get());
            verify(processor, new Times(1)).generateBarrierEvent();
        }

        @Test
        void testDeleteEventNonKeys() throws InterruptedException {
            AtomicBoolean singleMode = new AtomicBoolean(false);
            TapDeleteRecordEvent tapEvent = new TapDeleteRecordEvent();
            assertFalse(processor.toSingleMode(tapEvent, Collections.emptyList(), singleMode));
            assertFalse(singleMode.get());
            verify(processor, new Times(0)).generateBarrierEvent();
        }
    }

    @Nested
    class ProcessDMLTest {
        @Test
        void testInterrupted() throws InterruptedException {
            TapdataEvent tapdataEvent = mock(TapdataEvent.class);
            PartitionConcurrentProcessor processor = mock(PartitionConcurrentProcessor.class, CALLS_REAL_METHODS);
            doAnswer(invocationOnMock -> {throw new InterruptedException();}).when(processor).getTapRecordEventData(any());

            AtomicBoolean singleMode = new AtomicBoolean(false);
            assertThrows(InterruptedException.class, () -> processor.processDML(tapdataEvent, singleMode));
        }
        @Test
        void testError() throws InterruptedException {
            TapdataEvent tapdataEvent = mock(TapdataEvent.class);
            PartitionConcurrentProcessor processor = mock(PartitionConcurrentProcessor.class, CALLS_REAL_METHODS);
            doAnswer(invocationOnMock -> {throw new RuntimeException("xxx");}).when(processor).getTapRecordEventData(any());

            AtomicBoolean singleMode = new AtomicBoolean(false);
            assertThrows(RuntimeException.class, () -> processor.processDML(tapdataEvent, singleMode));
        }
    }

    @Test
    void testOther() throws InterruptedException {
        PartitionConcurrentProcessor processor = mock(PartitionConcurrentProcessor.class, CALLS_REAL_METHODS);
        UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "partitionsQueue", new ArrayList<>());
        UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "currentRunning", new AtomicBoolean(false));
        UnitTestUtils.injectField(PartitionConcurrentProcessor.class, processor, "nodeRunning", (Supplier<Boolean>) () -> false);

        // test not running
        processor.process(Collections.singletonList(generateInsertEvent("i", 1, null)), false);
        verify(processor, new Times(1)).isRunning();

        // test empty queue
        assertNull(processor.generateBarrierEvent());
        processor.generateWatermarkEvent(null);

        // test stop
        processor.stop();

        //test stop InterruptedException
        doAnswer(invocationOnMock -> {
            throw new InterruptedException("");
        }).when(processor).waitingForProcessToCurrent();
        processor.stop();

        // test process InterruptedException
        doAnswer(invocationOnMock -> {
            throw new InterruptedException("");
        }).when(processor).isRunning();
        processor.process(Collections.singletonList(generateInsertEvent("i", 1, null)), false);
        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    @SneakyThrows
    void testProcessHeartBeat() {
        List<TapdataEvent> syncList = new ArrayList<>();
        List<TapdataEvent> tapdataEvents = new ArrayList<>();

        tapdataEvents.add(new TapdataHeartbeatEvent());
        tapdataEvents.add(new TapdataHeartbeatEvent());
        tapdataEvents.add(new TapdataHeartbeatEvent());
        tapdataEvents.add(new TapdataHeartbeatEvent());
        tapdataEvents.add(new TapdataHeartbeatEvent());

        PartitionConcurrentProcessor processor = new PartitionConcurrentProcessor(2, 500, partitioner, keySelector, eventProcessor, flushOffset, errorHandler, nodeRunning, taskDto);

        when(nodeRunning.get()).thenReturn(true);

        doAnswer(invocationOnMock -> {
            logger.error(invocationOnMock.getArgument(1, String.class), invocationOnMock.getArgument(0, Throwable.class));
            return null;
        }).when(errorHandler).accept(any(), any());

        CountDownLatch countDownLatch = new CountDownLatch(tapdataEvents.size());
        doAnswer(invocationOnMock -> {
            List<TapdataEvent> events = invocationOnMock.getArgument(0);
            for (TapdataEvent e : events) {
                syncList.add(e);
                countDownLatch.countDown();
            }
            return null;
        }).when(eventProcessor).accept(anyList());

        try {
            processor.start();
            processor.process(tapdataEvents, false);
            assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        } finally {
            processor.stop();
        }

        assertFalse(processor.isRunning());
        assertEquals(tapdataEvents.size(), syncList.size());
    }
}

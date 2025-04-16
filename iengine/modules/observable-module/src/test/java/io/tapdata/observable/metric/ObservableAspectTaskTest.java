package io.tapdata.observable.metric;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.mongo.RestTemplateOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import io.tapdata.aspect.DropTableFuncAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.WriteRecordFuncAspect;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.aspect.*;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.observable.metric.handler.DataNodeSampleHandler;
import io.tapdata.observable.metric.handler.ProcessorNodeSampleHandler;
import io.tapdata.observable.metric.handler.TableSampleHandler;
import io.tapdata.observable.metric.handler.TaskSampleHandler;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.observable.metric.handler.*;
import io.tapdata.observable.metric.util.SyncGetMemorySizeHandler;
import io.tapdata.observable.metric.util.TapCompletableFutureTaskEx;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.core.utils.CommonUtils;
import net.sf.jsqlparser.statement.insert.Insert;
import org.apache.commons.lang3.RandomUtils;
import io.tapdata.observable.metric.util.TapCompletableFutureEx;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.*;


public class ObservableAspectTaskTest {
    ObservableAspectTask observableAspectTask;
    @BeforeEach
    void init() {
        observableAspectTask = new ObservableAspectTask();
        TaskDto taskDto = new TaskDto();
        taskDto.setId(ObjectId.get());
        ReflectionTestUtils.setField(observableAspectTask,"task",taskDto);
    }

    @Nested
    class InitCompletableFutureTest{
        @Test
        void testInitCompletableFuture() {
            observableAspectTask.initCompletableFuture();
            Assertions.assertNotNull(observableAspectTask.batchReadFuture);
            Assertions.assertNotNull(observableAspectTask.batchProcessFuture);
            Assertions.assertNotNull(observableAspectTask.streamProcessFuture);
            Assertions.assertNotNull(observableAspectTask.streamReadFuture);
            Assertions.assertNotNull(observableAspectTask.writeRecordFuture);
        }
    }

    @Test
    void testStartAndStop() {
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        taskDto.setType(TaskDto.TYPE_INITIAL_SYNC_CDC);
        taskDto.setTaskRecordId(UUID.randomUUID().toString());
        taskDto.setStartTime(new Date());
        observableAspectTask.setTask(taskDto);

        RestTemplateOperator mockRestTemplateOperator = mock(RestTemplateOperator.class);
        TaskSampleRetriever.getInstance().start(mockRestTemplateOperator);

        // test onStart and onStop if null handler
        observableAspectTask.onStart(new TaskStartAspect().task(taskDto));
        observableAspectTask.onStop(new TaskStopAspect().task(taskDto));

        // test onStop if handler not empty
        ReflectionTestUtils.setField(observableAspectTask, "tableSampleHandlers", new HashMap<String, TableSampleHandler>() {{
            put("test", mock(TableSampleHandler.class));
        }});
        ReflectionTestUtils.setField(observableAspectTask, "dataNodeSampleHandlers", new HashMap<String, DataNodeSampleHandler>() {{
            put("test", mock(DataNodeSampleHandler.class));
        }});
        ReflectionTestUtils.setField(observableAspectTask, "processorNodeSampleHandlers", new HashMap<String, ProcessorNodeSampleHandler>() {{
            put("test", mock(ProcessorNodeSampleHandler.class));
        }});
        observableAspectTask.onStop(new TaskStopAspect().task(taskDto));
    }

    @Nested
    class CloseCompletableFutureTest {
        @Test
        void testCloseCompletableFuture() {
            observableAspectTask.closeCompletableFuture();
            Assertions.assertNull(observableAspectTask.batchReadFuture);
            Assertions.assertNull(observableAspectTask.batchProcessFuture);
            Assertions.assertNull(observableAspectTask.streamProcessFuture);
            Assertions.assertNull(observableAspectTask.streamReadFuture);
            Assertions.assertNull(observableAspectTask.writeRecordFuture);
        }

        @Test
        void testCloseCompletableFuture1() {
            observableAspectTask.initCompletableFuture();
            observableAspectTask.batchReadFuture.thenRunAsync(() -> {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            });
            observableAspectTask.batchProcessFuture.thenRunAsync(() -> {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            });
            observableAspectTask.streamProcessFuture.thenRunAsync(() -> {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            });
            observableAspectTask.streamReadFuture.thenRunAsync(() -> {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            });
            observableAspectTask.writeRecordFuture.runAsync(() -> {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            });
            observableAspectTask.closeCompletableFuture();
            Assertions.assertNotNull(observableAspectTask.batchReadFuture);
            Assertions.assertNotNull(observableAspectTask.batchProcessFuture);
            Assertions.assertNotNull(observableAspectTask.streamProcessFuture);
            Assertions.assertNotNull(observableAspectTask.streamReadFuture);
            Assertions.assertNotNull(observableAspectTask.writeRecordFuture);

            Assertions.assertTrue(observableAspectTask.batchReadFuture.isDone());
            Assertions.assertTrue(observableAspectTask.batchProcessFuture.isDone());
            Assertions.assertTrue(observableAspectTask.streamProcessFuture.isDone());
            Assertions.assertTrue(observableAspectTask.streamReadFuture.isDone());
        }
    }

    @Nested
    class PrepareSyncGetMemorySizeHandlerTest {
        @Test
        void testPrepareSyncGetMemorySizeHandler() {
            SyncGetMemorySizeHandler handler = observableAspectTask.prepareSyncGetMemorySizeHandler();
            Assertions.assertNotNull(handler);
        }
    }

    @Nested
    class InterceptAspectsTest {
        @Test
        void testInterceptAspects() {
            List<Class<? extends Aspect>> list = observableAspectTask.interceptAspects();
            Assertions.assertNull(list);
        }
    }

    @Nested
    class OnInterceptAspectTest {
        @Test
        void testInterceptAspectTest() {
            AspectInterceptResult aspect = observableAspectTask.onInterceptAspect(null);
            Assertions.assertNull(aspect);
        }
        @Test
        void testInterceptAspectTest0() {
            AspectInterceptResult aspect = observableAspectTask.onInterceptAspect(new Aspect() {});
            Assertions.assertNull(aspect);
        }
    }

    @Nested
    class handleDropTableFunc {
        @Test
        void testHandleDropTableFuncInitWithTrue() {
            DropTableFuncAspect aspect = new DropTableFuncAspect();
            aspect.setInit(true);
            TaskSampleHandler taskSampleHandler =mock(TaskSampleHandler.class);
            observableAspectTask.handleDropTableFunc(aspect);
            verify(taskSampleHandler,times(0)).handleDdlEnd();
        }
        @Test
        void testHandleDropTableFuncInitWithFalse() {
            DropTableFuncAspect aspect = new DropTableFuncAspect();
            aspect.setInit(false);
            aspect.setState(20);
            DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
            aspect.setDataProcessorContext(dataProcessorContext);
            Node node = mock(Node.class);
            node.setId("id");
            when(dataProcessorContext.getNode()).thenReturn(node);
            TaskSampleHandler taskSampleHandler =mock(TaskSampleHandler.class);
            ReflectionTestUtils.setField(observableAspectTask,"taskSampleHandler",taskSampleHandler);
            ReflectionTestUtils.setField(observableAspectTask,"dataNodeSampleHandlers",new HashMap());

            observableAspectTask.handleDropTableFunc(aspect);
            verify(taskSampleHandler,times(1)).handleDdlEnd();
        }
    }

    @Nested
    class handleCreateTableFunc {
        @Test
        void testHandleCreateTableFuncInitWithTrue() {
            DropTableFuncAspect aspect = new DropTableFuncAspect();
            aspect.setInit(true);
            TaskSampleHandler taskSampleHandler =mock(TaskSampleHandler.class);
            observableAspectTask.handleDropTableFunc(aspect);
            verify(taskSampleHandler,times(0)).handleDdlEnd();
        }
        @Test
        void testHandleCreateTableFuncInitWithFalse() {
            DropTableFuncAspect aspect = new DropTableFuncAspect();
            aspect.setInit(false);
            aspect.setState(20);
            DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
            aspect.setDataProcessorContext(dataProcessorContext);
            Node node = mock(Node.class);
            node.setId("id");
            when(dataProcessorContext.getNode()).thenReturn(node);
            TaskSampleHandler taskSampleHandler =mock(TaskSampleHandler.class);
            ReflectionTestUtils.setField(observableAspectTask,"taskSampleHandler",taskSampleHandler);
            ReflectionTestUtils.setField(observableAspectTask,"dataNodeSampleHandlers",new HashMap());

            observableAspectTask.handleDropTableFunc(aspect);
            verify(taskSampleHandler,times(1)).handleDdlEnd();
        }
    }
    @Nested
    class handleTruncateTableFunc{
        DataNodeSampleHandler dataNodeSampleHandler;
        @BeforeEach
        void beforeInit(){
            dataNodeSampleHandler = mock(DataNodeSampleHandler.class);
            Map<String, DataNodeSampleHandler> dataNodeSampleHandlers = new HashMap<>();
            dataNodeSampleHandlers.put("testId", dataNodeSampleHandler);
            ReflectionTestUtils.setField(observableAspectTask, "dataNodeSampleHandlers", dataNodeSampleHandlers);
        }
        @Test
        void test1(){
            TruncateTableFuncAspect truncateTableFuncAspect = new TruncateTableFuncAspect();
            DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);

            Node node = mock(Node.class);
            when(node.getId()).thenReturn("testId");
            when(dataProcessorContext.getNode()).thenReturn(node);
            truncateTableFuncAspect.dataProcessorContext(dataProcessorContext).state(TruncateTableFuncAspect.STATE_START);
            observableAspectTask.handleTruncateTableFuncAspect(truncateTableFuncAspect);
            verify(dataNodeSampleHandler, times(1)).handleDdlStart();
        }
        @Test
        void test2(){
            TaskSampleHandler taskSampleHandler = mock(TaskSampleHandler.class);
            ReflectionTestUtils.setField(observableAspectTask,"taskSampleHandler",taskSampleHandler);
            TruncateTableFuncAspect truncateTableFuncAspect = new TruncateTableFuncAspect();
            DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
            Node node = mock(Node.class);
            when(node.getId()).thenReturn("testId");
            when(dataProcessorContext.getNode()).thenReturn(node);
            truncateTableFuncAspect.dataProcessorContext(dataProcessorContext).state(TruncateTableFuncAspect.STATE_END);
            observableAspectTask.handleTruncateTableFuncAspect(truncateTableFuncAspect);
            verify(dataNodeSampleHandler, times(1)).handleDdlEnd();
        }
    }
    @Nested
    class handleWriteRecordFunc {
        TapCompletableFutureEx writeRecordFuture;
        @BeforeEach
        void setUp(){
            writeRecordFuture = mock(TapCompletableFutureEx.class);
            ReflectionTestUtils.setField(observableAspectTask,"writeRecordFuture",writeRecordFuture);
            doAnswer(invocationOnMock -> {
                Runnable runnable = invocationOnMock.getArgument(0);
                runnable.run();
                return null;
            }).when(writeRecordFuture).thenRun(any());
        }
        @Test
        void testBatchSplit_isTaskSampleHandlerV2(){
            TaskSampleHandlerV2 taskSampleHandler = mock(TaskSampleHandlerV2.class);
            doNothing().when(taskSampleHandler).handleWriteBatchSplit();
            ReflectionTestUtils.setField(observableAspectTask,"taskSampleHandler",taskSampleHandler);
            TableNode tableNode = new TableNode();
            DataProcessorContext dataProcessorContext = DataProcessorContext.newBuilder().withNode(tableNode).build();
            WriteRecordFuncAspect aspect = new WriteRecordFuncAspect().state(WriteRecordFuncAspect.BATCH_SPLIT).dataProcessorContext(dataProcessorContext);
            observableAspectTask.handleWriteRecordFunc(aspect);
            verify(taskSampleHandler, times(1)).handleWriteBatchSplit();
        }

        @Test
        void testStateStart(){
            DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
            TableNode node = new TableNode();
            node.setId("id");
            when(dataProcessorContext.getNode()).thenReturn((Node)node);
            List<TapRecordEvent> results = new ArrayList<TapRecordEvent>();
            TapRecordEvent tapRecordEvent = new TapInsertRecordEvent();
            results.add(tapRecordEvent);
            TapTable tapTable = new TapTable();
            tapTable.setName("test");
            Map<String, DataNodeSampleHandler> dataNodeSampleHandlers = new HashMap<>();
            DataNodeSampleHandler dataNodeSampleHandler = mock(DataNodeSampleHandler.class);
            doNothing().when(dataNodeSampleHandler).handleWriteRecordStart(any(),any());
            doNothing().when(dataNodeSampleHandler).handleWriteRecordAccept(any(),any(),any());
            dataNodeSampleHandlers.put("id",dataNodeSampleHandler);
            ReflectionTestUtils.setField(observableAspectTask,"dataNodeSampleHandlers",dataNodeSampleHandlers);
            TaskSampleHandlerV2 taskSampleHandler = mock(TaskSampleHandlerV2.class);
            doNothing().when(taskSampleHandler).handleWriteRecordAccept(any(),any(),any());
            ReflectionTestUtils.setField(observableAspectTask,"taskSampleHandler",taskSampleHandler);
            ReflectionTestUtils.setField(observableAspectTask,"pipelineDelay",mock(PipelineDelayImpl.class));
            try(MockedStatic<HandlerUtil> handlerUtilMock = Mockito.mockStatic(HandlerUtil.class)){
                WriteRecordFuncAspect aspect = spy(new WriteRecordFuncAspect().state(WriteRecordFuncAspect.STATE_START).recordEvents(results).dataProcessorContext(dataProcessorContext).table(tapTable));
                handlerUtilMock.when(() -> HandlerUtil.countTapEvent(any())).thenReturn(mock(HandlerUtil.EventTypeRecorder.class));
                HandlerUtil.EventTypeRecorder inner = new HandlerUtil.EventTypeRecorder();
                inner.incrInsertTotal();
                ReflectionTestUtils.setField(inner,"newestEventTimestamp",1L);
                inner.incrProcessTimeTotal(1L, 1L);
                handlerUtilMock.when(() -> HandlerUtil.countTapEvent(any(),any())).thenReturn(inner);
                doAnswer(invocationOnMock -> {
                    BiConsumer<List<TapRecordEvent>, WriteListResult<TapRecordEvent>> biConsumer = invocationOnMock.getArgument(0);
                    biConsumer.accept(results,new WriteListResult<TapRecordEvent>());
                    return null;
                }).when(aspect).consumer(any());
                observableAspectTask.handleWriteRecordFunc(aspect);
                verify(dataNodeSampleHandler, times(1)).handleWriteRecordAccept(any(),any(),any());

            }
        }

        @Test
        void test_handleWriteBatchSplit(){
            TaskSampleHandlerV2 taskSampleHandler = mock(TaskSampleHandlerV2.class);
            doNothing().when(taskSampleHandler).handleWriteBatchSplit();
            ReflectionTestUtils.setField(observableAspectTask,"taskSampleHandler",taskSampleHandler);
            TaskBatchSplitAspect aspect = new TaskBatchSplitAspect();
            observableAspectTask.handleWriteBatchSplit(aspect);
            verify(taskSampleHandler, times(1)).handleWriteBatchSplit();
        }

        @Test
        void testStateStart_1() {
            TaskSampleRetriever.getInstance().start(mock(RestTemplateOperator.class));

            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            taskDto.setTaskRecordId(new ObjectId().toHexString());
            taskDto.setStartTime(new Date());
            taskDto.setSyncType("migrate");
            Node node = new DatabaseNode();
            node.setId("nodeId");
            List<SyncObjects> syncObjects = new ArrayList<>();
            SyncObjects syncObject = new SyncObjects();
            LinkedHashMap<String, String> tableNameRelation = new LinkedHashMap<>();
            tableNameRelation.put("test", "test");
            syncObject.setTableNameRelation(tableNameRelation);
            syncObjects.add(syncObject);
            ((DatabaseNode)node).setSyncObjects(syncObjects);

            ObservableAspectTask task = new ObservableAspectTask();
            task.initCompletableFuture();
            task.setTask(taskDto);

            Map<String, DataNodeSampleHandler> sampleHandler = new HashMap<>();

            sampleHandler.put(node.getId(), new DataNodeSampleHandler(taskDto, node));

            Map<String, TableSampleHandler> tableSampleHandlers = new HashMap<>();
            TableSampleHandler tableSampleHandler = new TableSampleHandler(taskDto, "table", 2L, new HashMap<>(), BigDecimal.ONE);
            tableSampleHandler.init();
            tableSampleHandlers.put("test", tableSampleHandler);
            TaskSampleHandler taskSampleHandler = new TaskSampleHandler(taskDto);
            taskSampleHandler.init();

            ReflectionTestUtils.setField(task, "dataNodeSampleHandlers", sampleHandler);
            ReflectionTestUtils.setField(task, "tableSampleHandlers", tableSampleHandlers);
            ReflectionTestUtils.setField(task, "taskSampleHandler", taskSampleHandler);

            WriteRecordFuncAspect aspect = mock(WriteRecordFuncAspect.class);
            DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
            when(aspect.getDataProcessorContext()).thenReturn(dataProcessorContext);

            when(dataProcessorContext.getNode()).thenReturn(node);
            when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);

            TapTable tapTable = new TapTable();
            tapTable.setName("test");
            when(aspect.getTable()).thenReturn(tapTable);
            when(aspect.getTime()).thenReturn(System.currentTimeMillis());

            when(aspect.getState()).thenReturn(WriteRecordFuncAspect.STATE_START);

            List<TapRecordEvent> recordEvents = Stream.generate(() -> {
                TapRecordEvent event = new TapInsertRecordEvent();
                event.setReferenceTime(System.currentTimeMillis());
                event.setTime(System.currentTimeMillis()- 10000);
                return event;
            }).limit(RandomUtils.nextInt()% 10).collect(Collectors.toList());

            when(aspect.getRecordEvents()).thenReturn(recordEvents);

            WriteListResult<TapRecordEvent> result = new WriteListResult<>();
            when(aspect.consumer(any())).then(answer -> {
                BiConsumer<List<TapRecordEvent>, WriteListResult<TapRecordEvent>> resultConsumer = answer.getArgument(0);
                resultConsumer.accept(recordEvents, result);
                return aspect;
            });

            task.handleWriteRecordFunc(aspect);

            task.closeCompletableFuture();

            CounterSampler snapshotInsertRowCounter = (CounterSampler) ReflectionTestUtils.getField(tableSampleHandler, "snapshotInsertRowCounter");

            Assertions.assertNotNull(snapshotInsertRowCounter);
            Assertions.assertNotNull(snapshotInsertRowCounter.value());
            Assertions.assertEquals(recordEvents.size(), snapshotInsertRowCounter.value().intValue());
        }

        @Test
        void testStateStart_2() {
            TaskSampleRetriever.getInstance().start(mock(RestTemplateOperator.class));

            TaskDto taskDto = new TaskDto();
            taskDto.setId(new ObjectId());
            taskDto.setTaskRecordId(new ObjectId().toHexString());
            taskDto.setStartTime(new Date());
            taskDto.setSyncType("migrate");
            Node node = new DatabaseNode();
            node.setId("nodeId");
            List<SyncObjects> syncObjects = new ArrayList<>();
            SyncObjects syncObject = new SyncObjects();
            LinkedHashMap<String, String> tableNameRelation = new LinkedHashMap<>();
            tableNameRelation.put("test", "test");
            syncObject.setTableNameRelation(tableNameRelation);
            syncObjects.add(syncObject);
            ((DatabaseNode)node).setSyncObjects(syncObjects);

            ObservableAspectTask task = new ObservableAspectTask();
            task.initCompletableFuture();
            task.setTask(taskDto);

            Map<String, DataNodeSampleHandler> sampleHandler = new HashMap<>();

            sampleHandler.put(node.getId(), new DataNodeSampleHandler(taskDto, node));

            Map<String, TableSampleHandler> tableSampleHandlers = new HashMap<>();
            TableSampleHandler tableSampleHandler = new TableSampleHandler(taskDto, "table", 2L, new HashMap<>(), BigDecimal.ONE);
            tableSampleHandler.init();
            tableSampleHandlers.put("test_1", tableSampleHandler);
            TaskSampleHandler taskSampleHandler = new TaskSampleHandler(taskDto);
            taskSampleHandler.init();

            ReflectionTestUtils.setField(task, "dataNodeSampleHandlers", sampleHandler);
            ReflectionTestUtils.setField(task, "tableSampleHandlers", tableSampleHandlers);
            ReflectionTestUtils.setField(task, "taskSampleHandler", taskSampleHandler);

            WriteRecordFuncAspect aspect = mock(WriteRecordFuncAspect.class);
            DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
            when(aspect.getDataProcessorContext()).thenReturn(dataProcessorContext);

            when(dataProcessorContext.getNode()).thenReturn(node);
            when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);

            TapTable tapTable = new TapTable();
            tapTable.setName("test");
            when(aspect.getTable()).thenReturn(tapTable);
            when(aspect.getTime()).thenReturn(System.currentTimeMillis());

            when(aspect.getState()).thenReturn(WriteRecordFuncAspect.STATE_START);

            List<TapRecordEvent> recordEvents = Stream.generate(() -> {
                TapRecordEvent event = new TapInsertRecordEvent();
                event.setReferenceTime(System.currentTimeMillis());
                event.setTime(System.currentTimeMillis()- 10000);
                return event;
            }).limit(RandomUtils.nextInt()% 10).collect(Collectors.toList());

            when(aspect.getRecordEvents()).thenReturn(recordEvents);

            WriteListResult<TapRecordEvent> result = new WriteListResult<>();
            when(aspect.consumer(any())).then(answer -> {
                BiConsumer<List<TapRecordEvent>, WriteListResult<TapRecordEvent>> resultConsumer = answer.getArgument(0);
                resultConsumer.accept(recordEvents, result);
                return aspect;
            });

            task.handleWriteRecordFunc(aspect);

            task.closeCompletableFuture();

            CounterSampler snapshotInsertRowCounter = (CounterSampler) ReflectionTestUtils.getField(tableSampleHandler, "snapshotInsertRowCounter");

            Assertions.assertNotNull(snapshotInsertRowCounter);
            Assertions.assertNotNull(snapshotInsertRowCounter.value());
            Assertions.assertEquals(0, snapshotInsertRowCounter.value().intValue());
        }



    }

}

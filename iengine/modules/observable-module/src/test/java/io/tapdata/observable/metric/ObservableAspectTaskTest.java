package io.tapdata.observable.metric;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.mongo.RestTemplateOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.DropTableFuncAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.observable.metric.handler.DataNodeSampleHandler;
import io.tapdata.observable.metric.handler.ProcessorNodeSampleHandler;
import io.tapdata.observable.metric.handler.TableSampleHandler;
import io.tapdata.observable.metric.handler.TaskSampleHandler;
import io.tapdata.observable.metric.util.SyncGetMemorySizeHandler;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

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
}

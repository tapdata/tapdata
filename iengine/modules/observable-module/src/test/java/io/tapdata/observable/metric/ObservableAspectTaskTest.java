//package io.tapdata.observable.metric;
//
//import io.tapdata.aspect.DataNodeInitAspect;
//import io.tapdata.entity.aspect.Aspect;
//import io.tapdata.entity.aspect.AspectInterceptResult;
//import io.tapdata.observable.metric.util.SyncGetMemorySizeHandler;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Nested;
//import org.junit.jupiter.api.Test;
//
//import java.util.List;
//
//import static org.mockito.Mockito.mock;
//
//public class ObservableAspectTaskTest {
//    ObservableAspectTask observableAspectTask;
//    @BeforeEach
//    void init() {
//        observableAspectTask = new ObservableAspectTask();
//    }
//
//    @Nested
//    class InitCompletableFutureTest{
//        @Test
//        void testInitCompletableFuture() {
//            observableAspectTask.initCompletableFuture();
//            Assertions.assertNotNull(observableAspectTask.batchProcessFuture);
//            Assertions.assertNotNull(observableAspectTask.batchProcessFuture);
//            Assertions.assertNotNull(observableAspectTask.streamProcessFuture);
//            Assertions.assertNotNull(observableAspectTask.streamReadFuture);
//        }
//    }
//
//    @Nested
//    class CloseCompletableFutureTest {
//        @Test
//        void testCloseCompletableFuture() {
//            observableAspectTask.closeCompletableFuture();
//            Assertions.assertNull(observableAspectTask.batchProcessFuture);
//            Assertions.assertNull(observableAspectTask.batchProcessFuture);
//            Assertions.assertNull(observableAspectTask.streamProcessFuture);
//            Assertions.assertNull(observableAspectTask.streamReadFuture);
//        }
//
//        @Test
//        void testCloseCompletableFuture1() {
//            observableAspectTask.initCompletableFuture();
//            observableAspectTask.closeCompletableFuture();
//            Assertions.assertNotNull(observableAspectTask.batchProcessFuture);
//            Assertions.assertNotNull(observableAspectTask.batchProcessFuture);
//            Assertions.assertNotNull(observableAspectTask.streamProcessFuture);
//            Assertions.assertNotNull(observableAspectTask.streamReadFuture);
//
//            Assertions.assertTrue(observableAspectTask.batchProcessFuture.isDone());
//            Assertions.assertTrue(observableAspectTask.batchProcessFuture.isDone());
//            Assertions.assertTrue(observableAspectTask.streamProcessFuture.isDone());
//            Assertions.assertTrue(observableAspectTask.streamReadFuture.isDone());
//        }
//    }
//
//    @Nested
//    class PrepareSyncGetMemorySizeHandlerTest {
//        @Test
//        void testPrepareSyncGetMemorySizeHandler() {
//            SyncGetMemorySizeHandler handler = observableAspectTask.prepareSyncGetMemorySizeHandler();
//            Assertions.assertNotNull(handler);
//        }
//    }
//
//    @Nested
//    class HandleDataNodeInitTest {
//    }
//
//
//    @Nested
//    class HandleDataNodeCloseTest {
//
//    }
//
//    @Nested
//    class HandlePDKNodeInitTest {
//
//    }
//
//    @Nested
//    class HandleSourceJoinHeartbeatTest {
//
//    }
//
//    @Nested
//    class HandleTableCountTest {
//
//    }
//
//    @Nested
//    class HandleBatchReadFuncTest {
//
//    }
//
//    @Nested
//    class HandleStreamReadFuncTest {
//
//    }
//
//    @Nested
//    class HandleSourceStateTest {
//
//    }
//
//    @Nested
//    class HandleSourceDynamicTableTest {
//
//    }
//
//    @Nested
//    class HandleCreateTableFuncTest {
//
//    }
//
//    @Nested
//    class HandleDropTableFuncTest {
//
//    }
//
//    @Nested
//    class HandleNewFieldFunTest {
//
//    }
//
//    @Nested
//    class HandleAlterFieldNameFuncTest {
//
//    }
//
//    @Nested
//    class HandleAlterFieldAttributesFuncTest {
//
//    }
//
//    @Nested
//    class HandleDropFieldFuncTest {
//
//    }
//
//    @Nested
//    class HandleCDCHeartbeatWriteAspectTest {
//
//    }
//
//    @Nested
//    class HandleWriteRecordFuncTest {
//
//    }
//
//    @Nested
//    class HandleSnapshotWriteTableCompleteFuncTest {
//
//    }
//
//    @Nested
//    class HandleProcessorNodeInitTest {
//
//    }
//
//    @Nested
//    class HandleProcessorNodeCloseTest {
//
//    }
//
//    @Nested
//    class HandleProcessorNodeProcessTest {
//
//    }
//
//    @Nested
//    class ObserveAspectsTest {
//
//    }
//
//    @Nested
//    class InterceptAspectsTest {
//        @Test
//        void testInterceptAspects() {
//            List<Class<? extends Aspect>> list = observableAspectTask.interceptAspects();
//            Assertions.assertNull(list);
//        }
//    }
//
//    @Nested
//    class OnInterceptAspectTest {
//        @Test
//        void testInterceptAspectTest() {
//            AspectInterceptResult aspect = observableAspectTask.onInterceptAspect(null);
//            Assertions.assertNull(aspect);
//        }
//        @Test
//        void testInterceptAspectTest0() {
//            AspectInterceptResult aspect = observableAspectTask.onInterceptAspect(new Aspect() {});
//            Assertions.assertNull(aspect);
//        }
//    }
//}

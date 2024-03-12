package io.tapdata.inspect.compare;

import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.InspectDataSource;
import com.tapdata.entity.inspect.InspectResultStats;
import com.tapdata.entity.inspect.InspectStatus;
import com.tapdata.entity.inspect.InspectTask;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.exception.TapCodeException;
import io.tapdata.inspect.InspectTaskContext;
import io.tapdata.inspect.util.InspectJobUtil;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.common.QueryHashByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapHashResult;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HashVerifyInspectJobTest {
    Logger logger;
    HashVerifyInspectJob hashVerifyInspectJob;
    InspectTaskContext inspectTaskContext;

    @Test
    void testConstructor() {
        com.tapdata.entity.inspect.InspectTask inspectTask = mock(com.tapdata.entity.inspect.InspectTask.class);
        when(inspectTask.getTaskId()).thenReturn("id");
        InspectDataSource mock = mock(InspectDataSource.class);
        when(inspectTask.getSource()).thenReturn(mock);
        when(inspectTask.getTarget()).thenReturn(mock);

        Connections connection = mock(Connections.class);
        when(inspectTask.getTaskId()).thenReturn("taskID");
        when(connection.getName()).thenReturn("connection");
        doNothing().when(mock).setConnectionName(anyString());

        when(inspectTaskContext.getClientMongoOperator()).thenReturn(mock(ClientMongoOperator.class));
        when(inspectTaskContext.getTask()).thenReturn(inspectTask);
        when(inspectTaskContext.getName()).thenReturn("name");
        when(inspectTaskContext.getSource()).thenReturn(connection);
        when(inspectTaskContext.getTarget()).thenReturn(connection);
        when(inspectTaskContext.getProgressUpdateCallback()).thenReturn(null);
        when(inspectTaskContext.getInspectResultParentId()).thenReturn(null);
        when(inspectTaskContext.getSourceConnectorNode()).thenReturn(null);
        when(inspectTaskContext.getTargetConnectorNode()).thenReturn(null);
        Assertions.assertDoesNotThrow(() -> new HashVerifyInspectJob(inspectTaskContext));
    }
    @Test
    void assertParam() {
        Assertions.assertEquals("failed", HashVerifyInspectJob.FAILED_TAG);
        Assertions.assertEquals("passed", HashVerifyInspectJob.SUCCEED_TAG);
    }

    @BeforeEach
    void init() {
        logger = mock(Logger.class);
        hashVerifyInspectJob = mock(HashVerifyInspectJob.class);
        inspectTaskContext = mock(InspectTaskContext.class);

        ReflectionTestUtils.setField(hashVerifyInspectJob, "logger", logger);
    }

    @Nested
    class DoRunTest {
        CompletableFuture future;
        AtomicBoolean lock;

        boolean lockOver;
        @BeforeEach
        void init() {
            lockOver = true;
            lock = mock(AtomicBoolean.class);
            future = mock(CompletableFuture.class);
            when(hashVerifyInspectJob.doSourceHash()).thenReturn(mock());
            when(hashVerifyInspectJob.doTargetHash()).thenReturn(mock());
            doNothing().when(hashVerifyInspectJob).doHashVerify(any(TapHashResult.class), any(TapHashResult.class));
            doNothing().when(hashVerifyInspectJob).doWhenException(any(Exception.class));
            doNothing().when(hashVerifyInspectJob).sleep(anyLong());
            ReflectionTestUtils.setField(hashVerifyInspectJob, "lock", lock);
            doCallRealMethod().when(hashVerifyInspectJob).doRun();
            when(lock.get()).then(a -> {
                if (!lockOver) return false;
                lockOver = false;
                return true;
            });
        }
        void assertVerify(int acceptTimes, int bothTimes, int doSourceTimes, int doTargetTimes, int doHashVerify, int doWhenException) {
            when(future.thenAcceptBoth(any(CompletableFuture.class), any(BiConsumer.class))).then(a -> {
                BiConsumer argument = a.getArgument(1, BiConsumer.class);
                argument.accept(mock(TapHashResult.class), mock(TapHashResult.class));
                return future;
            });
            try(MockedStatic<CompletableFuture> cf = mockStatic(CompletableFuture.class)) {
                cf.when(() -> CompletableFuture.supplyAsync(any(Supplier.class))).then(a -> {
                    Supplier argument = a.getArgument(0, Supplier.class);
                    argument.get();
                    return future;
                });
                hashVerifyInspectJob.doRun();
                cf.verify(() -> CompletableFuture.supplyAsync(any(Supplier.class)), times(acceptTimes));
            }
            verify(future, times(bothTimes)).thenAcceptBoth(any(CompletableFuture.class), any(BiConsumer.class));
            verify(hashVerifyInspectJob, times(doSourceTimes)).doSourceHash();
            verify(hashVerifyInspectJob, times(doTargetTimes)).doTargetHash();
            verify(hashVerifyInspectJob, times(doHashVerify)).doHashVerify(any(TapHashResult.class), any(TapHashResult.class));
            verify(hashVerifyInspectJob, times(doWhenException)).doWhenException(any(Exception.class));
            verify(hashVerifyInspectJob, times(1)).sleep(anyLong());
            verify(lock, times(2)).get();
        }

        @Test
        void testDoSourceException() {
            when(hashVerifyInspectJob.doSourceHash()).thenAnswer(a -> {throw new Exception("Failed-source");});
            assertVerify(1, 0, 1, 0, 0, 1);
        }
        @Test
        void testDoTargetException() {
            when(hashVerifyInspectJob.doTargetHash()).thenAnswer(a -> {throw new Exception("Failed-target");});
            assertVerify(2, 0, 1, 1, 0, 1);
        }
        @Test
        void testDoVerifyException() {
            doAnswer(a -> {throw new Exception("Failed-verify");}).when(hashVerifyInspectJob).doHashVerify(any(TapHashResult.class), any(TapHashResult.class));
            assertVerify(2, 1, 1, 1, 1, 1);
        }
        @Test
        void testNormal() {
            assertVerify(2, 1, 1, 1, 1, 0);
        }
    }

    @Nested
    class DoSourceHashTest {
        @Test
        void testNormal() {
            com.tapdata.entity.inspect.InspectTask inspectTask = mock(com.tapdata.entity.inspect.InspectTask.class);
            when(inspectTask.getSource()).thenReturn(mock(InspectDataSource.class));
            ReflectionTestUtils.setField(hashVerifyInspectJob, "sourceNode", mock(ConnectorNode.class));
            ReflectionTestUtils.setField(hashVerifyInspectJob, "inspectTask", inspectTask);
            when(hashVerifyInspectJob.doGetHash(any(InspectDataSource.class), any(ConnectorNode.class), anyString())).thenReturn(mock(TapHashResult.class));
            when(hashVerifyInspectJob.doSourceHash()).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> hashVerifyInspectJob.doSourceHash());
        }
    }
    @Nested
    class DoTargetHashTest {
        @Test
        void testNormal() {
            com.tapdata.entity.inspect.InspectTask inspectTask = mock(com.tapdata.entity.inspect.InspectTask.class);
            when(inspectTask.getTarget()).thenReturn(mock(InspectDataSource.class));
            ReflectionTestUtils.setField(hashVerifyInspectJob, "targetNode", mock(ConnectorNode.class));
            ReflectionTestUtils.setField(hashVerifyInspectJob, "inspectTask", inspectTask);
            when(hashVerifyInspectJob.doGetHash(any(InspectDataSource.class), any(ConnectorNode.class), anyString())).thenReturn(mock(TapHashResult.class));
            when(hashVerifyInspectJob.doTargetHash()).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> hashVerifyInspectJob.doTargetHash());
        }
    }

    @Nested
    class DoGetHashTest {
        InspectDataSource dataSource;
        ConnectorNode node;

        InspectTask inspectTask;
        List<QueryOperator> conditions;
        TapTable table;
        InspectTaskContext inspectTaskContext;
        ConnectorFunctions functions;
        QueryHashByAdvanceFilterFunction hashFunction;
        TapConnectorContext context;
        TapNodeSpecification specification;
        TapAdvanceFilter filter;

        @BeforeEach
        void init() {
            dataSource = mock(InspectDataSource.class);
            node = mock(ConnectorNode.class);

            inspectTask = mock(InspectTask.class);
            conditions = mock(List.class);
            table = mock(TapTable.class);
            inspectTaskContext = mock(InspectTaskContext.class);
            functions = mock(ConnectorFunctions.class);
            hashFunction = mock(QueryHashByAdvanceFilterFunction.class);
            context = mock(TapConnectorContext.class);
            specification = mock(TapNodeSpecification.class);
            filter = mock(TapAdvanceFilter.class);
            ReflectionTestUtils.setField(hashVerifyInspectJob, "inspectTaskContext", inspectTaskContext);

            when(hashVerifyInspectJob.doGetHash(dataSource, node, "message")).thenCallRealMethod();
        }

        void assertVerify(QueryHashByAdvanceFilterFunction f) throws Throwable {
            when(dataSource.getConditions()).thenReturn(conditions);
            when(node.getConnectorFunctions()).thenReturn(functions);
            when(functions.getQueryHashByAdvanceFilterFunction()).thenReturn(f);
            when(node.getConnectorContext()).thenReturn(context);
            when(context.getSpecification()).thenReturn(specification);
            when(specification.getId()).thenReturn("id");
            doAnswer(a -> {
                Consumer<TapHashResult> argument = (Consumer<TapHashResult>) a.getArgument(3, Consumer.class);
                argument.accept(TapHashResult.create().withHash(100L));
                return null;
            }).when(hashFunction).query(any(TapConnectorContext.class), any(TapAdvanceFilter.class), any(TapTable.class), any(Consumer.class));
            try (MockedStatic<InspectJobUtil> iju = mockStatic(InspectJobUtil.class);
                 MockedStatic<PDKInvocationMonitor> pi = mockStatic(PDKInvocationMonitor.class)){
                iju.when(() -> InspectJobUtil.getTapTable(any(InspectDataSource.class), any(InspectTaskContext.class))).thenReturn(table);
                iju.when(() -> InspectJobUtil.wrapFilter(conditions)).thenReturn(filter);
                pi.when(() -> PDKInvocationMonitor.invoke(any(), any(PDKMethod.class), any(CommonUtils.AnyError.class), anyString())).thenAnswer(a -> {
                    CommonUtils.AnyError argument = a.getArgument(2, CommonUtils.AnyError.class);
                    argument.run();
                    return null;
                });
                TapHashResult tapHashResult = hashVerifyInspectJob.doGetHash(dataSource, node, "message");
                iju.verify(() -> InspectJobUtil.getTapTable(any(InspectDataSource.class), any(InspectTaskContext.class)), times(1));
                iju.verify(() -> InspectJobUtil.wrapFilter(conditions), times(null == f ? 0 : 1));
                pi.verify(() -> PDKInvocationMonitor.invoke(any(), any(PDKMethod.class), any(CommonUtils.AnyError.class), anyString()), times(null == f ? 0 : 1));
            }
            verify(dataSource, times(1)).getConditions();
            verify(node, times(1)).getConnectorFunctions();
            verify(functions, times(1)).getQueryHashByAdvanceFilterFunction();
            verify(node, times(1)).getConnectorContext();

            verify(context, times(null == f ? 1 : 0)).getSpecification();
            verify(specification, times(null == f ? 1 : 0)).getId();

            verify(hashFunction, times(null == f ? 0 : 1)).query(any(TapConnectorContext.class), any(TapAdvanceFilter.class), any(TapTable.class), any(Consumer.class));
        }

        @Test
        void testNotSupportHashFunction() {
            Assertions.assertThrows(TapCodeException.class, () -> assertVerify(null));
        }

        @Test
        void testSupportHashFunction() {
            Assertions.assertDoesNotThrow(() -> assertVerify(hashFunction));
        }
    }

    @Nested
    class DoHashVerifyTest {
        TapHashResult sourceHash;
        TapHashResult targetHash;
        InspectResultStats stats;
        AtomicBoolean lock;
        @BeforeEach
        void init() {
            sourceHash = mock(TapHashResult.class);
            targetHash = mock(TapHashResult.class);
            lock = mock(AtomicBoolean.class);
            stats = mock(InspectResultStats.class);
            ReflectionTestUtils.setField(hashVerifyInspectJob, "stats", stats);
            ReflectionTestUtils.setField(hashVerifyInspectJob, "lock", lock);
            when(logger.isDebugEnabled()).thenReturn(true);

            doNothing().when(logger).debug(anyString(), anyString(), anyString());
            doNothing().when(logger).debug(anyString(), anyLong(), anyLong());
            doNothing().when(stats).setEnd(any(Date.class));
            doNothing().when(stats).setStatus("done");
            doNothing().when(stats).setResult(anyString());
            doNothing().when(stats).setProgress(1);
            doNothing().when(lock).set(false);
        }

        void assertVerify(TapHashResult s, TapHashResult t, Long sHash, Long tHash,
                          boolean isDebug) {
            doCallRealMethod().when(hashVerifyInspectJob).doHashVerify(s, t);
            when(sourceHash.getHash()).thenReturn(sHash);
            when(targetHash.getHash()).thenReturn(tHash);
            when(logger.isDebugEnabled()).thenReturn(isDebug);
            hashVerifyInspectJob.doHashVerify(s, t);
            verify(logger, times(1)).isDebugEnabled();
            verify(stats, times(1)).setEnd(any(Date.class));
            verify(stats, times(1)).setStatus("done");
            verify(stats, times(1)).setResult(anyString());
            verify(stats, times(1)).setProgress(1);
            verify(lock, times(1)).set(false);
            verify(logger, times(isDebug?1:0)).debug(anyString(), anyString(), anyString());
        }

        @Test
        void testSourceIsNull() {
            assertVerify(null, targetHash, 100L, 100L, true);
        }
        @Test
        void testSourceHashIsNull() {
            assertVerify(sourceHash, targetHash, null, 100L, true);
        }
        @Test
        void testTargetIsNull() {
            assertVerify(sourceHash, null, 100L, 100L, true);
        }
        @Test
        void testTargetHashIsNull() {
            assertVerify(sourceHash, targetHash, 100L, null, true);
        }
        @Test
        void testLoggerIsDebugEnabled() {
            assertVerify(sourceHash, targetHash, 100L, 100L, true);
        }
        @Test
        void testLoggerIsNotDebugEnabled() {
            assertVerify(sourceHash, targetHash, 100L, 100L, false);
        }
    }

    @Nested
    class DoWhenExceptionTest {
        com.tapdata.entity.inspect.InspectTask inspectTask;
        Connections source;
        Connections target;
        InspectResultStats stats;

        InspectDataSource sourceI;
        InspectDataSource targetI;

        StackTraceElement[] elements;
        Exception e;
        @BeforeEach
        void init() {
            inspectTask = mock(com.tapdata.entity.inspect.InspectTask.class);
            source = mock(Connections.class);
            target = mock(Connections.class);
            stats = mock(InspectResultStats.class);

            sourceI = mock(InspectDataSource.class);
            targetI = mock(InspectDataSource.class);
            elements = new StackTraceElement[]{};
            e = mock(Exception.class);
            when(e.getStackTrace()).thenReturn(elements);
            when(e.getMessage()).thenReturn("message");

            when(inspectTask.getTaskId()).thenReturn("taskId");
            when(source.getName()).thenReturn("sourceName");
            when(target.getName()).thenReturn("targetName");
            when(sourceI.getTable()).thenReturn("sourceTable");
            when(targetI.getTable()).thenReturn("targetTable");
            doNothing().when(logger).warn(anyString(), anyString(), anyString(), anyString(), anyString(), anyString());
            doNothing().when(stats).setEnd(any(Date.class));
            doNothing().when(stats).setStatus(InspectStatus.ERROR.getCode());
            doNothing().when(stats).setResult(HashVerifyInspectJob.FAILED_TAG);
            doNothing().when(stats).setErrorMsg(anyString());

            ReflectionTestUtils.setField(hashVerifyInspectJob, "inspectTask", inspectTask);
            ReflectionTestUtils.setField(hashVerifyInspectJob, "stats", stats);

            doCallRealMethod().when(hashVerifyInspectJob).doWhenException(e);
        }

        void assertVerify(Connections sc, Connections tc,InspectDataSource si, InspectDataSource ti,
                          int sNameTimes, int tNameTimes, int sTableTime, int tTableTimes) {
            when(inspectTask.getSource()).thenReturn(si);
            when(inspectTask.getTarget()).thenReturn(ti);
            ReflectionTestUtils.setField(hashVerifyInspectJob, "source", sc);
            ReflectionTestUtils.setField(hashVerifyInspectJob, "target", tc);
            hashVerifyInspectJob.doWhenException(e);
            verify(inspectTask, times(1)).getSource();
            verify(inspectTask, times(1)).getTarget();
            verify(source, times(sNameTimes)).getName();
            verify(target, times(tNameTimes)).getName();
            verify(sourceI, times(sTableTime)).getTable();
            verify(targetI, times(tTableTimes)).getTable();
            verify(inspectTask, times(1)).getTaskId();
            verify(stats, times(1)).setEnd(any(Date.class));
            verify(stats, times(1)).setStatus(InspectStatus.ERROR.getCode());
            verify(stats, times(1)).setResult(HashVerifyInspectJob.FAILED_TAG);
            verify(stats, times(1)).setErrorMsg(anyString());
            verify(e, times(1)).getMessage();
            verify(e, times(1)).getStackTrace();
        }

        @Test
        void testSourceIsNull() {
            assertVerify(null, target, sourceI, targetI, 0, 1, 1, 1);
        }

        @Test
        void testTargetIsNull() {
            assertVerify(source, null, sourceI, targetI, 1, 0, 1, 1);
        }

        @Test
        void testSourceInspectDataIsNull() {
            assertVerify(source, target, null, targetI, 1, 1, 0, 1);
        }

        @Test
        void testTargetInspectDataIsNull() {
            assertVerify(source, target, sourceI, null, 1, 1, 1, 0);
        }
    }

    @Nested
    class SleepTest {
        @Test
        void testNormal() {
            doCallRealMethod().when(hashVerifyInspectJob).sleep(anyLong());
            Assertions.assertDoesNotThrow(() -> hashVerifyInspectJob.sleep(0));
        }
        @Test
        void testException() {
            doCallRealMethod().when(hashVerifyInspectJob).sleep(anyLong());
            Assertions.assertDoesNotThrow(() -> hashVerifyInspectJob.sleep(-1));
        }
    }
}

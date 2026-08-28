package io.tapdata.task.skiperrorevent;

import com.tapdata.tm.commons.function.ThrowableFunction;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.processor.error.ScriptProcessorExCode_30;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import io.tapdata.PDKExCode_10;
import io.tapdata.aspect.SkipErrorDataAspect;
import io.tapdata.aspect.SkipErrorProcessAspect;
import io.tapdata.aspect.WriteRecordFuncAspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.dql.model.DqlEventReport;
import io.tapdata.dql.model.DqlEventReportResult;
import io.tapdata.dql.model.DqlRecordSuccessReport;
import io.tapdata.dql.model.DqlErrorType;
import io.tapdata.dql.model.DqlRouteDecision;
import io.tapdata.dql.model.DqlStormGuardReport;
import io.tapdata.dql.model.DqlClassificationConfidence;
import io.tapdata.dql.classifier.DqlFailedStage;
import io.tapdata.dql.reporter.DqlEventReportException;
import io.tapdata.dql.reporter.DqlEventReporter;
import io.tapdata.exception.TapCodeException;
import io.tapdata.exception.TapPdkViolateUniqueEx;
import io.tapdata.exception.TapPdkWriteLengthEx;
import io.tapdata.exception.TapPdkWriteTypeEx;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.script.ScriptException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class SkipErrorEventAspectTaskTest {
    private SkipErrorEventAspectTask skipErrorEventAspectTask;
    private ScheduledExecutorService EXECUTOR;
    private SplitFileLogger logger;

    @BeforeEach
    void setUp() {
        skipErrorEventAspectTask = new SkipErrorEventAspectTask();
        EXECUTOR = spy(Executors.newScheduledThreadPool(1));
        this.logger = mock(SplitFileLogger.class);
        ReflectionTestUtils.setField(skipErrorEventAspectTask, "EXECUTOR", EXECUTOR);
        ReflectionTestUtils.setField(skipErrorEventAspectTask, "logger", logger);
    }

    @Nested
    class ExecutorShutdownTest {
        @Test
        void shutdownExecutor_WhenExecutorIsNull_ShouldDoNothing() {
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "EXECUTOR", null);

            assertDoesNotThrow(() -> skipErrorEventAspectTask.shutdownExecutor());
        }

        @Test
        void shutdownExecutor_WhenExecutorAlreadyShutdown_ShouldDoNothing() {

            when(EXECUTOR.isShutdown()).thenReturn(true);

            skipErrorEventAspectTask.shutdownExecutor();
            verify(EXECUTOR, never()).shutdown();
            verify(EXECUTOR, never()).shutdownNow();
        }

        @Test
        void shutdownExecutor_WhenNormalShutdown_ShouldSucceed() throws InterruptedException {
            when(EXECUTOR.isShutdown()).thenReturn(false);
            when(EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

            skipErrorEventAspectTask.shutdownExecutor();

            verify(EXECUTOR).shutdown();
            verify(EXECUTOR, never()).shutdownNow();
        }

        @Test
        void shutdownExecutor_WhenFirstAwaitFails_ShouldCallShutdownNow() throws InterruptedException {
            when(EXECUTOR.isShutdown()).thenReturn(false);
            when(EXECUTOR.awaitTermination(5, TimeUnit.SECONDS))
                    .thenReturn(false)
                    .thenReturn(true);

            skipErrorEventAspectTask.shutdownExecutor();

            verify(EXECUTOR).shutdown();
            verify(EXECUTOR).shutdownNow();
        }

        @Test
        void shutdownExecutor_WhenBothAwaitsFail_ShouldLogError() throws InterruptedException {
            when(EXECUTOR.isShutdown()).thenReturn(false);
            when(EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(false);

            skipErrorEventAspectTask.shutdownExecutor();
            verify(logger).error(eq("shutdown executor failed"));
        }

        @Test
        void shutdownExecutor_WhenInterrupted_ShouldPreserveInterruptStatus() throws InterruptedException {
            when(EXECUTOR.isShutdown()).thenReturn(false);
            when(EXECUTOR.awaitTermination(5, TimeUnit.SECONDS))
                    .thenThrow(new InterruptedException());

            skipErrorEventAspectTask.shutdownExecutor();

            verify(EXECUTOR).shutdownNow();
            assertTrue(Thread.currentThread().isInterrupted());

            Thread.interrupted();
        }
    }

    @Nested
    class LogSkipEventTest {
        String taskId;
        long nextPrintTimes;
        Log log;
        Map<String, Map<String, AtomicLong>> syncAndSkipMap;
        TapRecordEvent tapRecordEvent;
        @BeforeEach
        void setUp() {
            taskId = "test";
            nextPrintTimes = System.currentTimeMillis() - 1000;
            log = mock(Log.class);
            syncAndSkipMap = new ConcurrentHashMap<>();
            syncAndSkipMap.put(taskId, new ConcurrentHashMap<>());
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "taskId", taskId);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "nextPrintTimes", nextPrintTimes);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "log", log);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "syncAndSkipMap", syncAndSkipMap);
            tapRecordEvent = mock(TapRecordEvent.class);
            when(tapRecordEvent.toString()).thenReturn("test");
        }
        @Test
        void logSkipEvent_TapPdkViolateUniqueEx() {
            Throwable ex = new TapPdkViolateUniqueEx("mysql", null, "\"ID\":7,\"NAME\":\"test1\"", null, null);
            skipErrorEventAspectTask.logSkipEvent(tapRecordEvent, ex);
            verify(log, new Times(2)).warn(anyString(), any(Object[].class));
        }
        @Test
        void logSkipEvent_TapPdkWriteTypeEx() {
            Throwable ex = new TapPdkWriteTypeEx("mysql", null, null, "\"ID\":test,\"NAME\":\"test1\"", null);
            skipErrorEventAspectTask.logSkipEvent(tapRecordEvent, ex);
            verify(log, new Times(2)).warn(anyString(), any(Object[].class));
        }
        @Test
        void logSkipEvent_TapPdkWriteLengthEx() {
            Throwable ex = new TapPdkWriteLengthEx("mysql", null, null, "\"ID\":7,\"NAME\":\"test1\"", null);
            skipErrorEventAspectTask.logSkipEvent(tapRecordEvent, ex);
            verify(log, new Times(2)).warn(anyString(), any(Object[].class));
        }

        @Test
        void logSkipEventShouldTellCustomerWhyTheRecordWasIsolatedAndThatTaskContinues() {
            TaskDto task = new TaskDto();
            task.setName("orders-sync");
            skipErrorEventAspectTask.setTask(task);
            tapRecordEvent = TapInsertRecordEvent.create()
                    .table("orders")
                    .after(Map.of("id", 7));
            Throwable ex = new IllegalStateException("duplicate key: orders.id=7");

            skipErrorEventAspectTask.logSkipEvent(tapRecordEvent, ex);

            ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Object[]> argumentsCaptor = ArgumentCaptor.forClass(Object[].class);
            verify(log, atLeastOnce()).warn(messageCaptor.capture(), argumentsCaptor.capture());
            int messageIndex = messageCaptor.getAllValues().indexOf(
                    "DQL record isolated successfully; task continues running: taskId={}, taskName={}, table={}, operation={}, failedNodeId={}, failedNodeName={}, errorCode={}, reason={}, skipCounts={}");
            assertTrue(messageIndex >= 0);
            Object[] arguments = argumentsCaptor.getAllValues().get(messageIndex);
            assertEquals("test", arguments[0]);
            assertEquals("orders-sync", arguments[1]);
            assertEquals("orders", arguments[2]);
            assertEquals("INSERT", arguments[3]);
            assertEquals("N/A", arguments[4]);
            assertEquals("N/A", arguments[5]);
            assertEquals("N/A", arguments[6]);
            assertEquals("duplicate key: orders.id=7", arguments[7]);
            assertEquals("{\"test\":{}}", arguments[8]);
        }
    }

    @Nested
    class TargetWriteCaptureTest {
        private final TapTable table = new TapTable("orders");
        private final PDKMethodInvoker pdkMethodInvoker = mock(PDKMethodInvoker.class);
        private final DqlEventReporter reporter = mock(DqlEventReporter.class);
        private final Log taskLog = mock(Log.class);

        @BeforeEach
        void setUpDqlCapture() {
            TaskDto task = new TaskDto();
            task.setId(new org.bson.types.ObjectId());
            task.setTaskRecordId("task-record-1");
            task.setName("orders-sync");
            task.setVersion(7L);
            task.setAgentId("agent-1");
            task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
            task.setStatus(TaskDto.STATUS_RUNNING);
            skipErrorEventAspectTask.setTask(task);

            TaskDto.SkipErrorEvent skipConfig = new TaskDto.SkipErrorEvent();
            skipConfig.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.SkipData);
            skipConfig.setLimitMode(TaskDto.SkipErrorEvent.LimitMode.SkipByLimit);
            skipConfig.setLimit(10L);
            skipConfig.setRate(100);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "skipErrorEvent", skipConfig);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "taskId", "task-1");
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "dqlEventReporter", reporter);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "nextPrintTimes", Long.MAX_VALUE);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "log", taskLog);
        }

        @Test
        void sharedBatchFailureShouldReturnToTaskRetryWithoutSplitting() {
            List<TapRecordEvent> events = List.of(insertEvent(1), insertEvent(2));
            TapCodeException sharedFailure = new TapCodeException(PDKExCode_10.RETRYABLE_ERROR);
            List<List<TapRecordEvent>> applied = new ArrayList<>();
            SkipErrorDataAspect aspect = aspect(events, records -> {
                applied.add(records);
                throw sharedFailure;
            });

            TapCodeException thrown = assertThrows(TapCodeException.class,
                    () -> skipErrorEventAspectTask.skipErrorDataNoeAspectImpl(aspect));

            assertSame(sharedFailure, thrown);
            assertEquals(List.of(events), applied);
            verifyNoInteractions(reporter);
        }

        @Test
        void targetRecordFailureShouldReportBeforeReturningSkip() {
            TapRecordEvent successfulEvent = insertEvent(1);
            TapRecordEvent failedEvent = insertEvent(2);
            List<TapRecordEvent> events = List.of(successfulEvent, failedEvent);
            TapCodeException batchFailure = new TapCodeException(PDKExCode_10.WRITE_TYPE);
            TapCodeException singleFailure = new TapCodeException(PDKExCode_10.WRITE_TYPE);
            List<List<TapRecordEvent>> applied = new ArrayList<>();
            SkipErrorDataAspect aspect = aspect(events, records -> {
                applied.add(records);
                if (records.size() > 1 || records.get(0) == failedEvent) {
                    throw records.size() > 1 ? batchFailure : singleFailure;
                }
                return null;
            });
            DqlEventReportResult acknowledgement = new DqlEventReportResult();
            acknowledgement.setEventId("dql-event-1");
            when(reporter.report(eq("task-1"), any(DqlEventReport.class))).thenReturn(acknowledgement);

            assertDoesNotThrow(() -> skipErrorEventAspectTask.skipErrorDataNoeAspectImpl(aspect));

            assertEquals(List.of(events, List.of(successfulEvent), List.of(failedEvent)), applied);
            verify(reporter).report(eq("task-1"), any(DqlEventReport.class));
            ArgumentCaptor<DqlEventReport> reportCaptor = ArgumentCaptor.forClass(DqlEventReport.class);
            verify(reporter).report(eq("task-1"), reportCaptor.capture());
            DqlEventReport report = reportCaptor.getValue();
            assertEquals(DqlRouteDecision.RECORD_DLQ, report.getRouteDecision());
            assertEquals(DqlFailedStage.TARGET_WRITE.name(), report.getFailedStage());
            assertEquals("orders", report.getTableId());
            assertNotNull(report.getEventIdentity());
            assertNotNull(report.getPayload().getPayloadData());
            assertEquals(1L, skipCount("orders"));
        }

        @Test
        void protectedSingleRecordFailureShouldNotReportOrSkip() {
            TapRecordEvent successfulEvent = insertEvent(1);
            TapRecordEvent failedEvent = insertEvent(2);
            List<TapRecordEvent> events = List.of(successfulEvent, failedEvent);
            TapCodeException batchFailure = new TapCodeException(PDKExCode_10.WRITE_TYPE);
            TapCodeException sharedFailure = new TapCodeException(PDKExCode_10.RETRYABLE_ERROR);
            SkipErrorDataAspect aspect = aspect(events, records -> {
                if (records.size() > 1) {
                    throw batchFailure;
                }
                if (records.get(0) == failedEvent) {
                    throw sharedFailure;
                }
                return null;
            });

            TapCodeException thrown = assertThrows(TapCodeException.class,
                    () -> skipErrorEventAspectTask.skipErrorDataNoeAspectImpl(aspect));

            assertSame(sharedFailure, thrown);
            verifyNoInteractions(reporter);
            assertEquals(0L, skipCount("orders"));
            assertLogContains(taskLog, "DQL task-level");
        }

        @Test
        void failedReportShouldRollbackCandidateSkipCountAndFailTask() {
            TapRecordEvent successfulEvent = insertEvent(1);
            TapRecordEvent failedEvent = insertEvent(2);
            List<TapRecordEvent> events = List.of(successfulEvent, failedEvent);
            TapCodeException batchFailure = new TapCodeException(PDKExCode_10.WRITE_TYPE);
            TapCodeException singleFailure = new TapCodeException(PDKExCode_10.WRITE_TYPE);
            SkipErrorDataAspect aspect = aspect(events, records -> {
                if (records.size() > 1 || records.get(0) == failedEvent) {
                    throw records.size() > 1 ? batchFailure : singleFailure;
                }
                return null;
            });
            DqlEventReportException reportFailure = new DqlEventReportException("task-1", "TM unavailable");
            when(reporter.report(eq("task-1"), any(DqlEventReport.class))).thenThrow(reportFailure);

            DqlEventReportException thrown = assertThrows(DqlEventReportException.class,
                    () -> skipErrorEventAspectTask.skipErrorDataNoeAspectImpl(aspect));

            assertSame(reportFailure, thrown);
            verify(reporter).report(eq("task-1"), any(DqlEventReport.class));
            assertEquals(0L, skipCount("orders"));
            assertLogContains(taskLog, "DQL task-level");
        }

        @Test
        void skipLimitShouldRollbackCandidateAndUseTaskLevelPath() {
            TapRecordEvent event = insertEvent(1);
            TaskDto.SkipErrorEvent configured = (TaskDto.SkipErrorEvent) ReflectionTestUtils
                    .getField(skipErrorEventAspectTask, "skipErrorEvent");
            configured.setLimit(0L);
            TapCodeException writeFailure = new TapCodeException(PDKExCode_10.WRITE_TYPE);
            SkipErrorDataAspect aspect = aspect(List.of(event), records -> {
                throw writeFailure;
            });

            TapCodeException thrown = assertThrows(TapCodeException.class,
                    () -> skipErrorEventAspectTask.skipErrorDataNoeAspectImpl(aspect));

            assertSame(writeFailure, thrown);
            verifyNoInteractions(reporter);
            assertEquals(0L, skipCount("orders"));
            assertLogContains(taskLog, "DQL task-level");
        }

        @Test
        void unknownSingleFailureShouldPassThroughStormGuardBeforeReporting() {
            TapRecordEvent event = insertEvent(1);
            IllegalStateException unknownFailure = new IllegalStateException("vendor row failure 1");
            SkipErrorDataAspect aspect = aspect(List.of(event), records -> {
                throw unknownFailure;
            });
            DqlEventReportResult acknowledgement = new DqlEventReportResult();
            acknowledgement.setEventId("dql-event-unknown-1");
            when(reporter.report(eq("task-1"), any(DqlEventReport.class))).thenReturn(acknowledgement);

            assertDoesNotThrow(() -> skipErrorEventAspectTask.skipErrorDataNoeAspectImpl(aspect));

            ArgumentCaptor<DqlEventReport> reportCaptor = ArgumentCaptor.forClass(DqlEventReport.class);
            verify(reporter).report(eq("task-1"), reportCaptor.capture());
            DqlEventReport report = reportCaptor.getValue();
            assertEquals(DqlRouteDecision.RECORD_DLQ, report.getRouteDecision());
            assertEquals(DqlErrorType.UNKNOWN_RECORD_ERROR, report.getErrorType());
            assertEquals(DqlClassificationConfidence.UNKNOWN_SINGLE, report.getClassificationConfidence());
            assertEquals(1L, skipCount("orders"));
        }

        @Test
        void stormGuardProtectedFailureShouldRollbackCandidateSkipCount() {
            TaskDto.SkipErrorEvent configured = (TaskDto.SkipErrorEvent) ReflectionTestUtils
                    .getField(skipErrorEventAspectTask, "skipErrorEvent");
            configured.setLimit(100L);
            for (int i = 1; i <= 20; i++) {
                TapRecordEvent event = insertEvent(i);
                IllegalStateException unknownFailure = new IllegalStateException("vendor row failure");
                DqlEventReportResult acknowledgement = new DqlEventReportResult();
                acknowledgement.setEventId("dql-event-unknown-" + i);
                when(reporter.report(eq("task-1"), any(DqlEventReport.class))).thenReturn(acknowledgement);

                assertDoesNotThrow(() -> skipErrorEventAspectTask.skipErrorDataNoeAspectImpl(
                        aspect(List.of(event), records -> {
                            throw unknownFailure;
                        })));
            }

            TapRecordEvent protectedEvent = insertEvent(21);
            IllegalStateException protectedFailure = new IllegalStateException("vendor row failure");
            IllegalStateException thrown = assertThrows(IllegalStateException.class,
                    () -> skipErrorEventAspectTask.skipErrorDataNoeAspectImpl(
                            aspect(List.of(protectedEvent), records -> {
                                throw protectedFailure;
                            })));

            assertSame(protectedFailure, thrown);
            verify(reporter, times(20)).report(eq("task-1"), any(DqlEventReport.class));
            ArgumentCaptor<DqlStormGuardReport> guardReportCaptor =
                    ArgumentCaptor.forClass(DqlStormGuardReport.class);
            verify(reporter).reportStormGuard(eq("task-1"), guardReportCaptor.capture());
            DqlStormGuardReport guardReport = guardReportCaptor.getValue();
            assertTrue(guardReport.getGuardKey().startsWith("sha256:"));
            assertEquals(DqlRouteDecision.TASK_RETRY, guardReport.getRouteDecision());
            assertEquals(21L, guardReport.getWindowCount());
            assertEquals(20L, guardReport.getGuardThreshold());
            assertEquals(20L, skipCount("orders"));
            assertLogContains(taskLog, "DQL task-level");
        }

        private SkipErrorDataAspect aspect(List<TapRecordEvent> events,
                                           ThrowableFunction<Void, List<TapRecordEvent>, Throwable> writeFunction) {
            SkipErrorDataAspect aspect = mock(SkipErrorDataAspect.class);
            when(aspect.getTapTable()).thenReturn(table);
            when(aspect.getTapRecordEvents()).thenReturn(events);
            when(aspect.getPdkMethodInvoker()).thenReturn(pdkMethodInvoker);
            when(aspect.getWriteRecordFunction()).thenReturn(writeFunction);
            return aspect;
        }

        private TapRecordEvent insertEvent(int id) {
            return TapInsertRecordEvent.create()
                    .table("orders")
                    .after(Map.of("id", id, "name", "order-" + id));
        }

        @SuppressWarnings("unchecked")
        private long skipCount(String tableId) {
            Map<String, Map<String, AtomicLong>> metrics =
                    (Map<String, Map<String, AtomicLong>>) ReflectionTestUtils
                            .getField(skipErrorEventAspectTask, "syncAndSkipMap");
            Map<String, AtomicLong> tableMetrics = metrics.get(tableId);
            return tableMetrics == null || tableMetrics.get("skip") == null
                    ? 0L : tableMetrics.get("skip").get();
        }

        private void assertLogContains(Log log, String expectedText) {
            ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
            verify(log, atLeastOnce()).warn(messageCaptor.capture(), any(Object[].class));
            assertTrue(messageCaptor.getAllValues().stream()
                    .anyMatch(message -> message.contains(expectedText)));
        }
    }

    @Nested
    class ProcessCaptureTest {
        private final DqlEventReporter reporter = mock(DqlEventReporter.class);
        private final ProcessorBaseContext processorBaseContext = mock(ProcessorBaseContext.class);
        private final Log taskLog = mock(Log.class);

        @BeforeEach
        void setUpProcessCapture() {
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "dqlEventReporter", reporter);
            when(processorBaseContext.getTaskDto()).thenReturn(skipErrorEventAspectTask.getTask());
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "taskId", "task-1");
            TaskDto.SkipErrorEvent skipConfig = new TaskDto.SkipErrorEvent();
            skipConfig.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.SkipData);
            skipConfig.setLimitMode(TaskDto.SkipErrorEvent.LimitMode.Disable);
            skipConfig.setLimit(10L);
            skipConfig.setRate(100);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "skipErrorEvent", skipConfig);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "log", taskLog);
        }

        @Test
        void processorRecordFailureShouldReportAndIntercept() {
            TapdataEvent inputEvent = insertTapdataEvent(1);
            TapCodeException processFailure = new TapCodeException(
                    ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESS_FAILED);
            DqlEventReportResult acknowledgement = new DqlEventReportResult();
            acknowledgement.setEventId("dql-process-event-1");
            when(reporter.report(eq("task-1"), any(DqlEventReport.class))).thenReturn(acknowledgement);

            AspectInterceptResult result = skipErrorEventAspectTask.skipErrorProcessAspectHandle(
                    processAspect(inputEvent, processFailure));

            assertNotNull(result);
            assertTrue(result.isIntercepted());
            ArgumentCaptor<DqlEventReport> reportCaptor = ArgumentCaptor.forClass(DqlEventReport.class);
            verify(reporter).report(eq("task-1"), reportCaptor.capture());
            DqlEventReport report = reportCaptor.getValue();
            assertEquals(DqlRouteDecision.RECORD_DLQ, report.getRouteDecision());
            assertEquals(DqlFailedStage.PROCESSOR.name(), report.getFailedStage());
            assertEquals("processor-1", report.getFailedNodeId());
            assertEquals("processor node", report.getFailedNodeName());
            assertEquals(1L, skipCount("orders"));
            assertLogContains(taskLog, "DQL record isolated");
        }

        @Test
        void nestedCustomScriptFailureShouldReportAndIntercept() {
            TapdataEvent inputEvent = insertTapdataEvent(1);
            TapCodeException processFailure = new TapCodeException(
                    io.tapdata.error.TaskProcessorExCode_11.UNKNOWN_ERROR,
                    new RuntimeException("Execute script error, record: {id=1}",
                            new ScriptException("undefined variable")));
            DqlEventReportResult acknowledgement = new DqlEventReportResult();
            acknowledgement.setEventId("dql-custom-process-event-1");
            when(reporter.report(eq("task-1"), any(DqlEventReport.class))).thenReturn(acknowledgement);

            AspectInterceptResult result = skipErrorEventAspectTask.skipErrorProcessAspectHandle(
                    processAspect(inputEvent, processFailure));

            assertNotNull(result);
            assertTrue(result.isIntercepted());
            ArgumentCaptor<DqlEventReport> reportCaptor = ArgumentCaptor.forClass(DqlEventReport.class);
            verify(reporter).report(eq("task-1"), reportCaptor.capture());
            DqlEventReport report = reportCaptor.getValue();
            assertEquals(DqlRouteDecision.RECORD_DLQ, report.getRouteDecision());
            assertEquals(DqlErrorType.TRANSFORM_ERROR, report.getErrorType());
            assertEquals(DqlFailedStage.PROCESSOR.name(), report.getFailedStage());
        }

        @Test
        void dqlModeShouldCaptureDeterministicProcessorFailureEvenWhenLegacySkipLimitIsZero() {
            TaskDto.SkipErrorEvent dqlConfig = new TaskDto.SkipErrorEvent();
            dqlConfig.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.DQL);
            dqlConfig.setLimitMode(TaskDto.SkipErrorEvent.LimitMode.SkipByLimit);
            dqlConfig.setLimit(0L);
            dqlConfig.setRate(0);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "skipErrorEvent", dqlConfig);

            DqlEventReportResult acknowledgement = new DqlEventReportResult();
            acknowledgement.setEventId("dql-process-limit-compatibility-1");
            when(reporter.report(eq("task-1"), any(DqlEventReport.class))).thenReturn(acknowledgement);

            AspectInterceptResult result = skipErrorEventAspectTask.skipErrorProcessAspectHandle(
                    processAspect(insertTapdataEvent(1), new TapCodeException(
                            ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESS_FAILED)));

            assertNotNull(result);
            assertTrue(result.isIntercepted());
            verify(reporter).report(eq("task-1"), any(DqlEventReport.class));
        }

        @Test
        void processorInitializationFailureShouldUseExistingErrorHandlePath() {
            AspectInterceptResult result = skipErrorEventAspectTask.skipErrorProcessAspectHandle(
                    processAspect(insertTapdataEvent(1), new TapCodeException(
                            ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESSOR_GET_SCRIPT_FAILED)));

            assertNull(result);
            verifyNoInteractions(reporter);
        }

        @Test
        void processorSharedFailureShouldUseExistingErrorHandlePath() {
            AspectInterceptResult result = skipErrorEventAspectTask.skipErrorProcessAspectHandle(
                    processAspect(insertTapdataEvent(1), new TapCodeException(PDKExCode_10.RETRYABLE_ERROR)));

            assertNull(result);
            verifyNoInteractions(reporter);
        }

        @Test
        void processorReportFailureShouldPropagateWithoutInterception() {
            DqlEventReportException reportFailure = new DqlEventReportException("task-1", "TM unavailable");
            when(reporter.report(eq("task-1"), any(DqlEventReport.class))).thenThrow(reportFailure);

            DqlEventReportException thrown = assertThrows(DqlEventReportException.class,
                    () -> skipErrorEventAspectTask.skipErrorProcessAspectHandle(
                            processAspect(insertTapdataEvent(1), new TapCodeException(
                                    ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESS_FAILED))));

            assertSame(reportFailure, thrown);
            assertEquals(0L, skipCount("orders"));
            assertLogContains(taskLog, "DQL task-level");
        }

        @Test
        void disabledSkipDataShouldKeepProcessorOnExistingErrorPath() {
            TaskDto.SkipErrorEvent disabled = new TaskDto.SkipErrorEvent();
            disabled.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.Disable);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "skipErrorEvent", disabled);

            AspectInterceptResult result = skipErrorEventAspectTask.skipErrorProcessAspectHandle(
                    processAspect(insertTapdataEvent(1), new TapCodeException(
                            ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESS_FAILED)));

            assertNull(result);
            verifyNoInteractions(reporter);
            assertEquals(0L, skipCount("orders"));
        }

        @Test
        void nonDmlProcessorFailureShouldUseExistingErrorHandlePath() {
            TapdataEvent heartbeat = new TapdataEvent();
            AspectInterceptResult result = skipErrorEventAspectTask.skipErrorProcessAspectHandle(
                    processAspect(heartbeat, new TapCodeException(
                            ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESS_FAILED)));

            assertNull(result);
            verifyNoInteractions(reporter);
        }

        private SkipErrorProcessAspect processAspect(TapdataEvent inputEvent, Throwable error) {
            return new SkipErrorProcessAspect()
                    .inputEvent(inputEvent)
                    .processorBaseContext(processorBaseContext)
                    .error(error)
                    .processStage(DqlFailedStage.PROCESSOR)
                    .nodeId("processor-1")
                    .nodeName("processor node");
        }

        private TapdataEvent insertTapdataEvent(int id) {
            TapdataEvent inputEvent = new TapdataEvent();
            inputEvent.setTapEvent(insertEvent(id));
            return inputEvent;
        }

        private TapRecordEvent insertEvent(int id) {
            return TapInsertRecordEvent.create()
                    .table("orders")
                    .after(Map.of("id", id, "name", "order-" + id));
        }

        @SuppressWarnings("unchecked")
        private long skipCount(String tableId) {
            Map<String, Map<String, AtomicLong>> metrics =
                    (Map<String, Map<String, AtomicLong>>) ReflectionTestUtils
                            .getField(skipErrorEventAspectTask, "syncAndSkipMap");
            Map<String, AtomicLong> tableMetrics = metrics.get(tableId);
            return tableMetrics == null || tableMetrics.get("skip") == null
                    ? 0L : tableMetrics.get("skip").get();
        }

        private void assertLogContains(Log log, String expectedText) {
            ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
            verify(log, atLeastOnce()).warn(messageCaptor.capture(), any(Object[].class));
            assertTrue(messageCaptor.getAllValues().stream()
                    .anyMatch(message -> message.contains(expectedText)));
        }
    }

    @Nested
    class LaterSuccessCaptureTest {
        private final TapTable table = new TapTable("orders");
        private final DqlEventReporter reporter = mock(DqlEventReporter.class);

        @BeforeEach
        void setUpLaterSuccessCapture() {
            TaskDto task = new TaskDto();
            task.setId(new org.bson.types.ObjectId());
            task.setTaskRecordId("task-record-1");
            task.setName("orders-sync");
            task.setVersion(7L);
            task.setAgentId("agent-1");
            task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
            task.setStatus(TaskDto.STATUS_RUNNING);
            skipErrorEventAspectTask.setTask(task);

            TaskDto.SkipErrorEvent skipConfig = new TaskDto.SkipErrorEvent();
            skipConfig.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.SkipData);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "skipErrorEvent", skipConfig);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "taskId", "task-1");
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "dqlEventReporter", reporter);
        }

        @Test
        void successfulWriteReportsOnlyRecordsWithoutWriteErrors() {
            TapRecordEvent successfulEvent = insertEvent(1, 1787580100000L);
            TapRecordEvent failedEvent = insertEvent(2, 1787580101000L);
            WriteRecordFuncAspect aspect = new WriteRecordFuncAspect()
                    .recordEvents(List.of(successfulEvent, failedEvent))
                    .table(table)
                    .start();

            skipErrorEventAspectTask.onObserveAspect(aspect);

            WriteListResult<TapRecordEvent> writeResult = new WriteListResult<TapRecordEvent>()
                    .insertedCount(1)
                    .addError(failedEvent, new IllegalArgumentException("bad record"));
            for (var consumer : aspect.getConsumers()) {
                consumer.accept(List.of(successfulEvent, failedEvent), writeResult);
            }

            verify(reporter).reportRecordSuccess(eq("task-1"), any(DqlRecordSuccessReport.class));
            ArgumentCaptor<DqlRecordSuccessReport> reportCaptor = ArgumentCaptor.forClass(DqlRecordSuccessReport.class);
            verify(reporter).reportRecordSuccess(eq("task-1"), reportCaptor.capture());
            DqlRecordSuccessReport report = reportCaptor.getValue();
            assertEquals("task-record-1", report.getTaskRecordId());
            assertEquals("orders", report.getSourceTable());
            assertEquals("orders", report.getTargetTable());
            assertEquals("orders", report.getTableId());
            assertEquals("I", report.getDmlType());
            assertEquals(1787580100000L, report.getEventTime());
            assertNotNull(report.getSuccessAt());
            assertNotNull(report.getPayloadHash());
            assertNotNull(report.getRecordIdentity());
        }

        @Test
        void successfulWriteUsesTheSameConfiguredBusinessKeyAsDqlCapture() {
            TapInsertRecordEvent event = TapInsertRecordEvent.create()
                    .table("orders")
                    .after(Map.of("external_id", "EXT-1", "name", "order-1"));
            TableNode targetNode = new TableNode();
            targetNode.setUpdateConditionFields(List.of("external_id"));
            WriteRecordFuncAspect aspect = new WriteRecordFuncAspect()
                    .recordEvents(List.of(event))
                    .table(table)
                    .dataProcessorContext(DataProcessorContext.newBuilder().withNode(targetNode).build())
                    .start();

            skipErrorEventAspectTask.onObserveAspect(aspect);

            WriteListResult<TapRecordEvent> writeResult = new WriteListResult<TapRecordEvent>()
                    .insertedCount(1);
            for (var consumer : aspect.getConsumers()) {
                consumer.accept(List.of(event), writeResult);
            }

            ArgumentCaptor<DqlRecordSuccessReport> reportCaptor =
                    ArgumentCaptor.forClass(DqlRecordSuccessReport.class);
            verify(reporter).reportRecordSuccess(eq("task-1"), reportCaptor.capture());
            DqlRecordSuccessReport report = reportCaptor.getValue();
            assertEquals(Map.of("external_id", "EXT-1"), report.getEventKey());
            assertEquals("UPDATE_CONDITION", report.getRecordIdentityType());
        }

        @Test
        void callbackFailureDoesNotPreventReportingTheNextSuccessfulRecord() {
            TapRecordEvent firstEvent = insertEvent(1, 1787580100000L);
            TapRecordEvent secondEvent = insertEvent(2, 1787580101000L);
            WriteRecordFuncAspect aspect = new WriteRecordFuncAspect()
                    .recordEvents(List.of(firstEvent, secondEvent))
                    .table(table)
                    .start();
            when(reporter.reportRecordSuccess(eq("task-1"), any(DqlRecordSuccessReport.class)))
                    .thenThrow(new RuntimeException("TM unavailable"))
                    .thenReturn(null);

            skipErrorEventAspectTask.onObserveAspect(aspect);

            WriteListResult<TapRecordEvent> writeResult = new WriteListResult<TapRecordEvent>()
                    .insertedCount(2);
            for (var consumer : aspect.getConsumers()) {
                consumer.accept(List.of(firstEvent, secondEvent), writeResult);
            }

            verify(reporter, times(2)).reportRecordSuccess(eq("task-1"), any(DqlRecordSuccessReport.class));
        }

        @Test
        void disabledSkipDataDoesNotAttachSuccessCallback() {
            TapRecordEvent event = insertEvent(1, 1787580100000L);
            WriteRecordFuncAspect aspect = new WriteRecordFuncAspect()
                    .recordEvents(List.of(event))
                    .table(table)
                    .start();
            TaskDto.SkipErrorEvent disabledConfig = new TaskDto.SkipErrorEvent();
            disabledConfig.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.Disable);
            ReflectionTestUtils.setField(skipErrorEventAspectTask, "skipErrorEvent", disabledConfig);

            skipErrorEventAspectTask.onObserveAspect(aspect);

            assertTrue(aspect.getConsumers().isEmpty());
            verifyNoInteractions(reporter);
        }

        private TapRecordEvent insertEvent(int id, long eventTime) {
            TapInsertRecordEvent event = TapInsertRecordEvent.create()
                    .table("orders")
                    .after(Map.of("id", id, "name", "order-" + id));
            event.setReferenceTime(eventTime);
            return event;
        }
    }
}

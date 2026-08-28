package io.tapdata.task.skiperrorevent;

import com.tapdata.tm.commons.function.ThrowableFunction;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.PDKExCode_10;
import io.tapdata.aspect.SkipErrorDataAspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.dql.model.DqlErrorType;
import io.tapdata.dql.model.DqlEventReport;
import io.tapdata.dql.model.DqlEventReportResult;
import io.tapdata.dql.model.DqlExceptionScope;
import io.tapdata.dql.model.DqlRouteDecision;
import io.tapdata.dql.reporter.DqlEventReporter;
import io.tapdata.exception.TapCodeException;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class C12EngineCaptureRegressionTest {
    private final PDKMethodInvoker pdkMethodInvoker = mock(PDKMethodInvoker.class);
    private final DqlEventReporter reporter = mock(DqlEventReporter.class);
    private final Log taskLog = mock(Log.class);
    private final SplitFileLogger splitLogger = mock(SplitFileLogger.class);
    private SkipErrorEventAspectTask skipErrorEventAspectTask;

    @BeforeEach
    void setUp() {
        skipErrorEventAspectTask = new SkipErrorEventAspectTask();
        TaskDto task = new TaskDto();
        task.setId(new org.bson.types.ObjectId());
        task.setTaskRecordId("task-record-1");
        task.setName("orders-sync");
        task.setVersion(7L);
        task.setAgentId("agent-1");
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setStatus(TaskDto.STATUS_RUNNING);
        DAG dag = mock(DAG.class);
        Node<?> sourceNode = mock(Node.class);
        when(sourceNode.getId()).thenReturn("source-node-1");
        when(sourceNode.getName()).thenReturn("Mongo source");
        Node targetNode = mock(Node.class);
        when(targetNode.getId()).thenReturn("target-node-1");
        when(targetNode.getName()).thenReturn("MySQL target");
        when(dag.getSourceNodes()).thenReturn(List.of(sourceNode));
        when(dag.getTargetNodes()).thenReturn(List.of(targetNode));
        task.setDag(dag);
        skipErrorEventAspectTask.setTask(task);

        TaskDto.SkipErrorEvent skipConfig = new TaskDto.SkipErrorEvent();
        skipConfig.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.SkipData);
        skipConfig.setLimitMode(TaskDto.SkipErrorEvent.LimitMode.Disable);
        skipConfig.setLimit(10L);
        skipConfig.setRate(100);
        ReflectionTestUtils.setField(skipErrorEventAspectTask, "skipErrorEvent", skipConfig);
        ReflectionTestUtils.setField(skipErrorEventAspectTask, "taskId", "task-1");
        ReflectionTestUtils.setField(skipErrorEventAspectTask, "dqlEventReporter", reporter);
        ReflectionTestUtils.setField(skipErrorEventAspectTask, "log", taskLog);
        ReflectionTestUtils.setField(skipErrorEventAspectTask, "logger", splitLogger);
        ReflectionTestUtils.setField(skipErrorEventAspectTask, "nextPrintTimes", Long.MAX_VALUE);
    }

    @Test
    void targetCaptureShouldPreserveCompleteDqlContract() {
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", 1001, "password", "secret-value", "status", "new"));
        event.setReferenceTime(1_787_580_100_000L);
        RuntimeException writeCause = new RuntimeException("dirty row cannot be written");
        TapCodeException failure = new TapCodeException(PDKExCode_10.WRITE_TYPE, writeCause);
        when(reporter.report(eq("task-1"), any(DqlEventReport.class)))
                .thenReturn(acknowledgement("dql-event-1"));

        assertNotNull(skipErrorEventAspectTask.skipErrorDataNoeAspectImpl(
                targetAspect(new TapTable("orders"), event, records -> {
                    throw failure;
                })));

        org.mockito.ArgumentCaptor<DqlEventReport> reportCaptor =
                org.mockito.ArgumentCaptor.forClass(DqlEventReport.class);
        verify(reporter).report(eq("task-1"), reportCaptor.capture());
        DqlEventReport report = reportCaptor.getValue();
        assertEquals("task-record-1", report.getTaskRecordId());
        assertEquals("orders", report.getSourceTable());
        assertEquals("orders", report.getTargetTable());
        assertEquals("orders", report.getTableId());
        assertEquals("I", report.getDmlType());
        assertEquals(1_787_580_100_000L, report.getEventTime());
        assertEquals(PDKExCode_10.WRITE_TYPE, report.getErrorCode());
        assertNotNull(report.getErrorDetails());
        assertTrue(report.getErrorDetails().contains("dirty row cannot be written"));
        assertTrue(report.getErrorDetails().contains("java.lang.RuntimeException"));
        assertEquals(DqlExceptionScope.RECORD, report.getExceptionScope());
        assertEquals(DqlRouteDecision.RECORD_DLQ, report.getRouteDecision());
        assertEquals(DqlErrorType.TARGET_WRITE_ERROR, report.getErrorType());
        assertNotNull(report.getEventIdentity());
        assertNotNull(report.getRecordIdentity());
        assertNotNull(report.getPayload());
        assertEquals("tap-record-event-json-v1", report.getPayload().getPayloadFormat());
        assertNotNull(report.getPayload().getPayloadData());
        assertNotNull(report.getPayload().getPayloadHash());
        assertNotNull(report.getPayload().getPayloadPreview());
        Map<?, ?> previewAfter = (Map<?, ?>) report.getPayload().getPayloadPreview().get("after");
        assertEquals("******", previewAfter.get("password"));
        assertFalse(report.getPayload().getPayloadPreview().toString().contains("secret-value"));
        assertEquals(1L, skipCount("orders"));
    }

    @Test
    void targetCaptureShouldRecordSourceTargetAndFailedNodeMetadata() {
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", 1001));
        TapCodeException failure = new TapCodeException(PDKExCode_10.WRITE_TYPE);
        when(reporter.report(eq("task-1"), any(DqlEventReport.class)))
                .thenReturn(acknowledgement("dql-event-1"));

        assertNotNull(skipErrorEventAspectTask.skipErrorDataNoeAspectImpl(
                targetAspect(new TapTable("orders"), event, records -> {
                    throw failure;
                })));

        org.mockito.ArgumentCaptor<DqlEventReport> reportCaptor =
                org.mockito.ArgumentCaptor.forClass(DqlEventReport.class);
        verify(reporter).report(eq("task-1"), reportCaptor.capture());
        DqlEventReport report = reportCaptor.getValue();
        assertEquals("source-node-1", report.getSourceNodeId());
        assertEquals("Mongo source", report.getSourceNodeName());
        assertEquals("target-node-1", report.getTargetNodeId());
        assertEquals("MySQL target", report.getTargetNodeName());
        assertEquals("target-node-1", report.getFailedNodeId());
        assertEquals("MySQL target", report.getFailedNodeName());
    }

    @Test
    void targetCaptureShouldUseConfiguredUpdateConditionAsBusinessKey() {
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("external_id", "EXT-1", "status", "new"));
        TapCodeException failure = new TapCodeException(PDKExCode_10.WRITE_TYPE);
        when(reporter.report(eq("task-1"), any(DqlEventReport.class)))
                .thenReturn(acknowledgement("dql-event-1"));

        TableNode targetNode = new TableNode();
        targetNode.setId("target-node-1");
        targetNode.setName("MySQL target");
        targetNode.setUpdateConditionFields(List.of("external_id"));

        assertNotNull(skipErrorEventAspectTask.skipErrorDataNoeAspectImpl(
                targetAspect(new TapTable("orders"), event, records -> {
                    throw failure;
                }, targetNode)));

        org.mockito.ArgumentCaptor<DqlEventReport> reportCaptor =
                org.mockito.ArgumentCaptor.forClass(DqlEventReport.class);
        verify(reporter).report(eq("task-1"), reportCaptor.capture());
        DqlEventReport report = reportCaptor.getValue();
        assertEquals(Map.of("external_id", "EXT-1"), report.getEventKey());
        assertEquals("UPDATE_CONDITION", report.getRecordIdentityType());
        assertEquals(List.of("external_id"), report.getRecordIdentityFields());
    }

    @Test
    void targetCaptureShouldUseDatabaseUpdateConditionMapAsBusinessKey() {
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("external_id", "EXT-1", "status", "new"));
        TapCodeException failure = new TapCodeException(PDKExCode_10.WRITE_TYPE);
        when(reporter.report(eq("task-1"), any(DqlEventReport.class)))
                .thenReturn(acknowledgement("dql-event-1"));

        DatabaseNode targetNode = new DatabaseNode();
        targetNode.setId("target-node-1");
        targetNode.setName("MySQL target");
        targetNode.setUpdateConditionFieldMap(Map.of("orders", List.of("external_id")));

        assertNotNull(skipErrorEventAspectTask.skipErrorDataNoeAspectImpl(
                targetAspect(new TapTable("orders"), event, records -> {
                    throw failure;
                }, targetNode)));

        org.mockito.ArgumentCaptor<DqlEventReport> reportCaptor =
                org.mockito.ArgumentCaptor.forClass(DqlEventReport.class);
        verify(reporter).report(eq("task-1"), reportCaptor.capture());
        DqlEventReport report = reportCaptor.getValue();
        assertEquals(Map.of("external_id", "EXT-1"), report.getEventKey());
        assertEquals("UPDATE_CONDITION", report.getRecordIdentityType());
    }

    @Test
    void batchNetworkFailureShouldStayOnExistingTaskRetryPath() {
        List<TapRecordEvent> events = List.of(insertEvent(1), insertEvent(2));
        List<List<TapRecordEvent>> applied = new ArrayList<>();
        SocketTimeoutException timeout = new SocketTimeoutException("target read timed out");

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> skipErrorEventAspectTask.skipErrorDataNoeAspectImpl(
                        targetAspect(new TapTable("orders"), events, records -> {
                            applied.add(records);
                            throw timeout;
                        })));

        assertSame(timeout, thrown.getCause());
        assertEquals(List.of(events), applied);
        verifyNoInteractions(reporter);
        assertEquals(0L, skipCount("orders"));
    }

    @Test
    void disabledSkipDataKeepsTargetOnExistingErrorPath() {
        TaskDto.SkipErrorEvent disabled = new TaskDto.SkipErrorEvent();
        disabled.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.Disable);
        ReflectionTestUtils.setField(skipErrorEventAspectTask, "skipErrorEvent", disabled);
        ReflectionTestUtils.setField(skipErrorEventAspectTask, "skipErrorDataNoeAspect",
                (Function<SkipErrorDataAspect, AspectInterceptResult>)
                        skipErrorEventAspectTask::skipErrorDataNoeAspectImpl);
        SkipErrorDataAspect aspect = mock(SkipErrorDataAspect.class);

        assertNull(skipErrorEventAspectTask.skipErrorDataNoeAspectHandle(aspect));
        verifyNoInteractions(pdkMethodInvoker, reporter);
    }

    private DqlEventReportResult acknowledgement(String eventId) {
        DqlEventReportResult acknowledgement = new DqlEventReportResult();
        acknowledgement.setEventId(eventId);
        return acknowledgement;
    }

    private SkipErrorDataAspect targetAspect(TapTable table,
                                             TapRecordEvent event,
                                             ThrowableFunction<Void, List<TapRecordEvent>, Throwable> writeFunction) {
        return targetAspect(table, List.of(event), writeFunction);
    }

    private SkipErrorDataAspect targetAspect(TapTable table,
                                             List<TapRecordEvent> events,
                                             ThrowableFunction<Void, List<TapRecordEvent>, Throwable> writeFunction) {
        Node targetNode = mock(Node.class);
        when(targetNode.getId()).thenReturn("target-node-1");
        when(targetNode.getName()).thenReturn("MySQL target");
        return targetAspect(table, events, writeFunction, targetNode);
    }

    private SkipErrorDataAspect targetAspect(TapTable table,
                                             TapRecordEvent event,
                                             ThrowableFunction<Void, List<TapRecordEvent>, Throwable> writeFunction,
                                             Node<?> targetNode) {
        return targetAspect(table, List.of(event), writeFunction, targetNode);
    }

    private SkipErrorDataAspect targetAspect(TapTable table,
                                             List<TapRecordEvent> events,
                                             ThrowableFunction<Void, List<TapRecordEvent>, Throwable> writeFunction,
                                             Node<?> targetNode) {
        SkipErrorDataAspect aspect = mock(SkipErrorDataAspect.class);
        when(aspect.getTapTable()).thenReturn(table);
        when(aspect.getTapRecordEvents()).thenReturn(events);
        when(aspect.getPdkMethodInvoker()).thenReturn(pdkMethodInvoker);
        when(aspect.getWriteRecordFunction()).thenReturn(writeFunction);
        DataProcessorContext context = mock(DataProcessorContext.class);
        when(context.getNode()).thenReturn((Node) targetNode);
        when(aspect.getDataProcessorContext()).thenReturn(context);
        return aspect;
    }

    private TapRecordEvent insertEvent(int id) {
        return TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", id, "status", "new"));
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
}

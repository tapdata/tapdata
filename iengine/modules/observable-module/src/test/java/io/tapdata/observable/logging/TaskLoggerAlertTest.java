package io.tapdata.observable.logging;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.entity.logger.alert.TapAlertType;
import io.tapdata.exception.TapCodeException;
import io.tapdata.observable.alert.TaskAlertDispatcher;
import io.tapdata.observable.alert.TaskAlertEvent;
import io.tapdata.observable.logging.appender.AppenderFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskLoggerAlertTest {

    @Test
    void alertShouldWriteErrorLogAndSubmitEvent() {
        TaskLogger taskLogger = mock(TaskLogger.class);
        AppenderFactory appenderFactory = mock(AppenderFactory.class);
        TaskAlertDispatcher dispatcher = mock(TaskAlertDispatcher.class);
        ReflectionTestUtils.setField(taskLogger, "logAppendFactory", appenderFactory);
        ReflectionTestUtils.setField(taskLogger, "taskAlertDispatcher", dispatcher);
        ReflectionTestUtils.setField(taskLogger, "suppressExternalAlert", false);
        doCallRealMethod().when(taskLogger).alert(any(), any(), any(), any());

        taskLogger.alert(() -> MonitoringLogsDto.builder()
                        .taskId("task-1")
                        .taskName("task")
                        .taskRecordId("record-1")
                        .nodeId("node-1")
                        .nodeName("source")
                        .timestamp(1L),
                new TapCodeException("42001"),
                "Table '{}' discarded",
                "CFPCN");

        ArgumentCaptor<MonitoringLogsDto> logCaptor = ArgumentCaptor.forClass(MonitoringLogsDto.class);
        verify(appenderFactory).appendLog(logCaptor.capture());
        MonitoringLogsDto logsDto = logCaptor.getValue();
        Assertions.assertEquals("ERROR", logsDto.getLevel());
        Assertions.assertTrue(logsDto.getLogTags().contains("alert=true"));
        Assertions.assertEquals("42001", logsDto.getErrorCode());

        ArgumentCaptor<TaskAlertEvent> eventCaptor = ArgumentCaptor.forClass(TaskAlertEvent.class);
        verify(dispatcher).submit(eventCaptor.capture());
        TaskAlertEvent event = eventCaptor.getValue();
        Assertions.assertEquals("task-1", event.getTaskId());
        Assertions.assertEquals("node-1", event.getNodeId());
        Assertions.assertEquals(TapAlertType.DATA_INTEGRITY, event.getType());
        Assertions.assertEquals("DATA_INTEGRITY", event.getCode());
        Assertions.assertEquals("node-1", event.getDedupKey());
    }

    @Test
    void alertShouldStillSubmitWhenLogAppendFails() {
        TaskLogger taskLogger = mock(TaskLogger.class);
        AppenderFactory appenderFactory = mock(AppenderFactory.class);
        TaskAlertDispatcher dispatcher = mock(TaskAlertDispatcher.class);
        ReflectionTestUtils.setField(taskLogger, "logAppendFactory", appenderFactory);
        ReflectionTestUtils.setField(taskLogger, "taskAlertDispatcher", dispatcher);
        ReflectionTestUtils.setField(taskLogger, "suppressExternalAlert", false);
        doThrow(new IllegalStateException("append failed")).when(appenderFactory).appendLog(any());
        doCallRealMethod().when(taskLogger).alert(any(), any(), any(), any());

        Assertions.assertThrows(IllegalStateException.class, () -> taskLogger.alert(
                () -> MonitoringLogsDto.builder().taskId("task-1").taskName("task").timestamp(1L),
                new IllegalStateException("cause"),
                "failed"));
        verify(dispatcher).submit(any());
    }

    @Test
    void testTaskShouldNotPublishExternalAlert() {
        TaskLogger taskLogger = mock(TaskLogger.class);
        AppenderFactory appenderFactory = mock(AppenderFactory.class);
        TaskAlertDispatcher dispatcher = mock(TaskAlertDispatcher.class);
        ReflectionTestUtils.setField(taskLogger, "logAppendFactory", appenderFactory);
        ReflectionTestUtils.setField(taskLogger, "taskAlertDispatcher", dispatcher);
        ReflectionTestUtils.setField(taskLogger, "suppressExternalAlert", true);
        doCallRealMethod().when(taskLogger).alert(any(), any(), any(), any());

        taskLogger.alert(() -> MonitoringLogsDto.builder().taskId("task-1").taskName("task").timestamp(1L),
                new IllegalStateException("cause"), "failed");
        verify(appenderFactory).appendLog(any());
        verify(dispatcher, never()).submit(any());
    }

    @Test
    void alertWithoutThrowableStillWritesErrorLog() {
        TaskLogger taskLogger = mock(TaskLogger.class);
        AppenderFactory appenderFactory = mock(AppenderFactory.class);
        TaskAlertDispatcher dispatcher = mock(TaskAlertDispatcher.class);
        ReflectionTestUtils.setField(taskLogger, "logAppendFactory", appenderFactory);
        ReflectionTestUtils.setField(taskLogger, "taskAlertDispatcher", dispatcher);
        ReflectionTestUtils.setField(taskLogger, "suppressExternalAlert", false);
        doCallRealMethod().when(taskLogger).alert(any(), any(), any(), any());

        Assertions.assertDoesNotThrow(() -> taskLogger.alert(
                () -> MonitoringLogsDto.builder().taskId("task-1").taskName("task").timestamp(1L),
                null,
                "CDC event discarded"));
        verify(appenderFactory).appendLog(any());
        verify(dispatcher).submit(any());
    }

    @Test
    void blankObsLoggerAlertIsNoOp() {
        BlankObsLogger blankObsLogger = new BlankObsLogger();
        AtomicBoolean failed = new AtomicBoolean(false);
        Assertions.assertDoesNotThrow(() -> blankObsLogger.alert("noop"));
        Assertions.assertFalse(failed.get());
    }
}

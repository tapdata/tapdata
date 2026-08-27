package io.tapdata.task.skiperrorevent;

import com.tapdata.entity.TapdataDqlRecoveryEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.SkipErrorDataAspect;
import io.tapdata.aspect.SkipErrorProcessAspect;
import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.dql.recovery.DqlRecoveryFailureRegistry;
import io.tapdata.dql.serializer.DqlPayloadSerializer;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.dql.reporter.DqlEventReporter;
import io.tapdata.PDKExCode_10;
import io.tapdata.exception.TapCodeException;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class SkipErrorEventRecoveryCaptureTest {
    private final PDKMethodInvoker pdkMethodInvoker = mock(PDKMethodInvoker.class);
    private final DqlEventReporter reporter = mock(DqlEventReporter.class);
    private SkipErrorEventAspectTask task;

    @BeforeEach
    void setUp() {
        task = new SkipErrorEventAspectTask();
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new org.bson.types.ObjectId());
        taskDto.setTaskRecordId("task-record-1");
        taskDto.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        taskDto.setStatus(TaskDto.STATUS_RUNNING);
        task.setTask(taskDto);

        TaskDto.SkipErrorEvent config = new TaskDto.SkipErrorEvent();
        config.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.SkipData);
        ReflectionTestUtils.setField(task, "skipErrorEvent", config);
        ReflectionTestUtils.setField(task, "taskId", "task-1");
        ReflectionTestUtils.setField(task, "dqlEventReporter", reporter);
    }

    @Test
    void recoveryTargetFailureKeepsOriginalErrorAndDoesNotReportNewDqlEvent() {
        TapRecordEvent recoveryRecord = recoveryRecord("event-1");
        TapCodeException failure = new TapCodeException(PDKExCode_10.WRITE_TYPE);
        AtomicReference<Throwable> notified = new AtomicReference<>();
        DqlRecoveryFailureRegistry.register("event-1", notified::set);

        try {
            SkipErrorDataAspect aspect = mock(SkipErrorDataAspect.class);
            when(aspect.getTapTable()).thenReturn(new TapTable("orders"));
            when(aspect.getTapRecordEvents()).thenReturn(java.util.List.of(recoveryRecord));
            when(aspect.getPdkMethodInvoker()).thenReturn(pdkMethodInvoker);
            when(aspect.getWriteRecordFunction()).thenAnswer(invocation -> {
                throw failure;
            });

            Throwable thrown = assertThrows(Throwable.class, () -> task.skipErrorDataNoeAspectImpl(aspect));

            assertSame(failure, thrown);
            assertSame(failure, notified.get());
            verifyNoInteractions(reporter);
        } finally {
            DqlRecoveryFailureRegistry.unregister("event-1");
        }
    }

    @Test
    void recoveryProcessorFailureNotifiesTheOriginalAttemptWithoutDqlCapture() {
        TapdataDqlRecoveryEvent recoveryEvent = TapdataDqlRecoveryEvent.createData(
                "batch-1", "event-2", "attempt-1", "operator-1", 7L,
                new DqlPayloadSerializer().serialize(TapInsertRecordEvent.create()
                        .table("orders")
                        .after(Map.of("id", 1))));
        TapCodeException failure = new TapCodeException(PDKExCode_10.WRITE_TYPE);
        AtomicReference<Throwable> notified = new AtomicReference<>();
        DqlRecoveryFailureRegistry.register("event-2", notified::set);

        try {
            SkipErrorProcessAspect aspect = new SkipErrorProcessAspect()
                    .inputEvent(recoveryEvent)
                    .error(failure);

            org.junit.jupiter.api.Assertions.assertNull(task.skipErrorProcessAspectHandle(aspect));
            assertSame(failure, notified.get());
            verifyNoInteractions(reporter);
        } finally {
            DqlRecoveryFailureRegistry.unregister("event-2");
        }
    }

    private TapRecordEvent recoveryRecord(String eventId) {
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", 1));
        DqlPayloadSnapshot snapshot = new DqlPayloadSerializer().serialize(event);
        return (TapRecordEvent) TapdataDqlRecoveryEvent.createData(
                "batch-1", eventId, "attempt-1", "operator-1", 7L, snapshot).getTapEvent();
    }
}

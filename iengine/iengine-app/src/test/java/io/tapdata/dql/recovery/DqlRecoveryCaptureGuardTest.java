package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataDqlRecoveryEvent;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.dql.serializer.DqlPayloadSerializer;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlRecoveryCaptureGuardTest {

    @Test
    void recoveryFailureNotifiesTheOriginalAttemptAndCanOnlyBeConsumedOnce() {
        TapdataDqlRecoveryEvent recoveryEvent = recoveryEvent("event-1");
        RuntimeException failure = new RuntimeException("duplicate key");
        AtomicReference<Throwable> notified = new AtomicReference<>();
        DqlRecoveryFailureRegistry.register("event-1", notified::set);

        try {
            assertTrue(DqlRecoveryCaptureGuard.isRecovery(recoveryEvent));
            assertTrue(DqlRecoveryCaptureGuard.notifyFailure(recoveryEvent, failure));
            assertFalse(DqlRecoveryCaptureGuard.notifyFailure(recoveryEvent, new RuntimeException("late")));
            assertSame(failure, notified.get());
        } finally {
            DqlRecoveryFailureRegistry.unregister("event-1");
        }
    }

    @Test
    void normalEventDoesNotEnterRecoveryFailurePath() {
        TapdataEvent normalEvent = new TapdataEvent();
        TapInsertRecordEvent record = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", 1));
        normalEvent.setTapEvent(record);

        assertFalse(DqlRecoveryCaptureGuard.isRecovery(normalEvent));
        assertFalse(DqlRecoveryCaptureGuard.isRecoveryRecord(record));
        assertFalse(DqlRecoveryCaptureGuard.notifyFailure(normalEvent, new RuntimeException("normal")));
    }

    @Test
    void wrappedRecoveryRecordIsRecognizedAtTargetWriteBoundary() {
        TapdataDqlRecoveryEvent recoveryEvent = recoveryEvent("event-2");
        TapRecordEvent record = (TapRecordEvent) recoveryEvent.getTapEvent();

        assertTrue(DqlRecoveryCaptureGuard.isRecoveryRecord(record));
        assertTrue(DqlRecoveryCaptureGuard.eventId(record).isPresent());
        assertTrue(DqlRecoveryCaptureGuard.eventId(record).get().equals("event-2"));
    }

    @Test
    void recoveryStateMapNamespaceUsesTemporaryTaskId() {
        TaskDto task = new TaskDto();
        ObjectId taskId = new ObjectId();
        task.setId(taskId);

        assertTrue(DqlRecoveryCaptureGuard.stateMapNamespace(task).endsWith(taskId.toHexString()));
        assertTrue(DqlRecoveryCaptureGuard.stateMapNamespace(task).startsWith("DQL_RECOVERY_"));
    }

    private TapdataDqlRecoveryEvent recoveryEvent(String eventId) {
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", 1));
        DqlPayloadSnapshot snapshot = new DqlPayloadSerializer().serialize(event);
        return TapdataDqlRecoveryEvent.createData(
                "batch-1", eventId, "attempt-1", "operator-1", 7L, snapshot);
    }
}

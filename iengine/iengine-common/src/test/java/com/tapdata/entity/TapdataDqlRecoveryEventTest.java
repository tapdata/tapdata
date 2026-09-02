package com.tapdata.entity;

import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.dql.serializer.DqlPayloadSerializer;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TapdataDqlRecoveryEventTest {
    private final DqlPayloadSerializer serializer = new DqlPayloadSerializer();

    @Test
    void dataEventRebuildsInsertAndKeepsExactlyOnceIdentity() {
        TapInsertRecordEvent original = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", 1, "status", "paid"));
        original.setTime(100L);
        original.setReferenceTime(90L);
        original.setExactlyOnceId("eo-insert-1");
        original.setInfo(Map.of("sourceOffset", "binlog:12"));
        DqlPayloadSnapshot snapshot = serializer.serialize(original);

        TapdataDqlRecoveryEvent recovery = TapdataDqlRecoveryEvent.createData(
                "batch-1", "event-1", "attempt-1", "operator-1", 8L, snapshot);

        TapInsertRecordEvent restored = assertInstanceOf(TapInsertRecordEvent.class, recovery.getTapEvent());
        assertEquals("event-1", recovery.getEventId());
        assertEquals("batch-1", recovery.getBatchId());
        assertEquals("attempt-1", recovery.getAttemptId());
        assertEquals("operator-1", recovery.getOperatorId());
        assertEquals(8L, recovery.getTaskVersion());
        assertEquals("eo-insert-1", restored.getExactlyOnceId());
        assertEquals("orders", restored.getTableId());
        assertEquals(100L, restored.getTime());
        assertEquals(90L, restored.getReferenceTime());
        assertEquals("binlog:12", restored.getInfo().get("sourceOffset"));
        assertEquals(Boolean.TRUE, restored.getInfo().get(TapdataDqlRecoveryEvent.INFO_KEY_DQL_RECOVERY));
        assertEquals("event-1", restored.getInfo().get(TapdataDqlRecoveryEvent.INFO_KEY_DQL_EVENT_ID));
        assertEquals("batch-1", restored.getInfo().get(TapdataDqlRecoveryEvent.INFO_KEY_DQL_BATCH_ID));
        assertEquals("attempt-1", restored.getInfo().get(TapdataDqlRecoveryEvent.INFO_KEY_DQL_ATTEMPT_ID));
        assertFalse(original.getInfo().containsKey(TapdataDqlRecoveryEvent.INFO_KEY_DQL_RECOVERY));
    }

    @Test
    void dataEventRebuildsUpdateAndDeleteWithoutChangingDmlType() {
        TapUpdateRecordEvent update = TapUpdateRecordEvent.create()
                .table("orders")
                .before(Map.of("id", 1, "status", "new"))
                .after(Map.of("id", 1, "status", "paid"))
                .removedFields(List.of("legacy"));
        update.setExactlyOnceId("eo-update-1");

        TapDeleteRecordEvent delete = TapDeleteRecordEvent.create()
                .table("orders")
                .before(Map.of("id", 1, "status", "paid"));
        delete.setExactlyOnceId("eo-delete-1");

        TapdataDqlRecoveryEvent updateRecovery = TapdataDqlRecoveryEvent.createData(
                "batch-1", "event-update", "attempt-update", null, 8L, serializer.serialize(update));
        TapdataDqlRecoveryEvent deleteRecovery = TapdataDqlRecoveryEvent.createData(
                "batch-1", "event-delete", "attempt-delete", null, 8L, serializer.serialize(delete));

        TapUpdateRecordEvent restoredUpdate = assertInstanceOf(TapUpdateRecordEvent.class, updateRecovery.getTapEvent());
        TapDeleteRecordEvent restoredDelete = assertInstanceOf(TapDeleteRecordEvent.class, deleteRecovery.getTapEvent());
        assertEquals(update.getType(), restoredUpdate.getType());
        assertEquals(update.getBefore(), restoredUpdate.getBefore());
        assertEquals(update.getAfter(), restoredUpdate.getAfter());
        assertEquals(update.getRemovedFields(), restoredUpdate.getRemovedFields());
        assertEquals("eo-update-1", restoredUpdate.getExactlyOnceId());
        assertEquals(delete.getType(), restoredDelete.getType());
        assertEquals(delete.getBefore(), restoredDelete.getBefore());
        assertEquals("eo-delete-1", restoredDelete.getExactlyOnceId());
    }

    @Test
    void rejectsIncompletePayloadAndSupportsBeginEndEvents() {
        DqlPayloadSnapshot incomplete = new DqlPayloadSnapshot();
        incomplete.setPayloadFormat(DqlPayloadSerializer.FORMAT);
        incomplete.setPayloadComplete(false);

        assertThrows(IllegalArgumentException.class, () -> TapdataDqlRecoveryEvent.createData(
                "batch-1", "event-1", "attempt-1", null, 8L, incomplete));

        TapdataDqlRecoveryEvent begin = TapdataDqlRecoveryEvent.createBegin("batch-1");
        TapdataDqlRecoveryEvent end = TapdataDqlRecoveryEvent.createEnd("batch-1");
        assertFalse(begin.isDataEvent());
        assertFalse(end.isDataEvent());
        assertEquals(TapdataDqlRecoveryEvent.TYPE_BEGIN, begin.getRecoveryType());
        assertEquals(TapdataDqlRecoveryEvent.TYPE_END, end.getRecoveryType());
        assertTrue(TapdataDqlRecoveryEvent.isRecoveryEvent(
                TapdataDqlRecoveryEvent.createData(
                        "batch-1", "event-1", "attempt-1", null, 8L,
                        serializer.serialize(TapInsertRecordEvent.create().table("orders").after(Map.of("id", 1))))
                        .getTapEvent()));
    }

    @Test
    void clonePreservesRecoveryMetadataAndDmlPayload() {
        TapInsertRecordEvent original = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", 1));
        original.setExactlyOnceId("eo-clone-1");
        TapdataDqlRecoveryEvent recovery = TapdataDqlRecoveryEvent.createData(
                "batch-1", "event-1", "attempt-1", "operator-1", 8L, serializer.serialize(original));

        TapdataDqlRecoveryEvent cloned = assertInstanceOf(TapdataDqlRecoveryEvent.class, recovery.clone());

        assertEquals(recovery.getEventId(), cloned.getEventId());
        assertEquals(recovery.getBatchId(), cloned.getBatchId());
        assertEquals(recovery.getAttemptId(), cloned.getAttemptId());
        assertEquals(recovery.getRecoveryType(), cloned.getRecoveryType());
        assertEquals(recovery.getOperatorId(), cloned.getOperatorId());
        assertEquals(recovery.getTaskVersion(), cloned.getTaskVersion());
        TapInsertRecordEvent clonedDml = assertInstanceOf(TapInsertRecordEvent.class, cloned.getTapEvent());
        assertEquals("eo-clone-1", clonedDml.getExactlyOnceId());
        assertTrue(TapdataDqlRecoveryEvent.isRecoveryEvent(clonedDml));
    }
}

package io.tapdata.dql.serializer;

import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlPayloadSerializerTest {

    @Test
    @DisplayName("insert payload round trip preserves replay fields")
    void insertRoundTripPreservesReplayFields() {
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", 1, "status", "new"));
        event.setTime(1_787_580_000_000L);
        event.setReferenceTime(1_787_580_000_100L);
        event.setExactlyOnceId("eo-insert-1");
        event.setInfo(Map.of("syncStage", "CDC", "sourceOffset", "mysql-bin.000001:12"));
        event.setConnector("mysql");
        event.setConnectorVersion("1.0");

        DqlPayloadSnapshot snapshot = new DqlPayloadSerializer().serialize(event);
        TapRecordEvent restored = new DqlPayloadSerializer().deserialize(snapshot);

        assertEquals(DqlPayloadSerializer.FORMAT, snapshot.getPayloadFormat());
        assertTrue(snapshot.getPayloadComplete());
        assertTrue(snapshot.getPayloadSize() > 0L);
        Map<?, ?> payloadData = assertInstanceOf(Map.class, snapshot.getPayloadData());
        assertEquals(TapInsertRecordEvent.class.getName(), payloadData.get("tapEventClass"));
        assertEquals(TapInsertRecordEvent.TYPE, payloadData.get("type"));
        TapInsertRecordEvent restoredInsert = assertInstanceOf(TapInsertRecordEvent.class, restored);
        assertEquals(event.getAfter(), restoredInsert.getAfter());
        assertBaseFields(event, restoredInsert);
        assertEquals("mysql", restoredInsert.getConnector());
        assertEquals("1.0", restoredInsert.getConnectorVersion());
    }

    @Test
    @DisplayName("update payload round trip preserves before after and update metadata")
    void updateRoundTripPreservesBeforeAfterAndMetadata() {
        TapUpdateRecordEvent event = TapUpdateRecordEvent.create()
                .table("orders")
                .before(Map.of("id", 1, "status", "new"))
                .after(Map.of("id", 1, "status", "paid"))
                .removedFields(List.of("legacy"));
        event.setIsReplaceEvent(true);
        event.setBeforeIllegalDateFieldName(List.of("created"));
        event.setAfterIllegalDateFieldName(List.of("updated"));
        event.setContainsIllegalDate(true);
        event.setTime(1_787_580_001_000L);
        event.setReferenceTime(1_787_580_001_100L);
        event.setExactlyOnceId("eo-update-1");
        event.setInfo(Map.of("syncStage", "CDC"));

        DqlPayloadSnapshot snapshot = new DqlPayloadSerializer().serialize(event);
        TapUpdateRecordEvent restored = assertInstanceOf(TapUpdateRecordEvent.class,
                new DqlPayloadSerializer().deserialize(snapshot));

        assertEquals(event.getBefore(), restored.getBefore());
        assertEquals(event.getAfter(), restored.getAfter());
        assertEquals(List.of("legacy"), restored.getRemovedFields());
        assertEquals(Boolean.TRUE, restored.getIsReplaceEvent());
        assertEquals(List.of("created"), restored.getBeforeIllegalDateFieldName());
        assertEquals(List.of("updated"), restored.getAfterIllegalDateFieldName());
        assertTrue(restored.getContainsIllegalDate());
        assertBaseFields(event, restored);
    }

    @Test
    @DisplayName("delete payload round trip preserves before image")
    void deleteRoundTripPreservesBeforeImage() {
        TapDeleteRecordEvent event = TapDeleteRecordEvent.create()
                .table("orders")
                .before(Map.of("id", 1, "status", "paid"));
        event.setTime(1_787_580_002_000L);
        event.setReferenceTime(1_787_580_002_100L);
        event.setExactlyOnceId("eo-delete-1");
        event.setInfo(Map.of("syncStage", "CDC"));

        DqlPayloadSnapshot snapshot = new DqlPayloadSerializer().serialize(event);
        TapDeleteRecordEvent restored = assertInstanceOf(TapDeleteRecordEvent.class,
                new DqlPayloadSerializer().deserialize(snapshot));

        assertEquals(event.getBefore(), restored.getBefore());
        assertBaseFields(event, restored);
    }

    @Test
    @DisplayName("payload round trip preserves local date time runtime type")
    void payloadRoundTripPreservesLocalDateTimeRuntimeType() {
        LocalDateTime eventTime = LocalDateTime.of(2026, 9, 1, 15, 38, 47, 588_000_000);
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("event_time", eventTime));

        DqlPayloadSnapshot snapshot = new DqlPayloadSerializer().serialize(event);
        Map<?, ?> serializedAfter = assertInstanceOf(Map.class,
                assertInstanceOf(Map.class, snapshot.getPayloadData()).get("after"));
        assertEquals("local_date_time", assertInstanceOf(Map.class, serializedAfter.get("event_time"))
                .get("__tapdata_dql_value_type"));
        TapInsertRecordEvent restored = assertInstanceOf(TapInsertRecordEvent.class,
                new DqlPayloadSerializer().deserialize(snapshot));

        assertEquals(eventTime, restored.getAfter().get("event_time"));
        assertInstanceOf(LocalDateTime.class, restored.getAfter().get("event_time"));
    }

    @Test
    @DisplayName("oversized payload is marked incomplete and is not retained")
    void oversizedPayloadIsMarkedIncompleteAndNotRetained() {
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("content", "x".repeat(512)));

        DqlPayloadSnapshot snapshot = new DqlPayloadSerializer(128L).serialize(event);

        assertEquals(DqlPayloadSerializer.FORMAT, snapshot.getPayloadFormat());
        assertFalse(snapshot.getPayloadComplete());
        assertTrue(snapshot.getPayloadSize() > 128L);
        assertNull(snapshot.getPayloadData());
        assertThrows(IllegalArgumentException.class,
                () -> new DqlPayloadSerializer().deserialize(snapshot));
    }

    @Test
    @DisplayName("malformed or unsupported snapshots are rejected")
    void malformedOrUnsupportedSnapshotsAreRejected() {
        DqlPayloadSerializer serializer = new DqlPayloadSerializer();
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", 1));
        DqlPayloadSnapshot wrongFormat = serializer.serialize(event);
        wrongFormat.setPayloadFormat("unknown-format");
        DqlPayloadSnapshot unsupportedClass = completeSnapshot(Map.of(
                "tapEventClass", "java.lang.Runtime",
                "type", TapInsertRecordEvent.TYPE,
                "after", Map.of("id", 1)
        ));
        DqlPayloadSnapshot mismatchedType = completeSnapshot(Map.of(
                "tapEventClass", TapInsertRecordEvent.class.getName(),
                "type", TapUpdateRecordEvent.TYPE,
                "after", Map.of("id", 1)
        ));
        DqlPayloadSnapshot forgedOversized = completeSnapshot(Map.of(
                "tapEventClass", TapInsertRecordEvent.class.getName(),
                "type", TapInsertRecordEvent.TYPE,
                "after", Map.of("content", "x".repeat(512))
        ));
        forgedOversized.setPayloadSize(513L);

        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(wrongFormat));
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(unsupportedClass));
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(mismatchedType));
        assertThrows(IllegalArgumentException.class,
                () -> new DqlPayloadSerializer(128L).deserialize(forgedOversized));
        assertThrows(IllegalArgumentException.class, () -> serializer.serialize(null));
        assertThrows(IllegalArgumentException.class, () -> new DqlPayloadSerializer(0L));
    }

    private static DqlPayloadSnapshot completeSnapshot(Map<String, Object> payloadData) {
        DqlPayloadSnapshot snapshot = new DqlPayloadSnapshot();
        snapshot.setPayloadFormat(DqlPayloadSerializer.FORMAT);
        snapshot.setPayloadComplete(true);
        snapshot.setPayloadData(payloadData);
        return snapshot;
    }

    private static void assertBaseFields(TapRecordEvent expected, TapRecordEvent actual) {
        assertNotNull(actual);
        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getTableId(), actual.getTableId());
        assertEquals(expected.getTime(), actual.getTime());
        assertEquals(expected.getReferenceTime(), actual.getReferenceTime());
        assertEquals(expected.getExactlyOnceId(), actual.getExactlyOnceId());
        assertEquals(expected.getInfo(), actual.getInfo());
    }
}

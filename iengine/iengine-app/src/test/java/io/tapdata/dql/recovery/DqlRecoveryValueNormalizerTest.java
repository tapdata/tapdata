package io.tapdata.dql.recovery;

import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.dql.serializer.DqlPayloadSerializer;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class DqlRecoveryValueNormalizerTest {

    @Test
    void restoresLegacyTimestampArrayToScalarTimestamp() {
        TapTable table = new TapTable("orders")
                .add(new TapField("event_time", "timestamp(3) without time zone"));
        Map<String, Object> after = new LinkedHashMap<>();
        after.put("event_time", List.of(2026, 9, 1, 15, 38, 47, 588_000_000));
        TapInsertRecordEvent event = TapInsertRecordEvent.create().table("orders").after(after);

        DqlRecoveryValueNormalizer.normalize(event, table);

        assertEquals(LocalDateTime.of(2026, 9, 1, 15, 38, 47, 588_000_000), after.get("event_time"));
        assertInstanceOf(LocalDateTime.class, after.get("event_time"));
    }

    @Test
    void leavesTimestampArrayColumnUntouched() {
        TapTable table = new TapTable("orders")
                .add(new TapField("event_times", "timestamp(3) without time zone array"));
        List<Integer> values = List.of(2026, 9, 1);
        Map<String, Object> after = new LinkedHashMap<>();
        after.put("event_times", values);
        TapInsertRecordEvent event = TapInsertRecordEvent.create().table("orders").after(after);

        DqlRecoveryValueNormalizer.normalize(event, table);

        assertEquals(values, after.get("event_times"));
    }

    @Test
    void restoresLegacyTimestampArrayAfterPayloadDeserialization() {
        DqlPayloadSnapshot snapshot = new DqlPayloadSnapshot();
        snapshot.setPayloadFormat(DqlPayloadSerializer.FORMAT);
        snapshot.setPayloadComplete(true);
        snapshot.setPayloadData(Map.of(
                "tapEventClass", TapInsertRecordEvent.class.getName(),
                "type", TapInsertRecordEvent.TYPE,
                "table", "orders",
                "after", Map.of("event_time", List.of(2026, 9, 1, 15, 38, 47, 588_000_000))));

        TapInsertRecordEvent event = (TapInsertRecordEvent) new DqlPayloadSerializer().deserialize(snapshot);
        DqlRecoveryValueNormalizer.normalize(event, new TapTable("orders")
                .add(new TapField("event_time", "timestamp(3) without time zone")));

        assertEquals(LocalDateTime.of(2026, 9, 1, 15, 38, 47, 588_000_000), event.getAfter().get("event_time"));
    }
}

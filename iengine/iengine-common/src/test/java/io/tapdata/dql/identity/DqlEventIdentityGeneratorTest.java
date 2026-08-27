package io.tapdata.dql.identity;

import io.tapdata.dql.model.DqlEventIdentity;
import io.tapdata.dql.model.DqlRecordIdentityType;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlEventIdentityGeneratorTest {
    private static final long EVENT_TIME = 1_787_580_000_000L;

    private final DqlEventIdentityGenerator generator = new DqlEventIdentityGenerator();

    @Test
    @DisplayName("primary key is preferred for event key and record identity")
    void usesPrimaryKeyIdentity() {
        TapTable table = new TapTable("orders")
                .add(new TapField("id", "INT").primaryKeyPos(1))
                .add(new TapField("status", "VARCHAR"));
        TapInsertRecordEvent event = insertEvent(Map.of("id", 1001, "status", "new"));

        DqlEventIdentity identity = generator.generate(event, table, "record-1", "target-node");

        assertEquals(Map.of("id", 1001), identity.getEventKey());
        assertFalse(identity.isEventKeyMissing());
        assertEquals(DqlRecordIdentityType.PRIMARY_KEY, identity.getRecordIdentityType());
        assertEquals(List.of("id"), identity.getRecordIdentityFields());
        assertTrue(identity.getRecordIdentity().startsWith("key:orders:sha256:"));
        assertTrue(identity.getPayloadHash().startsWith("sha256:"));
        assertTrue(identity.getEventIdentity().startsWith("key:record-1:orders:I:1787580000000:sha256:"));
    }

    @Test
    @DisplayName("first unique index is used when the table has no primary key")
    void usesUniqueIndexIdentity() {
        TapIndex uniqueIndex = new TapIndex()
                .name("uk_tenant_order")
                .unique(true)
                .indexField(new TapIndexField().name("tenant_id").fieldAsc(true))
                .indexField(new TapIndexField().name("order_no").fieldAsc(true));
        TapTable table = new TapTable("orders").add(uniqueIndex);
        TapUpdateRecordEvent event = updateEvent(
                Map.of("tenant_id", "tenant-1", "order_no", "A-100", "status", "new"),
                Map.of("tenant_id", "tenant-1", "order_no", "A-100", "status", "paid"));

        DqlEventIdentity identity = generator.generate(event, table, "record-1", "target-node");

        assertEquals(Map.of("tenant_id", "tenant-1", "order_no", "A-100"), identity.getEventKey());
        assertEquals(DqlRecordIdentityType.UNIQUE_INDEX, identity.getRecordIdentityType());
        assertEquals(List.of("tenant_id", "order_no"), identity.getRecordIdentityFields());
        assertTrue(identity.getRecordIdentity().startsWith("key:orders:sha256:"));
    }

    @Test
    @DisplayName("record image without a key uses a canonical full-field hash")
    void usesFullFieldHashWhenKeyIsUnavailable() {
        TapTable table = new TapTable("orders");
        TapInsertRecordEvent first = insertEvent(linkedMap("status", "new", "id", 1001));
        TapInsertRecordEvent second = insertEvent(linkedMap("id", 1001, "status", "new"));

        DqlEventIdentity firstIdentity = generator.generate(first, table, "record-1", "target-node");
        DqlEventIdentity secondIdentity = generator.generate(second, table, "record-1", "target-node");

        assertTrue(firstIdentity.isEventKeyMissing());
        assertEquals(DqlRecordIdentityType.FULL_FIELD_HASH, firstIdentity.getRecordIdentityType());
        assertEquals(List.of(), firstIdentity.getRecordIdentityFields());
        assertTrue(firstIdentity.getRecordIdentity().startsWith("hash:orders:sha256:"));
        assertEquals(firstIdentity.getPayloadHash(), secondIdentity.getPayloadHash());
        assertEquals(firstIdentity.getRecordIdentity(), secondIdentity.getRecordIdentity());
        assertTrue(firstIdentity.getEventIdentity().startsWith("payload:record-1:orders:I:1787580000000:sha256:"));
    }

    @Test
    @DisplayName("missing record image remains an unknown identity")
    void leavesIdentityUnknownWhenRecordImageIsMissing() {
        TapTable table = new TapTable("orders");
        TapInsertRecordEvent event = insertEvent(null);

        DqlEventIdentity identity = generator.generate(event, table, "record-1", "target-node");

        assertTrue(identity.isEventKeyMissing());
        assertEquals(DqlRecordIdentityType.UNKNOWN, identity.getRecordIdentityType());
        assertEquals(List.of(), identity.getRecordIdentityFields());
        assertNull(identity.getRecordIdentity());
        assertTrue(identity.getEventIdentity().startsWith("payload:record-1:orders:I:1787580000000:sha256:"));
    }

    @Test
    @DisplayName("exactly once id and source offset take precedence over key fallback")
    void followsEventIdentityPrecedence() {
        TapTable table = new TapTable("orders")
                .add(new TapField("id", "INT").primaryKeyPos(1));
        TapInsertRecordEvent event = insertEvent(Map.of("id", 1001));
        event.setExactlyOnceId("eo-1001");
        event.setInfo(Map.of("sourceOffset", Map.of("position", 42, "file", "mysql-bin.000001")));

        DqlEventIdentity exactlyOnce = generator.generate(event, table, "record-1", "target-node");
        assertEquals("eo:eo-1001", exactlyOnce.getEventIdentity());

        event.setExactlyOnceId(null);
        DqlEventIdentity sourceOffset = generator.generate(event, table, "record-1", "target-node");
        assertTrue(sourceOffset.getEventIdentity().startsWith("offset:sha256:"));
        assertNotEquals(exactlyOnce.getEventIdentity(), sourceOffset.getEventIdentity());
    }

    private static TapInsertRecordEvent insertEvent(Map<String, Object> after) {
        TapInsertRecordEvent event = TapInsertRecordEvent.create().table("orders").after(after);
        event.setReferenceTime(EVENT_TIME);
        return event;
    }

    private static TapUpdateRecordEvent updateEvent(Map<String, Object> before, Map<String, Object> after) {
        TapUpdateRecordEvent event = TapUpdateRecordEvent.create()
                .table("orders")
                .before(before)
                .after(after);
        event.setReferenceTime(EVENT_TIME);
        return event;
    }

    private static Map<String, Object> linkedMap(String firstKey, Object firstValue,
                                                  String secondKey, Object secondValue) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(firstKey, firstValue);
        result.put(secondKey, secondValue);
        return result;
    }
}

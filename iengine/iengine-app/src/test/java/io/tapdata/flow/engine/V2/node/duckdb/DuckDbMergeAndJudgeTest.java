package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbMergeAndJudgeTest {
    @Test
    void wideTableCdcEvent_basicBehavior() {
        WideTableCdcEvent e = new WideTableCdcEvent(WideTableCdcEvent.OpType.UPDATE, 1L, Map.of("id", 1L));
        assertEquals(WideTableCdcEvent.OpType.UPDATE, e.getOpType());
        assertEquals(1L, e.getPrimaryKey());
        assertEquals(Map.of("id", 1L), e.getData());
        assertTrue(e.toString().contains("UPDATE"));
    }

    @Test
    void fourStateJudge_generatesInsertUpdateDelete() {
        FourStateJudge judge = new FourStateJudge("wide", List.of("id"));

        List<Map<String, Object>> beforePks = List.of(
                Map.of("id", 1L),
                Map.of("id", 2L)
        );
        List<Map<String, Object>> afterData = List.of(
                Map.of("id", 2L, "v", "new"),
                Map.of("id", 3L, "v", "ins")
        );

        List<TapdataEvent> events = judge.judge(beforePks, afterData);
        assertEquals(3, events.size());

        boolean hasDelete = events.stream().anyMatch(e -> e.getTapEvent() instanceof TapDeleteRecordEvent);
        boolean hasUpdate = events.stream().anyMatch(e -> e.getTapEvent() instanceof TapUpdateRecordEvent);
        boolean hasInsert = events.stream().anyMatch(e -> e.getTapEvent() instanceof TapInsertRecordEvent);
        assertTrue(hasDelete);
        assertTrue(hasUpdate);
        assertTrue(hasInsert);
        assertTrue(events.stream().allMatch(e -> e.getSyncStage() == SyncStage.CDC));
    }

    @Test
    void smartMerger_mergeEventsSmart_mergesUpdateAndInsertAndDelete() {
        NodeSchemaInfo schema = buildSchema("node1", "users", List.of("id"));

        TapInsertRecordEvent ins = TapInsertRecordEvent.create()
                .table("users")
                .after(Map.of("id", 1L, "name", "A"));
        TapUpdateRecordEvent upd = TapUpdateRecordEvent.create()
                .table("users")
                .before(Map.of("id", 1L, "name", "A"))
                .after(Map.of("id", 1L, "name", "B"));
        TapDeleteRecordEvent del = TapDeleteRecordEvent.create()
                .table("users")
                .before(Map.of("id", 2L, "name", "X"));

        List<TapdataEvent> tapdataEvents = List.of(wrap(ins), wrap(upd), wrap(del));
        List<SmartMerger.MergedRecord> merged = SmartMerger.mergeEventsSmart(tapdataEvents, "users", schema);
        assertEquals(2, merged.size());

        SmartMerger.MergedRecord record1 = merged.stream()
                .filter(r -> r.getMainTableAfterPks().stream().anyMatch(pk -> "1".equals(String.valueOf(pk.get("id")))))
                .findFirst()
                .orElseThrow();
        assertEquals("UPDATE", record1.getFinalOp());
        assertFalse(record1.getBeforeRows().isEmpty());
        assertFalse(record1.getAfterRows().isEmpty());
        assertEquals("users", record1.getTableName());
        assertSame(schema, record1.getSchema());

        SmartMerger.MergedRecord record2 = merged.stream()
                .filter(r -> r.getAfterRows().isEmpty())
                .findFirst()
                .orElseThrow();
        assertEquals("DELETE", record2.getFinalOp());
    }

    @Test
    void smartMerger_mergeLastWins_returnsAfterStateOnly() {
        NodeSchemaInfo schema = buildSchema("node1", "users", List.of("id"));

        TapInsertRecordEvent v1 = TapInsertRecordEvent.create()
                .table("users")
                .after(Map.of("id", 1L, "name", "A"));
        TapUpdateRecordEvent v2 = TapUpdateRecordEvent.create()
                .table("users")
                .after(Map.of("id", 1L, "name", "B"));

        List<Map<String, Object>> rows = SmartMerger.mergeLastWins(List.of(wrap(v1), wrap(v2)), "users", schema);
        assertEquals(1, rows.size());
        assertEquals("B", rows.get(0).get("name"));
    }

    private static TapdataEvent wrap(io.tapdata.entity.event.TapEvent tapEvent) {
        TapdataEvent tapdataEvent = new TapdataEvent();
        tapdataEvent.setTapEvent(tapEvent);
        return tapdataEvent;
    }

    private static NodeSchemaInfo buildSchema(String nodeId, String tableName, List<String> pks) {
        TapTable tapTable = new TapTable();
        LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();
        TapField id = new TapField();
        id.setName("id");
        id.setOriginalFieldName("id");
        id.setDataType("BIGINT");
        id.setPrimaryKey(true);
        id.setTapType(new TapNumber());
        fieldMap.put("id", id);
        TapField name = new TapField();
        name.setName("name");
        name.setOriginalFieldName("name");
        name.setDataType("VARCHAR");
        fieldMap.put("name", name);
        tapTable.setNameFieldMap(fieldMap);
        return new NodeSchemaInfo(nodeId, tableName, tableName, pks, fieldMap, tapTable, null);
    }
}


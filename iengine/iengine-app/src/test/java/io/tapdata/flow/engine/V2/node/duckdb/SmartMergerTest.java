package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SmartMergerTest {

    // 测试用的表名和 schema
    private static final String TEST_TABLE_NAME = "users";
    private NodeSchemaInfo testSchema;

    /**
     * 创建测试用的 NodeSchemaInfo
     */
    private NodeSchemaInfo createTestSchema() {
        if (testSchema == null) {
            List<String> primaryKeys = Collections.singletonList("id");
            Map<String, TapField> fieldMap = new HashMap<>();
            TapField idField = new TapField("id", "INT");
            idField.setPrimaryKey(true);
            fieldMap.put("id", idField);
            fieldMap.put("name", new TapField("name", "VARCHAR"));
            testSchema = new NodeSchemaInfo("test-node", TEST_TABLE_NAME, "test.qualified.name",
                    primaryKeys, fieldMap, null, null);
        }
        return testSchema;
    }

    @Test
    void testEmptyInput() {
        List<SmartMerger.MergedRecord> merged = SmartMerger.mergeEventsSmart(Collections.<TapdataEvent>emptyList(), TEST_TABLE_NAME, createTestSchema());
        assertNotNull(merged);
        assertTrue(merged.isEmpty());
    }

    @Test
    void testMergeEventsSmart_pureInsert() {
        List<TapdataEvent> events = new ArrayList<>();
        events.add(createTapdataInsertEvent("users", "id", 1, "name", "Alice"));
        events.add(createTapdataInsertEvent("users", "id", 2, "name", "Bob"));

        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events, TEST_TABLE_NAME, createTestSchema());

        assertEquals(2, mergedRecords.size());
        assertEquals(1, mergedRecords.get(0).getInitialPk());
        assertEquals(2, mergedRecords.get(1).getInitialPk());
        assertEquals("INSERT", mergedRecords.get(0).getFinalOp());
        assertEquals("Alice", mergedRecords.get(0).getFinalState().get("name"));
    }

    @Test
    void testMergeEventsSmart_insertThenUpdate() {
        List<TapdataEvent> events = new ArrayList<>();
        events.add(createTapdataInsertEvent("users", "id", 1, "name", "Alice"));
        events.add(createTapdataUpdateEvent("users", "id", 1, "name", "Alice Updated"));

        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events, TEST_TABLE_NAME, createTestSchema());

        assertEquals(1, mergedRecords.size());
        assertEquals("UPDATE", mergedRecords.get(0).getFinalOp());
        assertEquals("Alice Updated", mergedRecords.get(0).getFinalState().get("name"));
    }

    // ==================== Helper Methods ====================

    /**
     * 创建 TapdataEvent (INSERT)
     */
    private TapdataEvent createTapdataInsertEvent(String tableName, Object... keyValues) {
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapInsertRecordEvent insertEvent = new TapInsertRecordEvent();
        insertEvent.setTableId(tableName);

        Map<String, Object> after = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            after.put((String) keyValues[i], keyValues[i + 1]);
        }
        insertEvent.setAfter(after);

        tapdataEvent.setTapEvent(insertEvent);
        return tapdataEvent;
    }

    /**
     * 创建 TapdataEvent (UPDATE)
     */
    private TapdataEvent createTapdataUpdateEvent(String tableName, Object... keyValues) {
        TapdataEvent tapdataEvent = new TapdataEvent();
        io.tapdata.entity.event.dml.TapUpdateRecordEvent updateEvent = new io.tapdata.entity.event.dml.TapUpdateRecordEvent();
        updateEvent.setTableId(tableName);

        Map<String, Object> before = new HashMap<>();
        Map<String, Object> after = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            String key = (String) keyValues[i];
            Object value = keyValues[i + 1];
            before.put(key, value);
            after.put(key, value);
        }
        updateEvent.setBefore(before);
        updateEvent.setAfter(after);

        tapdataEvent.setTapEvent(updateEvent);
        return tapdataEvent;
    }
}

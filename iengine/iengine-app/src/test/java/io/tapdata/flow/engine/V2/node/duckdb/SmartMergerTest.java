package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
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
        events.add(createTapdataUpdateEvent("users", "id", 1, "name", "Alice", "Alice Updated"));

        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events, TEST_TABLE_NAME, createTestSchema());

        assertEquals(1, mergedRecords.size());
        assertEquals("UPDATE", mergedRecords.get(0).getFinalOp());
        assertEquals("Alice Updated", mergedRecords.get(0).getFinalState().get("name"));
    }

    // ==================== 新字段测试（2026-06-07 重构）====================

    @Test
    void testNewFields_pureInsert() {
        // 纯 INSERT：beforeRows 为空，afterRows 有数据
        List<TapdataEvent> events = new ArrayList<>();
        events.add(createTapdataInsertEvent("users", "id", 1, "name", "Alice"));

        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events, TEST_TABLE_NAME, createTestSchema());

        assertEquals(1, mergedRecords.size());
        SmartMerger.MergedRecord record = mergedRecords.get(0);

        // beforeRows 应为空（INSERT 无旧数据）
        assertTrue(record.getBeforeRows().isEmpty());

        // afterRows 应有一条记录
        assertEquals(1, record.getAfterRows().size());
        assertEquals("Alice", record.getAfterRows().iterator().next().get("name"));

        // mainTableBeforePks 应为空
        assertTrue(record.getMainTableBeforePks().isEmpty());

        // mainTableAfterPks 应包含 PK=1
        assertEquals(1, record.getMainTableAfterPks().size());
        assertTrue(record.getMainTableAfterPks().contains(1));

        // tableName 应正确设置
        assertEquals(TEST_TABLE_NAME, record.getTableName());
    }

    @Test
    void testNewFields_insertThenUpdate() {
        // INSERT → UPDATE：beforeRows 有初始状态，afterRows 有最终状态
        List<TapdataEvent> events = new ArrayList<>();
        events.add(createTapdataInsertEvent("users", "id", 1, "name", "Alice"));
        events.add(createTapdataUpdateEvent("users", "id", 1, "name", "Alice", "Alice Updated"));

        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events, TEST_TABLE_NAME, createTestSchema());

        assertEquals(1, mergedRecords.size());
        SmartMerger.MergedRecord record = mergedRecords.get(0);

        // beforeRows 应有一条记录（最小化：只保留第一条 before 状态）
        assertEquals(1, record.getBeforeRows().size());

        // afterRows 应有一条记录（最终状态）
        assertEquals(1, record.getAfterRows().size());
        assertEquals("Alice Updated", record.getAfterRows().iterator().next().get("name"));

        // mainTableAfterPks 应包含 PK=1
        assertTrue(record.getMainTableAfterPks().contains(1));
    }

    @Test
    void testNewFields_insertThenDelete() {
        // INSERT → DELETE：beforeRows 有数据，afterRows 为空
        List<TapdataEvent> events = new ArrayList<>();
        events.add(createTapdataInsertEvent("users", "id", 1, "name", "Alice"));
        events.add(createTapdataDeleteEvent("users", "id", 1));

        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events, TEST_TABLE_NAME, createTestSchema());

        assertEquals(1, mergedRecords.size());
        SmartMerger.MergedRecord record = mergedRecords.get(0);

        // beforeRows 应有一条记录
        assertEquals(1, record.getBeforeRows().size());

        // afterRows 应为空（已删除）
        assertTrue(record.getAfterRows().isEmpty());

        // mainTableBeforePks 应包含 PK=1
        assertTrue(record.getMainTableBeforePks().contains(1));

        // mainTableAfterPks 应为空
        assertTrue(record.getMainTableAfterPks().isEmpty());
    }

    @Test
    void testNewFields_pkChange() {
        // UPDATE PK 变更：会新增一个 MergedRecord
        // 原记录 PK=1 进入 before，新记录 PK=2 进入 after
        List<TapdataEvent> events = new ArrayList<>();
        events.add(createTapdataInsertEvent("users", "id", 1, "name", "Alice"));
        events.add(createTapdataUpdateEventWithPkChange("users", "id", 1, "id", 2, "name", "Alice v2"));

        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events, TEST_TABLE_NAME, createTestSchema());

        // PK 变更会创建两个记录：原 PK=1 的记录 + 新 PK=2 的记录
        assertEquals(2, mergedRecords.size());

        // 找到旧 PK 记录（mainTableBeforePks 包含 1）
        SmartMerger.MergedRecord oldRecord = mergedRecords.stream()
                .filter(r -> r.getMainTableBeforePks().contains(1))
                .findFirst().orElse(null);
        assertNotNull(oldRecord);
        assertTrue(oldRecord.getMainTableBeforePks().contains(1));

        // 找到新 PK 记录（mainTableAfterPks 包含 2）
        SmartMerger.MergedRecord newRecord = mergedRecords.stream()
                .filter(r -> r.getMainTableAfterPks().contains(2))
                .findFirst().orElse(null);
        assertNotNull(newRecord);
        assertTrue(newRecord.getMainTableAfterPks().contains(2));
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
     * 用法：createTapdataUpdateEvent("users", "id", 1, "name", "Alice", "Alice Updated")
     * 表示：before={id:1, name:"Alice"}, after={id:1, name:"Alice Updated"}
     */
    private TapdataEvent createTapdataUpdateEvent(String tableName, Object... keyValues) {
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapUpdateRecordEvent updateEvent = new TapUpdateRecordEvent();
        updateEvent.setTableId(tableName);

        // keyValues 格式：pkField, pkValue, field, beforeValue, afterValue
        // 例如：("users", "id", 1, "name", "Alice", "Alice Updated")
        String pkField = (String) keyValues[0];
        Object pkValue = keyValues[1];

        Map<String, Object> before = new HashMap<>();
        Map<String, Object> after  = new HashMap<>();
        before.put(pkField, pkValue);
        after.put(pkField, pkValue);

        for (int i = 2; i < keyValues.length; i += 3) {
            String field = (String) keyValues[i];
            Object beforeValue = keyValues[i + 1];
            Object afterValue  = keyValues[i + 2];
            before.put(field, beforeValue);
            after.put(field, afterValue);
        }
        updateEvent.setBefore(before);
        updateEvent.setAfter(after);

        tapdataEvent.setTapEvent(updateEvent);
        return tapdataEvent;
    }

    /**
     * 创建 TapdataEvent (DELETE)
     */
    private TapdataEvent createTapdataDeleteEvent(String tableName, Object... keyValues) {
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapDeleteRecordEvent deleteEvent = new TapDeleteRecordEvent();
        deleteEvent.setTableId(tableName);

        Map<String, Object> before = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            before.put((String) keyValues[i], keyValues[i + 1]);
        }
        deleteEvent.setBefore(before);

        tapdataEvent.setTapEvent(deleteEvent);
        return tapdataEvent;
    }

    /**
     * 创建 TapdataEvent (UPDATE with PK change)
     * keyValues 格式：beforeKey, beforeValue, afterKey, afterValue[, fieldName, fieldValue]...
     */
    private TapdataEvent createTapdataUpdateEventWithPkChange(String tableName,
                                                                  Object... keyValues) {
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapUpdateRecordEvent updateEvent = new TapUpdateRecordEvent();
        updateEvent.setTableId(tableName);

        Map<String, Object> before = new HashMap<>();
        Map<String, Object> after  = new HashMap<>();

        // 前4个参数：beforeKey, beforeValue, afterKey, afterValue
        String beforeKey = (String) keyValues[0];
        Object beforeValue = keyValues[1];
        String afterKey   = (String) keyValues[2];
        Object afterValue  = keyValues[3];

        before.put(beforeKey, beforeValue);
        after.put(afterKey, afterValue);

        // 处理其余字段（非 PK 字段）
        for (int i = 4; i < keyValues.length; i += 2) {
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

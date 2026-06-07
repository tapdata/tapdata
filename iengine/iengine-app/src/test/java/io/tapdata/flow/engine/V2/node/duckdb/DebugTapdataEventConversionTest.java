package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 调试测试 - 验证TapdataEvent转换逻辑
 */
class DebugTapdataEventConversionTest {

    @Test
    void testEventConversion() throws Exception {
        // 创建INSERT事件
        TapInsertRecordEvent insertEvent = new TapInsertRecordEvent();
        insertEvent.setTableId("users");
        Map<String, Object> insertAfter = new HashMap<>();
        insertAfter.put("id", 123);
        insertAfter.put("name", "Test User");
        insertEvent.setAfter(insertAfter);
        
        TapdataEvent insertTapdataEvent = new TapdataEvent();
        insertTapdataEvent.setTapEvent(insertEvent);
        
        // 创建UPDATE事件
        TapUpdateRecordEvent updateEvent = new TapUpdateRecordEvent();
        updateEvent.setTableId("users");
        Map<String, Object> updateBefore = new HashMap<>();
        updateBefore.put("id", 123);
        updateEvent.setBefore(updateBefore);
        
        Map<String, Object> updateAfter = new HashMap<>();
        updateAfter.put("id", 456);
        updateAfter.put("name", "Updated User");
        updateEvent.setAfter(updateAfter);
        
        TapdataEvent updateTapdataEvent = new TapdataEvent();
        updateTapdataEvent.setTapEvent(updateEvent);
        
        List<TapdataEvent> events = Arrays.asList(insertTapdataEvent, updateTapdataEvent);
        
        // 验证事件创建正确
        assertEquals(2, events.size());
        assertEquals("users", ((TapInsertRecordEvent) events.get(0).getTapEvent()).getTableId());
        assertEquals("users", ((TapUpdateRecordEvent) events.get(1).getTapEvent()).getTableId());
        
        // 打印事件内容
        System.out.println("=== INSERT Event ===");
        System.out.println("TableId: " + ((TapInsertRecordEvent) events.get(0).getTapEvent()).getTableId());
        System.out.println("After: " + ((TapInsertRecordEvent) events.get(0).getTapEvent()).getAfter());
        
        System.out.println("\n=== UPDATE Event ===");
        System.out.println("TableId: " + ((TapUpdateRecordEvent) events.get(1).getTapEvent()).getTableId());
        System.out.println("Before: " + ((TapUpdateRecordEvent) events.get(1).getTapEvent()).getBefore());
        System.out.println("After: " + ((TapUpdateRecordEvent) events.get(1).getTapEvent()).getAfter());
        
        // 转换为SmartMerger格式
        Map<String, Object> insertMap = convertTapdataEventToMap((TapInsertRecordEvent) events.get(0).getTapEvent());
        Map<String, Object> updateMap = convertTapdataEventToMap((TapUpdateRecordEvent) events.get(1).getTapEvent());
        
        System.out.println("\n=== Converted INSERT Map ===");
        System.out.println(insertMap);
        
        System.out.println("\n=== Converted UPDATE Map ===");
        System.out.println(updateMap);
        
        // 验证转换后的格式
        assertEquals("INSERT", insertMap.get("op"));
        assertEquals(123, insertMap.get("id"));
        
        assertEquals("UPDATE", updateMap.get("op"));
        assertEquals(123, ((Map<?, ?>) updateMap.get("o2")).get("id"));
        assertEquals(456, ((Map<?, ?>) updateMap.get("updatedFields")).get("id"));
        assertEquals(456, updateMap.get("id"));
        
        // 创建测试 schema
        List<String> primaryKeys = Collections.singletonList("id");
        Map<String, TapField> fieldMap = new HashMap<>();
        TapField idField = new TapField("id", "INT");
        idField.setPrimaryKey(true);
        fieldMap.put("id", idField);
        fieldMap.put("name", new TapField("name", "VARCHAR"));
        NodeSchemaInfo schema = new NodeSchemaInfo("test-node", "users", "test.qualified.name",
                primaryKeys, fieldMap, null, null);
        
        // 测试SmartMerger: 使用原始 TapdataEvent 列表（而非 Map 列表）
        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events, "users", schema);

        System.out.println("\n=== SmartMerger Result ===");
        System.out.println("Merged records count: " + mergedRecords.size());
        for (SmartMerger.MergedRecord record : mergedRecords) {
            System.out.println("InitialPk: " + record.getInitialPk());
            System.out.println("CurrentPk: " + record.getCurrentPk());
            System.out.println("Operations: " + record.getOperations());
        }
        
        assertEquals(1, mergedRecords.size());
        assertEquals(123, mergedRecords.get(0).getInitialPk());
        assertEquals(456, mergedRecords.get(0).getCurrentPk());
    }
    
    private Map<String, Object> convertTapdataEventToMap(io.tapdata.entity.event.dml.TapRecordEvent recordEvent) {
        Map<String, Object> mapEvent = new HashMap<>();
        
        if (recordEvent instanceof TapInsertRecordEvent) {
            mapEvent.put("op", "INSERT");
            Map<String, Object> after = io.tapdata.flow.engine.V2.util.TapEventUtil.getAfter(recordEvent);
            if (after != null) {
                mapEvent.putAll(after);
            }
        } else if (recordEvent instanceof TapUpdateRecordEvent) {
            mapEvent.put("op", "UPDATE");
            Map<String, Object> before = io.tapdata.flow.engine.V2.util.TapEventUtil.getBefore(recordEvent);
            Map<String, Object> after = io.tapdata.flow.engine.V2.util.TapEventUtil.getAfter(recordEvent);
            
            if (before != null) {
                mapEvent.put("o2", new HashMap<>(before));
            }
            
            if (after != null) {
                Map<String, Object> updatedFields = new HashMap<>(after);
                mapEvent.put("updatedFields", updatedFields);
                mapEvent.putAll(after);
            }
        }
        
        return mapEvent;
    }
}

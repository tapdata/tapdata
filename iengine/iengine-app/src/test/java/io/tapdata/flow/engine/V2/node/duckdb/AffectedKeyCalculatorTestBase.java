package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * AffectedKeyCalculator 测试抽象基类
 * 提供公共Mock设置、事件构建工具、断言方法
 */
public abstract class AffectedKeyCalculatorTestBase {

    protected DuckDbOperator mockDuckDbOperator;

    @BeforeEach
    void baseSetUp() {
        mockDuckDbOperator = Mockito.mock(DuckDbOperator.class);
    }

    // ==================== 旧模式计算器工厂 ====================

    protected AffectedKeyCalculator createOldModeCalculator(
            List<FromTableConfig> fromTables,
            Map<String, String> customJoinQueries
    ) {
        return new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );
    }

    protected AffectedKeyCalculator createOldModeCalculator(
            String wideTablePk,
            String mainTable,
            String mainTablePk,
            List<FromTableConfig> fromTables,
            Map<String, String> customJoinQueries
    ) {
        return new AffectedKeyCalculator(
                wideTablePk,
                mainTable,
                mainTablePk,
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );
    }

    // ==================== 新模式计算器工厂 ====================

    protected AffectedKeyCalculator createNewModeCalculator(
            List<FromTableConfig> fromTables
    ) {
        return new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                Collections.emptyMap(),
                mockDuckDbOperator,
                new WithCteSqlGenerator()
        );
    }

    protected AffectedKeyCalculator createNewModeCalculator(
            String wideTablePk,
            String mainTable,
            String mainTablePk,
            List<FromTableConfig> fromTables
    ) {
        return new AffectedKeyCalculator(
                wideTablePk,
                mainTable,
                mainTablePk,
                fromTables,
                Collections.emptyMap(),
                mockDuckDbOperator,
                new WithCteSqlGenerator()
        );
    }

    // ==================== 旧模式事件构建器 ====================

    protected Map<String, Object> createInsertEvent(String pkField, Object pkValue) {
        Map<String, Object> event = new HashMap<>();
        event.put(pkField, pkValue);
        return event;
    }

    protected Map<String, Object> createUpdateEvent(String pkField, Object pkValue) {
        Map<String, Object> event = new HashMap<>();
        event.put(pkField, pkValue);
        return event;
    }

    protected Map<String, Object> createDeleteEvent(String pkField, Object pkValue) {
        Map<String, Object> event = new HashMap<>();
        event.put(pkField, pkValue);
        return event;
    }

    protected Map<String, Object> createEventWithAfter(String pkField, Object pkValue) {
        Map<String, Object> event = new HashMap<>();
        Map<String, Object> after = new HashMap<>();
        after.put(pkField, pkValue);
        event.put("after", after);
        return event;
    }

    protected Map<String, Object> createEventWithBefore(String pkField, Object pkValue) {
        Map<String, Object> event = new HashMap<>();
        Map<String, Object> before = new HashMap<>();
        before.put(pkField, pkValue);
        event.put("before", before);
        return event;
    }

    /**
     * 创建旧模式格式的事件（支持INSERT/UPDATE/DELETE操作类型）
     */
    protected Map<String, Object> createEvent(String pkField, Object pkValue, String operation) {
        Map<String, Object> event = new HashMap<>();
        if ("DELETE".equals(operation)) {
            event.put("before", Collections.singletonMap(pkField, pkValue));
        } else if ("UPDATE".equals(operation)) {
            event.put("before", Collections.singletonMap(pkField, pkValue));
            event.put("after", Collections.singletonMap(pkField, pkValue));
        } else {
            event.put(pkField, pkValue);
        }
        return event;
    }

    // ==================== 新模式SmartMerger事件构建器 ====================

    protected List<Map<String, Object>> createSmartMergerInsertEvents(String pkField, Object... pkValues) {
        List<Map<String, Object>> events = new ArrayList<>();
        for (Object pk : pkValues) {
            Map<String, Object> event = new HashMap<>();
            event.put("op", "INSERT");
            event.put(pkField, pk);
            events.add(event);
        }
        return events;
    }

    protected List<Map<String, Object>> createSmartMergerUpdateEvents(
            String pkField,
            Object oldPk,
            Object newPk
    ) {
        List<Map<String, Object>> events = new ArrayList<>();
        // 先INSERT
        Map<String, Object> insert = new HashMap<>();
        insert.put("op", "INSERT");
        insert.put(pkField, oldPk);
        events.add(insert);
        
        // 再UPDATE
        Map<String, Object> update = new HashMap<>();
        update.put("op", "UPDATE");
        update.put("o2", Map.of(pkField, oldPk));
        update.put("updatedFields", Map.of(pkField, newPk));
        events.add(update);
        
        return events;
    }

    protected List<Map<String, Object>> createSmartMergerDeleteEvents(String pkField, Object... pkValues) {
        List<Map<String, Object>> events = new ArrayList<>();
        // 先INSERT所有记录
        for (Object pk : pkValues) {
            Map<String, Object> insert = new HashMap<>();
            insert.put("op", "INSERT");
            insert.put(pkField, pk);
            events.add(insert);
        }
        // 再DELETE所有记录
        for (Object pk : pkValues) {
            Map<String, Object> delete = new HashMap<>();
            delete.put("op", "DELETE");
            delete.put("o", Map.of(pkField, pk));
            delete.put("o2", Map.of(pkField, pk));
            events.add(delete);
        }
        return events;
    }

    // ==================== 公共断言 ====================

    protected void assertContainsKeys(Set<Object> result, Object... expectedKeys) {
        assertNotNull(result);
        for (Object expected : expectedKeys) {
            assertTrue(result.contains(expected), "Expected to contain: " + expected);
        }
    }

    protected void assertEmptyKeys(Set<Object> result) {
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    // ==================== Mock工具 ====================

    @SuppressWarnings("unchecked")
    protected void mockQueryReturns(List<Map<String, Object>> queryResult) {
        try {
            when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void mockQueryThrows(SQLException exception) {
        try {
            doThrow(exception).when(mockDuckDbOperator).executeQuery(anyString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected List<Map<String, Object>> createQueryResult(Object... pkValues) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object pk : pkValues) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", pk);
            result.add(row);
        }
        return result;
    }

    protected List<Map<String, Object>> createWideTableRow(Object pk, String... extraFields) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", pk);
        for (int i = 0; i < extraFields.length; i += 2) {
            if (i + 1 < extraFields.length) {
                row.put(extraFields[i], extraFields[i + 1]);
            }
        }
        return Collections.singletonList(row);
    }

    // ==================== 新模式TapdataEvent事件构建器 ====================

    /**
     * 将SmartMerger格式的事件列表转换为TapdataEvent列表
     */
    protected List<TapdataEvent> createTapdataEvents(String tableName, List<Map<String, Object>> smartMergerEvents) {
        List<TapdataEvent> events = new ArrayList<>();
        for (Map<String, Object> event : smartMergerEvents) {
            String op = (String) event.get("op");
            TapdataEvent tapdataEvent = new TapdataEvent();
            TapRecordEvent recordEvent;
            
            if ("INSERT".equals(op)) {
                TapInsertRecordEvent insertEvent = new TapInsertRecordEvent();
                insertEvent.setTableId(tableName);
                Map<String, Object> after = new HashMap<>(event);
                after.remove("op");
                insertEvent.setAfter(after);
                recordEvent = insertEvent;
            } else if ("UPDATE".equals(op)) {
                TapUpdateRecordEvent updateEvent = new TapUpdateRecordEvent();
                updateEvent.setTableId(tableName);
                Map<String, Object> after = new HashMap<>(event);
                after.remove("op");
                after.remove("o");
                after.remove("o2");
                after.remove("updatedFields");
                after.remove("old_pk");
                after.remove("fields");
                updateEvent.setAfter(after);
                
                // 提取before数据
                @SuppressWarnings("unchecked")
                Map<String, Object> before = (Map<String, Object>) event.get("o2");
                if (before == null) {
                    before = (Map<String, Object>) event.get("o");
                }
                if (before != null) {
                    updateEvent.setBefore(new HashMap<>(before));
                }
                recordEvent = updateEvent;
            } else if ("DELETE".equals(op)) {
                TapDeleteRecordEvent deleteEvent = new TapDeleteRecordEvent();
                deleteEvent.setTableId(tableName);
                @SuppressWarnings("unchecked")
                Map<String, Object> before = (Map<String, Object>) event.get("o2");
                if (before == null) {
                    before = (Map<String, Object>) event.get("o");
                }
                if (before != null) {
                    deleteEvent.setBefore(new HashMap<>(before));
                }
                recordEvent = deleteEvent;
            } else {
                continue;
            }
            
            tapdataEvent.setTapEvent(recordEvent);
            events.add(tapdataEvent);
        }
        return events;
    }

    /**
     * 创建单个INSERT TapdataEvent
     */
    protected TapdataEvent createInsertTapdataEvent(String tableName, String pkField, Object pkValue) {
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapInsertRecordEvent insertEvent = new TapInsertRecordEvent();
        insertEvent.setTableId(tableName);
        Map<String, Object> after = new HashMap<>();
        after.put(pkField, pkValue);
        insertEvent.setAfter(after);
        tapdataEvent.setTapEvent(insertEvent);
        return tapdataEvent;
    }

    /**
     * 创建单个UPDATE TapdataEvent
     */
    protected TapdataEvent createUpdateTapdataEvent(String tableName, String pkField, Object oldPk, Object newPk) {
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapUpdateRecordEvent updateEvent = new TapUpdateRecordEvent();
        updateEvent.setTableId(tableName);
        
        Map<String, Object> before = new HashMap<>();
        before.put(pkField, oldPk);
        updateEvent.setBefore(before);
        
        Map<String, Object> after = new HashMap<>();
        after.put(pkField, newPk);
        updateEvent.setAfter(after);
        
        tapdataEvent.setTapEvent(updateEvent);
        return tapdataEvent;
    }

    /**
     * 创建单个DELETE TapdataEvent
     */
    protected TapdataEvent createDeleteTapdataEvent(String tableName, String pkField, Object pkValue) {
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapDeleteRecordEvent deleteEvent = new TapDeleteRecordEvent();
        deleteEvent.setTableId(tableName);
        Map<String, Object> before = new HashMap<>();
        before.put(pkField, pkValue);
        deleteEvent.setBefore(before);
        tapdataEvent.setTapEvent(deleteEvent);
        return tapdataEvent;
    }
}

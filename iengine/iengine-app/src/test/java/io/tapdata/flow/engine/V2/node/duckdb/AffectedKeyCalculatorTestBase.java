package io.tapdata.flow.engine.V2.node.duckdb;

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
}

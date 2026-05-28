package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 集成测试 - WITH CTE 端到端流程
 * 验证 AffectedKeyCalculator + WithCteSqlGenerator + WideTableIncrementalUpdater 联合工作
 */
@ExtendWith(MockitoExtension.class)
class WithCteIntegrationTest {

    @Mock
    private DuckDbOperator mockDuckDbOperator;

    private AffectedKeyCalculator calculator;
    private WithCteSqlGenerator sqlGenerator;
    private WideTableIncrementalUpdater updater;

    @BeforeEach
    void setUp() throws SQLException {
        List<FromTableConfig> fromTables = Arrays.asList(
                new FromTableConfig("users", "id", "SELECT id, name, email FROM users", Arrays.asList("id", "name", "email"))
        );

        sqlGenerator = new WithCteSqlGenerator();
        
        calculator = new AffectedKeyCalculator(
                "id", "users", "id", fromTables, new HashMap<>(), mockDuckDbOperator, sqlGenerator
        );

        List<String> fields = Arrays.asList("id", "name", "email");
        updater = new WideTableIncrementalUpdater("users", "id",
                "SELECT id, name, email FROM users",
                fields, sqlGenerator, mockDuckDbOperator);
    }

    @Test
    void testEndToEnd_WithCteFlow() throws SQLException, IOException {
        // 1. CDC 事件 - 使用SmartMerger格式（需要先INSERT再UPDATE）
        List<Map<String, Object>> smartMergerEvents = new ArrayList<>();
        // 第一个记录：先INSERT id=123，再UPDATE id=123→456
        smartMergerEvents.add(createSmartMergerInsertEvent("id", 123));
        smartMergerEvents.add(createSmartMergerUpdateEvent("id", 123, 456));
        // 第二个记录：先INSERT id=789，再UPDATE id=789→789（主键不变）
        smartMergerEvents.add(createSmartMergerInsertEvent("id", 789));
        smartMergerEvents.add(createSmartMergerUpdateEvent("id", 789, 789));

        // 转换为TapdataEvent
        List<TapdataEvent> events = createTapdataEventsFromSmartMerger("users", smartMergerEvents);

        // 2. 计算 before/after 主键（mock未配置，返回空）
        // 注意：实际应用中需要配置mock返回宽表查询结果
        Set<Object> beforeKeys = new LinkedHashSet<>(Arrays.asList(123, 789));
        Set<Object> afterKeys = new LinkedHashSet<>(Arrays.asList(456, 789));

        // 3. 提取 after 数据行
        List<Map<String, Object>> afterRows = new ArrayList<>();
        Map<String, Object> afterRow1 = new HashMap<>();
        afterRow1.put("id", 456);
        afterRow1.put("name", "John");
        afterRow1.put("email", "john@example.com");
        afterRows.add(afterRow1);

        Map<String, Object> afterRow2 = new HashMap<>();
        afterRow2.put("id", 789);
        afterRow2.put("name", "Jane Updated");
        afterRow2.put("email", "jane@example.com");
        afterRows.add(afterRow2);

        // 4. Mock DuckDB 查询结果
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(afterRows);

        // 5. 执行宽表更新
        List<TapdataEvent> updateEvents = updater.updateWideTableAsTapdataEvents(
                beforeKeys, afterKeys, afterRows, "users");

        // 6. 验证事件
        List<TapdataEvent> deleteEvents = filterByType(updateEvents, TapDeleteRecordEvent.class);
        assertEquals(1, deleteEvents.size());

        List<TapdataEvent> insertEvents = filterByType(updateEvents, TapInsertRecordEvent.class);
        assertEquals(1, insertEvents.size());

        List<TapdataEvent> updateEventsList = filterByType(updateEvents, TapUpdateRecordEvent.class);
        assertEquals(1, updateEventsList.size());

        // 7. 验证 WITH CTE SQL 被生成
        verify(mockDuckDbOperator).executeQuery(argThat(sql ->
                sql != null && sql.contains("WITH users AS") && sql.contains("VALUES")
        ));
    }

    @Test
    void testEndToEnd_ComplexJoinSql() throws SQLException, IOException {
        // 使用复杂 SQL（JOIN）
        String complexSql = "SELECT u.id, u.name, o.order_id, o.amount " +
                "FROM users u INNER JOIN orders o ON u.id = o.user_id " +
                "WHERE u.status = 1";

        WideTableIncrementalUpdater complexUpdater = new WideTableIncrementalUpdater(
                "wide_table", "id", complexSql, Arrays.asList("id", "name"), sqlGenerator, mockDuckDbOperator);

        Set<Object> beforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        Set<Object> afterKeys = new LinkedHashSet<>(Collections.singletonList(456));

        List<Map<String, Object>> afterRows = Collections.singletonList(createRow(456, "John"));
        List<Map<String, Object>> queryResult = Collections.singletonList(createRow(456, "John"));
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        List<TapdataEvent> events = complexUpdater.updateWideTableAsTapdataEvents(
                beforeKeys, afterKeys, afterRows, "users");

        assertEquals(2, events.size());

        // 验证 WITH CTE SQL 包含复杂 JOIN
        verify(mockDuckDbOperator).executeQuery(argThat(sql ->
                sql != null && sql.contains("WITH users AS") && sql.contains("INNER JOIN")
        ));
    }

    // ==================== Helper Methods ====================

    /**
     * 创建SmartMerger格式的INSERT事件
     */
    private Map<String, Object> createSmartMergerInsertEvent(String pkField, Object pkValue) {
        Map<String, Object> event = new HashMap<>();
        event.put("op", "INSERT");
        event.put(pkField, pkValue);
        event.put("name", "Test User");
        event.put("email", "test@example.com");
        return event;
    }

    /**
     * 创建SmartMerger格式的UPDATE事件
     */
    private Map<String, Object> createSmartMergerUpdateEvent(String pkField, Object beforePk, Object afterPk) {
        Map<String, Object> event = new HashMap<>();
        event.put("op", "UPDATE");
        // o2存放before数据（用于SmartMerger提取旧主键）
        Map<String, Object> beforeData = new HashMap<>();
        beforeData.put(pkField, beforePk);
        event.put("o2", beforeData);
        // updatedFields存放更新的字段
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put(pkField, afterPk);
        event.put("updatedFields", updatedFields);
        // 顶层也要有after的主键（用于SmartMerger提取当前主键）
        event.put(pkField, afterPk);
        return event;
    }

    /**
     * 将SmartMerger格式的事件列表转换为TapdataEvent列表
     */
    private List<TapdataEvent> createTapdataEventsFromSmartMerger(String tableName, List<Map<String, Object>> smartMergerEvents) {
        List<TapdataEvent> events = new ArrayList<>();
        for (Map<String, Object> event : smartMergerEvents) {
            String op = (String) event.get("op");
            TapdataEvent tapdataEvent = new TapdataEvent();
            io.tapdata.entity.event.dml.TapRecordEvent recordEvent;
            
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
     * 将TapRecordEvent转换为SmartMerger兼容的Map格式
     */
    private Map<String, Object> convertTapdataEventToMap(io.tapdata.entity.event.dml.TapRecordEvent recordEvent) {
        Map<String, Object> mapEvent = new HashMap<>();
        
        if (recordEvent instanceof io.tapdata.entity.event.dml.TapInsertRecordEvent) {
            mapEvent.put("op", "INSERT");
            Map<String, Object> after = io.tapdata.flow.engine.V2.util.TapEventUtil.getAfter(recordEvent);
            if (after != null) {
                mapEvent.putAll(after);
            }
        } else if (recordEvent instanceof io.tapdata.entity.event.dml.TapUpdateRecordEvent) {
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
        } else if (recordEvent instanceof io.tapdata.entity.event.dml.TapDeleteRecordEvent) {
            mapEvent.put("op", "DELETE");
            Map<String, Object> before = io.tapdata.flow.engine.V2.util.TapEventUtil.getBefore(recordEvent);
            if (before != null) {
                mapEvent.put("o2", new HashMap<>(before));
                mapEvent.put("o", new HashMap<>(before));
            }
        }
        
        return mapEvent;
    }

    private Map<String, Object> createUpdateEvent(String pkField, Object beforePk, Object afterPk) {
        Map<String, Object> event = new HashMap<>();
        Map<String, Object> before = new HashMap<>();
        before.put(pkField, beforePk);
        event.put("before", before);
        Map<String, Object> after = new HashMap<>();
        after.put(pkField, afterPk);
        event.put("after", after);
        return event;
    }

    private Map<String, Object> createRow(Object id, String name) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", name);
        return row;
    }

    private List<TapdataEvent> filterByType(List<TapdataEvent> events, Class<?> eventType) {
        List<TapdataEvent> filtered = new ArrayList<>();
        for (TapdataEvent event : events) {
            if (eventType.isInstance(event.getTapEvent())) {
                filtered.add(event);
            }
        }
        return filtered;
    }
}

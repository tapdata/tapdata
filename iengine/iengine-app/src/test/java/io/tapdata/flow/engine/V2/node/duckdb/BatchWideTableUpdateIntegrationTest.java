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
import static org.mockito.Mockito.when;

/**
 * 集成测试 - 端到端批量处理
 * 验证 AffectedKeyCalculator + WideTableIncrementalUpdater 联合工作
 */
@ExtendWith(MockitoExtension.class)
class BatchWideTableUpdateIntegrationTest {

    @Mock
    private DuckDbOperator mockDuckDbOperator;

    private AffectedKeyCalculator calculator;
    private WideTableIncrementalUpdater updater;

    @BeforeEach
    void setUp() throws SQLException {
        // 配置源表
        List<FromTableConfig> fromTables = Arrays.asList(
                new FromTableConfig("users", "id"),
                new FromTableConfig("orders", "user_id")
        );

        // 初始化计算器
        calculator = new AffectedKeyCalculator(
                "id", "users", "id", fromTables, new HashMap<>(), mockDuckDbOperator
        );

        // 初始化更新器
        List<String> fields = Arrays.asList("id", "name", "email");
        updater = new WideTableIncrementalUpdater("users", "id",
                "SELECT id, name, email FROM users",
                fields, new WithCteSqlGenerator(), mockDuckDbOperator);
    }

    @Test
    void testBatchProcessing_multiTableMixedEvents() throws SQLException, IOException {
        // CDC 事件缓冲区（多表混合）
        Map<String, List<Map<String, Object>>> eventsByTable = new HashMap<>();

        // 主表事件：主键更新 + 普通更新
        List<Map<String, Object>> userEvents = new ArrayList<>();
        userEvents.add(createUpdateEvent("id", 123, 456)); // 主键更新 123 -> 456
        userEvents.add(createUpdateEvent("id", 789, 789)); // 普通更新（主键不变）
        eventsByTable.put("users", userEvents);

        // Mock DuckDB 查询结果
        List<Map<String, Object>> queryResult = Arrays.asList(
                createWideTableRow(456, "John", "john@example.com"),
                createWideTableRow(789, "Jane Updated", "jane@example.com")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        // 执行批量计算
        Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);
        Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);

        // 验证 before keys
        assertEquals(2, beforeKeys.size());
        assertTrue(beforeKeys.contains(123));
        assertTrue(beforeKeys.contains(789));

        // 验证 after keys
        assertEquals(2, afterKeys.size());
        assertTrue(afterKeys.contains(456));
        assertTrue(afterKeys.contains(789));

        // 执行宽表更新
        List<Map<String, Object>> afterRows = queryResult;
        List<TapdataEvent> events = updater.updateWideTableAsTapdataEvents(beforeKeys, afterKeys, afterRows, "users");

        // 验证事件生成
        List<TapdataEvent> deleteEvents = filterByType(events, TapDeleteRecordEvent.class);
        assertEquals(1, deleteEvents.size());

        List<TapdataEvent> insertEvents = filterByType(events, TapInsertRecordEvent.class);
        assertEquals(1, insertEvents.size());

        List<TapdataEvent> updateEvents = filterByType(events, TapUpdateRecordEvent.class);
        assertEquals(1, updateEvents.size());
    }

    @Test
    void testBatchProcessing_pureInsertScenario() throws SQLException, IOException {
        Map<String, List<Map<String, Object>>> eventsByTable = new HashMap<>();

        List<Map<String, Object>> userEvents = new ArrayList<>();
        userEvents.add(createInsertEvent("id", 100));
        userEvents.add(createInsertEvent("id", 200));
        eventsByTable.put("users", userEvents);

        List<Map<String, Object>> queryResult = Arrays.asList(
                createWideTableRow(100, "New User 1", "user1@example.com"),
                createWideTableRow(200, "New User 2", "user2@example.com")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);
        Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);

        assertTrue(beforeKeys.isEmpty());
        assertEquals(2, afterKeys.size());

        List<TapdataEvent> events = updater.updateWideTableAsTapdataEvents(beforeKeys, afterKeys, queryResult, "users");

        assertEquals(2, events.size());
        assertEquals(2, filterByType(events, TapInsertRecordEvent.class).size());
    }

    @Test
    void testBatchProcessing_pureDeleteScenario() throws SQLException, IOException {
        Map<String, List<Map<String, Object>>> eventsByTable = new HashMap<>();

        List<Map<String, Object>> userEvents = new ArrayList<>();
        userEvents.add(createDeleteEvent("id", 100));
        userEvents.add(createDeleteEvent("id", 200));
        eventsByTable.put("users", userEvents);

        Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);
        Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);

        assertEquals(2, beforeKeys.size());
        assertTrue(afterKeys.isEmpty());

        List<TapdataEvent> events = updater.updateWideTableAsTapdataEvents(beforeKeys, afterKeys, Collections.emptyList(), "users");

        assertEquals(2, events.size());
        assertEquals(2, filterByType(events, TapDeleteRecordEvent.class).size());
    }

    // ==================== Helper Methods ====================

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

    private Map<String, Object> createInsertEvent(String pkField, Object pk) {
        Map<String, Object> event = new HashMap<>();
        Map<String, Object> after = new HashMap<>();
        after.put(pkField, pk);
        event.put("after", after);
        return event;
    }

    private Map<String, Object> createDeleteEvent(String pkField, Object pk) {
        Map<String, Object> event = new HashMap<>();
        Map<String, Object> before = new HashMap<>();
        before.put(pkField, pk);
        event.put("before", before);
        return event;
    }

    private Map<String, Object> createWideTableRow(Object id, String name, String email) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", name);
        row.put("email", email);
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

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
        // 配置源表（带 querySql 和 fields）
        List<FromTableConfig> fromTables = Arrays.asList(
                new FromTableConfig("users", "id", "SELECT id, name, email FROM users", Arrays.asList("id", "name", "email")),
                new FromTableConfig("orders", "user_id", "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id", Arrays.asList("user_id", "order_id"))
        );

        WithCteSqlGenerator withCteSqlGenerator = new WithCteSqlGenerator();

        // 初始化计算器
        calculator = new AffectedKeyCalculator(
                "id", "users", "id", fromTables, new HashMap<>(), mockDuckDbOperator, withCteSqlGenerator
        );

        // 初始化更新器
        List<String> fields = Arrays.asList("id", "name", "email");
        updater = new WideTableIncrementalUpdater("users", "id",
                "SELECT id, name, email FROM users",
                fields, new WithCteSqlGenerator(), mockDuckDbOperator);
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

        // SmartMerger 要求先有 INSERT 再 DELETE
        List<Map<String, Object>> userEvents = new ArrayList<>();
        userEvents.add(createInsertEvent("id", 100));
        userEvents.add(createInsertEvent("id", 200));
        userEvents.add(createDeleteEvent("id", 100));
        userEvents.add(createDeleteEvent("id", 200));
        eventsByTable.put("users", userEvents);

        // Mock DuckDB 查询结果（DELETE 场景需要查询 before 数据对应的宽表主键）
        List<Map<String, Object>> queryResult = Arrays.asList(
                createWideTableRow(100, "User 100", "user100@example.com"),
                createWideTableRow(200, "User 200", "user200@example.com")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);
        Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);

        assertEquals(2, beforeKeys.size());
        assertTrue(afterKeys.isEmpty());

        List<TapdataEvent> events = updater.updateWideTableAsTapdataEvents(beforeKeys, afterKeys, Collections.emptyList(), "users");

        assertEquals(2, events.size());
        assertEquals(2, filterByType(events, TapDeleteRecordEvent.class).size());
    }

    @Test
    void testBatchProcessing_updateScenario() throws SQLException, IOException {
        // CDC 事件缓冲区：单条记录主键更新
        Map<String, List<Map<String, Object>>> eventsByTable = new HashMap<>();

        List<Map<String, Object>> userEvents = new ArrayList<>();
        userEvents.add(createInsertEvent("id", 123));
        userEvents.add(createUpdateEvent("id", 123, 456));
        eventsByTable.put("users", userEvents);

        // Mock DuckDB 查询结果
        List<Map<String, Object>> queryResult = Arrays.asList(
                createWideTableRow(456, "John", "john@example.com")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);
        Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);

        // 验证 before keys 和 after keys 都不为空
        assertFalse(beforeKeys.isEmpty());
        assertFalse(afterKeys.isEmpty());

        // 执行宽表更新
        List<TapdataEvent> events = updater.updateWideTableAsTapdataEvents(beforeKeys, afterKeys, queryResult, "users");

        // 验证事件生成（至少有一个事件）
        assertFalse(events.isEmpty());
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> createUpdateEvent(String pkField, Object beforePk, Object afterPk) {
        Map<String, Object> event = new HashMap<>();
        event.put("op", "UPDATE");
        event.put("o2", Map.of(pkField, beforePk));
        event.put("updatedFields", Map.of(pkField, afterPk));
        return event;
    }

    private Map<String, Object> createInsertEvent(String pkField, Object pk) {
        Map<String, Object> event = new HashMap<>();
        event.put("op", "INSERT");
        event.put(pkField, pk);
        return event;
    }

    private Map<String, Object> createDeleteEvent(String pkField, Object pk) {
        Map<String, Object> event = new HashMap<>();
        event.put("op", "DELETE");
        // SmartMerger's extractPk checks o/o2 for DELETE events
        event.put("o", Map.of(pkField, pk));
        event.put("o2", Map.of(pkField, pk));
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

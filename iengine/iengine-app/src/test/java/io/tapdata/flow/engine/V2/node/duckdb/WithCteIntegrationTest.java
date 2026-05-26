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
                new FromTableConfig("users", "id")
        );

        calculator = new AffectedKeyCalculator(
                "id", "users", "id", fromTables, new HashMap<>(), mockDuckDbOperator
        );

        sqlGenerator = new WithCteSqlGenerator();

        List<String> fields = Arrays.asList("id", "name", "email");
        updater = new WideTableIncrementalUpdater("users", "id",
                "SELECT id, name, email FROM users",
                fields, sqlGenerator, mockDuckDbOperator);
    }

    @Test
    void testEndToEnd_WithCteFlow() throws SQLException, IOException {
        // 1. CDC 事件
        Map<String, List<Map<String, Object>>> eventsByTable = new HashMap<>();
        List<Map<String, Object>> userEvents = new ArrayList<>();
        userEvents.add(createUpdateEvent("id", 123, 456));
        userEvents.add(createUpdateEvent("id", 789, 789));
        eventsByTable.put("users", userEvents);

        // 2. 计算 before/after 主键
        Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);
        Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);

        assertEquals(2, beforeKeys.size());
        assertTrue(beforeKeys.contains(123));
        assertTrue(beforeKeys.contains(789));

        assertEquals(2, afterKeys.size());
        assertTrue(afterKeys.contains(456));
        assertTrue(afterKeys.contains(789));

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
        List<TapdataEvent> events = updater.updateWideTableAsTapdataEvents(
                beforeKeys, afterKeys, afterRows, "users");

        // 6. 验证事件
        List<TapdataEvent> deleteEvents = filterByType(events, TapDeleteRecordEvent.class);
        assertEquals(1, deleteEvents.size());

        List<TapdataEvent> insertEvents = filterByType(events, TapInsertRecordEvent.class);
        assertEquals(1, insertEvents.size());

        List<TapdataEvent> updateEvents = filterByType(events, TapUpdateRecordEvent.class);
        assertEquals(1, updateEvents.size());

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

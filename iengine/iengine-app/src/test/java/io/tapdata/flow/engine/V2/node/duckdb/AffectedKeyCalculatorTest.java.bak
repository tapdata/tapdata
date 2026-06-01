package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AffectedKeyCalculatorTest {

    private DuckDbOperator mockDuckDbOperator;
    private AffectedKeyCalculator affectedKeyCalculator;

    @BeforeEach
    void setUp() {
        mockDuckDbOperator = Mockito.mock(DuckDbOperator.class);

        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        affectedKeyCalculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );
    }

    @Test
    void testCalculateAffectedKeys_MainTableInsert() throws SQLException {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("id", 1L);
        eventData.put("name", "Alice");

        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(1L));
    }

    @Test
    void testCalculateAffectedKeys_MainTableUpdate() throws SQLException {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("id", 2L);
        eventData.put("name", "Updated Bob");

        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(2L));
    }

    @Test
    void testCalculateAffectedKeys_MainTableDelete() throws SQLException {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("id", 3L);
        eventData.put("name", "Charlie");

        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(3L));
    }

    @Test
    void testCalculateAffectedKeys_FromTableWithCustomQuery() throws SQLException {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("order_id", "ORD001");
        eventData.put("user_id", 1L);
        eventData.put("amount", 100.0);

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1L);
        queryResult.add(row);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(1L));
    }

    @Test
    void testCalculateAffectedBeforeKeys_UnknownTable() throws SQLException {
        List<TapdataEvent> events = new ArrayList<>();
        TapdataEvent event = createTapdataInsertEvent("unknown_table", "id", 1L);
        events.add(event);

        Set<Object> result = affectedKeyCalculator.calculateAffectedBeforeKeys(events, "unknown_table");

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testCalculateAffectedKeys_MissingPrimaryKey() throws SQLException {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("name", "Alice");

        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testCalculateAffectedKeys_NullEvents() throws SQLException {
        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("users", null);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testCalculateAffectedKeys_EmptyEvents() throws SQLException {
        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("users", Collections.emptyList());

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testCalculateAffectedKeys_FromTableWithEmptyResult() throws SQLException {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("order_id", "ORD002");

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(new ArrayList<>());

        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testCalculateAffectedKeys_FromTableQueryFails() throws SQLException {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("order_id", "ORD003");

        when(mockDuckDbOperator.executeQuery(anyString())).thenThrow(new SQLException("Query failed"));

        assertThrows(SQLException.class, () -> {
            affectedKeyCalculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));
        });
    }

    @Test
    void testCalculateAffectedKeys_MultipleFromTables() throws SQLException {
        List<FromTableConfig> multiFromTables = new ArrayList<>();

        FromTableConfig orders = new FromTableConfig();
        orders.setPreNodeId("orders");
        orders.setTableNameInSql("order_id");
        multiFromTables.add(orders);

        FromTableConfig payments = new FromTableConfig();
        payments.setPreNodeId("payments");
        payments.setTableNameInSql("payment_id");
        multiFromTables.add(payments);

        Map<String, String> multiQueries = new HashMap<>();
        multiQueries.put("orders", "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");
        multiQueries.put("payments", "SELECT u.id FROM users u JOIN payments p ON u.id = p.user_id WHERE p.payment_id IN (${pkValues})");

        AffectedKeyCalculator multiCalculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                multiFromTables,
                multiQueries,
                mockDuckDbOperator
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("payment_id", "PAY001");

        assertDoesNotThrow(() -> multiCalculator.calculateAffectedKeys("payments", Collections.singletonList(eventData)));
    }

    @Test
    void testCalculateAffectedKeys_NullPrimaryKeyInData() throws SQLException {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("id", null);
        eventData.put("name", "Alice");

        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testCalculateAffectedKeys_PrimaryKeyInteger() throws SQLException {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("id", 123);
        eventData.put("name", "Alice");

        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(123));
    }

    @Test
    void testCalculateAffectedKeys_PrimaryKeyString() throws SQLException {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("id", "user_001");
        eventData.put("name", "Alice");

        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains("user_001"));
    }

    @Test
    void testCalculateAffectedKeys_MultipleAffectedKeys() throws SQLException {
        Map<String, Object> eventData1 = new HashMap<>();
        eventData1.put("order_id", "ORD004");
        
        Map<String, Object> eventData2 = new HashMap<>();
        eventData2.put("order_id", "ORD005");

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1L);
        queryResult.add(row1);
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 2L);
        queryResult.add(row2);
        Map<String, Object> row3 = new HashMap<>();
        row3.put("id", 3L);
        queryResult.add(row3);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("orders", Arrays.asList(eventData1, eventData2));

        assertNotNull(result);
        assertEquals(3, result.size());
        assertTrue(result.contains(1L));
        assertTrue(result.contains(2L));
        assertTrue(result.contains(3L));
    }

    @Test
    void testCalculateAffectedKeys_NullFromTables() throws SQLException {
        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                null,
                new HashMap<>(),
                mockDuckDbOperator
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("id", 1L);

        Set<Object> result = calculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    void testCalculateAffectedKeys_NullCustomQueries() throws SQLException {
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                null,
                mockDuckDbOperator
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("order_id", "ORD005");

        // Now we expect an exception when no custom query is configured
        assertThrows(SQLException.class, () -> {
            calculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));
        });
    }

    @Test
    void testCalculateAffectedKeys_WithAfterField() throws SQLException {
        Map<String, Object> eventData = new HashMap<>();
        Map<String, Object> after = new HashMap<>();
        after.put("id", 5L);
        after.put("name", "Eve");
        eventData.put("after", after);

        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(5L));
    }

    @Test
    void testCalculateAffectedKeys_WithBeforeField() throws SQLException {
        Map<String, Object> eventData = new HashMap<>();
        Map<String, Object> before = new HashMap<>();
        before.put("id", 6L);
        before.put("name", "Frank");
        eventData.put("before", before);

        Set<Object> result = affectedKeyCalculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(6L));
    }

    // 新增：任务 1 - Join key 为非主键字段
    @Test
    void testCalculateAffectedKeys_NonPrimaryKeyJoin() throws SQLException {
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("user_profiles");
        fromTable.setTableNameInSql("profile_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("user_profiles", "SELECT DISTINCT u.id FROM users u INNER JOIN user_profiles p ON u.email = p.email WHERE p.profile_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("profile_id", "PROF001");
        eventData.put("email", "test@example.com");

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 100L);
        queryResult.add(row);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("user_profiles", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(100L));
    }

    // 新增：任务 2 - 多表链式关联
    @Test
    void testCalculateAffectedKeys_MultiTableChainedJoin() throws SQLException {
        List<FromTableConfig> fromTables = new ArrayList<>();

        FromTableConfig ordersTable = new FromTableConfig();
        ordersTable.setPreNodeId("orders");
        ordersTable.setTableNameInSql("order_id");
        fromTables.add(ordersTable);

        FromTableConfig itemsTable = new FromTableConfig();
        itemsTable.setPreNodeId("order_items");
        itemsTable.setTableNameInSql("item_id");
        fromTables.add(itemsTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");
        customJoinQueries.put("order_items", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id INNER JOIN order_items i ON o.order_id = i.order_id WHERE i.item_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("item_id", "ITEM001");
        eventData.put("order_id", "ORD001");

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 200L);
        queryResult.add(row);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("order_items", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(200L));
    }

    // 新增：任务 3 - 主键类型混合
    @Test
    void testCalculateAffectedKeys_MixedPrimaryKeyTypes() throws SQLException {
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        Map<String, Object> eventData1 = new HashMap<>();
        eventData1.put("order_id", "ORD_STR_1");

        Map<String, Object> eventData2 = new HashMap<>();
        eventData2.put("order_id", 12345); // numeric

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", "USER_STR_1"); // string PK
        queryResult.add(row1);
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 67890); // numeric PK
        queryResult.add(row2);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", Arrays.asList(eventData1, eventData2));

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains("USER_STR_1"));
        assertTrue(result.contains(67890));
    }

    // 新增：任务 4 - 批量处理边界（999/1000/1001）
    @Test
    void testCalculateAffectedKeys_BatchBoundary_999() throws SQLException {
        testBatchBoundary(999);
    }

    @Test
    void testCalculateAffectedKeys_BatchBoundary_1000() throws SQLException {
        testBatchBoundary(1000);
    }

    @Test
    void testCalculateAffectedKeys_BatchBoundary_1001() throws SQLException {
        testBatchBoundary(1001);
    }

    private void testBatchBoundary(int eventCount) throws SQLException {
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        // Create events
        List<Map<String, Object>> events = new ArrayList<>();
        Set<Object> expectedPks = new LinkedHashSet<>();
        for (int i = 0; i < eventCount; i++) {
            Map<String, Object> eventData = new HashMap<>();
            eventData.put("order_id", "ORD_" + i);
            events.add(eventData);
            expectedPks.add((long) (i % 100)); // cycle through 0-99 to create duplicates
        }

        // Mock query returns corresponding user IDs
        List<Map<String, Object>> queryResult = new ArrayList<>();
        for (Object pk : expectedPks) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", pk);
            queryResult.add(row);
        }

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        // Should have deduplicated results
        assertEquals(expectedPks.size(), result.size());
        for (Object expected : expectedPks) {
            assertTrue(result.contains(expected));
        }
    }

    // 新增：任务 5 - 空值/空字符串处理
    @Test
    void testCalculateAffectedKeys_NullAndEmptyValues() throws SQLException {
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        // Mix of valid and invalid events
        Map<String, Object> validEvent1 = new HashMap<>();
        validEvent1.put("order_id", "VALID_001");

        Map<String, Object> nullPkEvent = new HashMap<>();
        nullPkEvent.put("order_id", null);

        Map<String, Object> emptyStringEvent = new HashMap<>();
        emptyStringEvent.put("order_id", "");

        Map<String, Object> validEvent2 = new HashMap<>();
        validEvent2.put("order_id", "VALID_002");

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 999L);
        queryResult.add(row);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", Arrays.asList(validEvent1, nullPkEvent, emptyStringEvent, validEvent2));

        assertNotNull(result);
        // Only valid events should produce results (both valid events map to same user for this test)
        assertEquals(1, result.size());
        assertTrue(result.contains(999L));
    }

    // 新增：任务 6 - SQL 特殊字符处理
    @Test
    void testCalculateAffectedKeys_SqlSpecialCharacters() throws SQLException {
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("order_id", "ORD'001\\test"); // contains single quote and backslash

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 777L);
        queryResult.add(row);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(777L));
    }

    // 新增：任务 7 - 大小写敏感测试
    @Test
    void testCalculateAffectedKeys_CaseInsensitiveTableName() throws SQLException {
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders"); // lowercase
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("order_id", "ORD_123");

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 555L);
        queryResult.add(row);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        // Test with UPPERCASE table name
        Set<Object> result1 = calculator.calculateAffectedKeys("ORDERS", Collections.singletonList(eventData));

        assertNotNull(result1);
        assertEquals(1, result1.size());
        assertTrue(result1.contains(555L));

        // Also test with mixed case
        reset(mockDuckDbOperator);
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result2 = calculator.calculateAffectedKeys("OrDeRs", Collections.singletonList(eventData));

        assertNotNull(result2);
        assertEquals(1, result2.size());
        assertTrue(result2.contains(555L));
    }

    // 新增：任务 8 - 重复关联结果去重
    @Test
    void testCalculateAffectedKeys_DuplicateJoinResults() throws SQLException {
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        Map<String, Object> event1 = new HashMap<>();
        event1.put("order_id", "ORD_A");

        Map<String, Object> event2 = new HashMap<>();
        event2.put("order_id", "ORD_B");

        // Both orders map to the same user
        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 111L);
        queryResult.add(row);
        queryResult.add(row); // duplicate intentionally

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", Arrays.asList(event1, event2));

        assertNotNull(result);
        assertEquals(1, result.size()); // should be deduplicated
        assertTrue(result.contains(111L));
    }

    // 新增：任务 9 - 查询返回部分 null
    @Test
    void testCalculateAffectedKeys_PartialNullResults() throws SQLException {
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("order_id", "ORD_001");

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 123L);
        queryResult.add(row1);

        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", null); // null PK
        queryResult.add(row2);

        Map<String, Object> row3 = new HashMap<>();
        row3.put("id", 456L);
        queryResult.add(row3);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(2, result.size()); // null should be filtered
        assertTrue(result.contains(123L));
        assertTrue(result.contains(456L));
        assertFalse(result.contains(null));
    }

    // 新增：任务 10 - 同一子表多 Join Key 关联
    @Test
    void testCalculateAffectedKeys_MultipleJoinKeys_SingleQuery() throws SQLException {
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        // Single query with multiple join conditions (user_id OR customer_id)
        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id OR u.id = o.customer_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("order_id", "ORD_MULTI_001");
        eventData.put("user_id", 1001L);
        eventData.put("customer_id", 2002L);

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1001L);
        queryResult.add(row1);
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 2002L);
        queryResult.add(row2);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains(1001L));
        assertTrue(result.contains(2002L));
    }

    @Test
    void testCalculateAffectedKeys_MultipleJoinKeys_DifferentConfigs() throws SQLException {
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        // First config: join by user_id
        Map<String, String> customJoinQueries1 = new HashMap<>();
        customJoinQueries1.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator1 = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries1,
                mockDuckDbOperator
        );

        // Second config: join by customer_id
        Map<String, String> customJoinQueries2 = new HashMap<>();
        customJoinQueries2.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.customer_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator2 = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries2,
                mockDuckDbOperator
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("order_id", "ORD_001");

        List<Map<String, Object>> queryResult1 = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 3001L);
        queryResult1.add(row1);

        List<Map<String, Object>> queryResult2 = new ArrayList<>();
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 4002L);
        queryResult2.add(row2);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult1);

        // Calculator 1 should use first config
        Set<Object> result1 = calculator1.calculateAffectedKeys("orders", Collections.singletonList(eventData));
        assertNotNull(result1);
        assertEquals(1, result1.size());
        assertTrue(result1.contains(3001L));

        // Calculator 2 should use second config (independent)
        reset(mockDuckDbOperator);
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult2);

        Set<Object> result2 = calculator2.calculateAffectedKeys("orders", Collections.singletonList(eventData));
        assertNotNull(result2);
        assertEquals(1, result2.size());
        assertTrue(result2.contains(4002L));
    }

    // 新增：任务 11-25 - ABA 场景测试（15个测试）
    // Helper method to create event with specific operation type
    private Map<String, Object> createEvent(String pkField, Object pkValue, String operation) {
        Map<String, Object> event = new HashMap<>();
        // Handle different CDC event formats
        if ("DELETE".equals(operation)) {
            event.put("before", Collections.singletonMap(pkField, pkValue));
        } else if ("UPDATE".equals(operation)) {
            event.put("before", Collections.singletonMap(pkField, pkValue));
            event.put("after", Collections.singletonMap(pkField, pkValue));
        } else { // INSERT or default
            event.put(pkField, pkValue);
        }
        return event;
    }

    @Test
    void testABA_1_ContinuousDuplicate_InsertUpdateDelete() throws SQLException {
        // ABA-1: 连续重复 + 增改删
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        List<Map<String, Object>> events = Arrays.asList(
                createEvent("order_id", "ORD_1", "INSERT"),
                createEvent("order_id", "ORD_1", "UPDATE"),
                createEvent("order_id", "ORD_2", "INSERT"),
                createEvent("order_id", "ORD_2", "UPDATE"),
                createEvent("order_id", "ORD_3", "INSERT")
        );

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1L);
        queryResult.add(row1);
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 2L);
        queryResult.add(row2);
        Map<String, Object> row3 = new HashMap<>();
        row3.put("id", 3L);
        queryResult.add(row3);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(3, result.size()); // [1, 2, 3] - deduplicated
        // Verify insertion order preserved
        Iterator<Object> iterator = result.iterator();
        assertEquals(1L, iterator.next());
        assertEquals(2L, iterator.next());
        assertEquals(3L, iterator.next());
    }

    @Test
    void testABA_2_IntervalDuplicate_InsertInsertDelete() throws SQLException {
        // ABA-2: 间隔重复 + 增删改
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        List<Map<String, Object>> events = Arrays.asList(
                createEvent("order_id", "ORD_1", "INSERT"),
                createEvent("order_id", "ORD_2", "INSERT"),
                createEvent("order_id", "ORD_1", "DELETE"),
                createEvent("order_id", "ORD_3", "INSERT"),
                createEvent("order_id", "ORD_1", "UPDATE")
        );

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1L);
        queryResult.add(row1);
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 2L);
        queryResult.add(row2);
        Map<String, Object> row3 = new HashMap<>();
        row3.put("id", 3L);
        queryResult.add(row3);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(3, result.size());
        Iterator<Object> iterator = result.iterator();
        assertEquals(1L, iterator.next());
        assertEquals(2L, iterator.next());
        assertEquals(3L, iterator.next());
    }

    @Test
    void testABA_3_CrossBatchDuplicate() throws SQLException {
        // ABA-3: 跨批次重复
        // Create > 1000 events to trigger batching
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        List<Map<String, Object>> events = new ArrayList<>();
        // Batch 1: ORD_1, ORD_2, ORD_3
        for (int i = 1; i <= 1000; i++) {
            events.add(createEvent("order_id", "ORD_" + (i % 3 + 1), "INSERT"));
        }
        // Batch 2: ORD_1, ORD_4, ORD_5
        for (int i = 1001; i <= 1100; i++) {
            events.add(createEvent("order_id", "ORD_" + ((i - 1000) % 3 + 4), "INSERT"));
        }

        List<Map<String, Object>> queryResult = new ArrayList<>();
        for (long id = 1; id <= 5; id++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", id);
            queryResult.add(row);
        }

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(5, result.size()); // [1, 2, 3, 4, 5] - cross-batch deduplication
    }

    @Test
    void testABA_4_ReverseOrderDuplicate() throws SQLException {
        // ABA-4: 顺序倒序重复
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        List<Map<String, Object>> events = Arrays.asList(
                createEvent("order_id", "ORD_3", "INSERT"),
                createEvent("order_id", "ORD_2", "INSERT"),
                createEvent("order_id", "ORD_1", "INSERT"),
                createEvent("order_id", "ORD_2", "UPDATE"),
                createEvent("order_id", "ORD_3", "UPDATE")
        );

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row3 = new HashMap<>();
        row3.put("id", 3L);
        queryResult.add(row3);
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 2L);
        queryResult.add(row2);
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1L);
        queryResult.add(row1);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(3, result.size());
        // Verify insertion order preserved (3, 2, 1)
        Iterator<Object> iterator = result.iterator();
        assertEquals(3L, iterator.next());
        assertEquals(2L, iterator.next());
        assertEquals(1L, iterator.next());
    }

    @Test
    void testABA_5_InsertDeleteInsert() throws SQLException {
        // ABA-5: 增-删-增
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        List<Map<String, Object>> events = Arrays.asList(
                createEvent("order_id", "ORD_1", "INSERT"),
                createEvent("order_id", "ORD_1", "DELETE"),
                createEvent("order_id", "ORD_1", "INSERT")
        );

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1L);
        queryResult.add(row);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(1, result.size()); // Only one unique key
        assertTrue(result.contains(1L));
    }

    @Test
    void testABA_6_InsertUpdateDelete() throws SQLException {
        // ABA-6: 增-改-删
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        List<Map<String, Object>> events = Arrays.asList(
                createEvent("order_id", "ORD_1", "INSERT"),
                createEvent("order_id", "ORD_1", "UPDATE"),
                createEvent("order_id", "ORD_1", "DELETE")
        );

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1L);
        queryResult.add(row);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(1L));
    }

    @Test
    void testABA_7_UpdateDeleteUpdate() throws SQLException {
        // ABA-7: 改-删-改
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        List<Map<String, Object>> events = Arrays.asList(
                createEvent("order_id", "ORD_1", "UPDATE"),
                createEvent("order_id", "ORD_1", "DELETE"),
                createEvent("order_id", "ORD_1", "UPDATE")
        );

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1L);
        queryResult.add(row);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(1L));
    }

    @Test
    void testABA_8_DeleteInsertDelete() throws SQLException {
        // ABA-8: 删-增-删
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        List<Map<String, Object>> events = Arrays.asList(
                createEvent("order_id", "ORD_1", "DELETE"),
                createEvent("order_id", "ORD_1", "INSERT"),
                createEvent("order_id", "ORD_1", "DELETE")
        );

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1L);
        queryResult.add(row);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(1L));
    }

    @Test
    void testABA_9_InsertUpdateUpdateDelete() throws SQLException {
        // ABA-9: 增-改-改-删
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        List<Map<String, Object>> events = Arrays.asList(
                createEvent("order_id", "ORD_1", "INSERT"),
                createEvent("order_id", "ORD_1", "UPDATE"),
                createEvent("order_id", "ORD_1", "UPDATE"),
                createEvent("order_id", "ORD_1", "DELETE")
        );

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1L);
        queryResult.add(row);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(1L));
    }

    @Test
    void testABA_10_JoinKeyUnchanged_PKDuplicate() throws SQLException {
        // ABA-10: Join key 不变，主键重复
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        // Join key (user_id) stays the same, but different order PKs relate back
        List<Map<String, Object>> events = Arrays.asList(
                createEvent("order_id", "ORD_1", "INSERT"),
                createEvent("order_id", "ORD_2", "INSERT"),
                createEvent("order_id", "ORD_1", "UPDATE")
        );

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1L);
        queryResult.add(row1);
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 2L);
        queryResult.add(row2);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains(1L));
        assertTrue(result.contains(2L));
    }

    @Test
    void testABA_11_JoinKeyChanges_DifferentPKs() throws SQLException {
        // ABA-11: Join key 变化，关联不同主键
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        List<Map<String, Object>> events = Arrays.asList(
                createEvent("order_id", "ORD_1", "INSERT"),
                createEvent("order_id", "ORD_2", "INSERT"),
                createEvent("order_id", "ORD_3", "INSERT")
        );

        // Each order maps to different user
        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 100L);
        queryResult.add(row1);
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 200L);
        queryResult.add(row2);
        Map<String, Object> row3 = new HashMap<>();
        row3.put("id", 300L);
        queryResult.add(row3);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(3, result.size());
        assertTrue(result.contains(100L));
        assertTrue(result.contains(200L));
        assertTrue(result.contains(300L));
    }

    @Test
    void testABA_12_JoinKeyFromNullToValue() throws SQLException {
        // ABA-12: Join key 从有到无再到有
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        List<Map<String, Object>> events = new ArrayList<>();

        // Valid event with join key
        Map<String, Object> event1 = new HashMap<>();
        event1.put("order_id", "ORD_1");
        events.add(event1);

        // Event with null join key in data
        Map<String, Object> event2 = new HashMap<>();
        event2.put("order_id", null);
        events.add(event2);

        // Another valid event
        Map<String, Object> event3 = new HashMap<>();
        event3.put("order_id", "ORD_1");
        events.add(event3);

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1L);
        queryResult.add(row);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(1, result.size()); // Null should be filtered out
        assertTrue(result.contains(1L));
    }

    @Test
    void testABA_13_JoinKeyAlternating() throws SQLException {
        // ABA-13: Join key 交替变化
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        List<Map<String, Object>> events = Arrays.asList(
                createEvent("order_id", "ORD_A", "INSERT"),
                createEvent("order_id", "ORD_B", "INSERT"),
                createEvent("order_id", "ORD_A", "UPDATE"),
                createEvent("order_id", "ORD_B", "UPDATE")
        );

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> rowA = new HashMap<>();
        rowA.put("id", 111L);
        queryResult.add(rowA);
        Map<String, Object> rowB = new HashMap<>();
        rowB.put("id", 222L);
        queryResult.add(rowB);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains(111L));
        assertTrue(result.contains(222L));
    }

    @Test
    void testABA_14_3Dimensional_Scenario1() throws SQLException {
        // ABA-14: 3维度综合场景1
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        // PK sequence: [1, 2, 1, 3, 2], ops: [I, I, U, I, D]
        List<Map<String, Object>> events = Arrays.asList(
                createEvent("order_id", "ORD_1", "INSERT"),
                createEvent("order_id", "ORD_2", "INSERT"),
                createEvent("order_id", "ORD_1", "UPDATE"),
                createEvent("order_id", "ORD_3", "INSERT"),
                createEvent("order_id", "ORD_2", "DELETE")
        );

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1L);
        queryResult.add(row1);
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 2L);
        queryResult.add(row2);
        Map<String, Object> row3 = new HashMap<>();
        row3.put("id", 3L);
        queryResult.add(row3);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(3, result.size());
        Iterator<Object> iterator = result.iterator();
        assertEquals(1L, iterator.next());
        assertEquals(2L, iterator.next());
        assertEquals(3L, iterator.next());
    }

    @Test
    void testABA_15_3Dimensional_Scenario2() throws SQLException {
        // ABA-15: 3维度综合场景2
        List<FromTableConfig> fromTables = new ArrayList<>();
        FromTableConfig fromTable = new FromTableConfig();
        fromTable.setPreNodeId("orders");
        fromTable.setTableNameInSql("order_id");
        fromTables.add(fromTable);

        Map<String, String> customJoinQueries = new HashMap<>();
        customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

        AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "id",
                "users",
                "id",
                fromTables,
                customJoinQueries,
                mockDuckDbOperator
        );

        // PK sequence: [5, 4, 5, 3, 4, 5], ops: [I, I, U, I, U, D]
        List<Map<String, Object>> events = Arrays.asList(
                createEvent("order_id", "ORD_5", "INSERT"),
                createEvent("order_id", "ORD_4", "INSERT"),
                createEvent("order_id", "ORD_5", "UPDATE"),
                createEvent("order_id", "ORD_3", "INSERT"),
                createEvent("order_id", "ORD_4", "UPDATE"),
                createEvent("order_id", "ORD_5", "DELETE")
        );

        List<Map<String, Object>> queryResult = new ArrayList<>();
        Map<String, Object> row5 = new HashMap<>();
        row5.put("id", 5L);
        queryResult.add(row5);
        Map<String, Object> row4 = new HashMap<>();
        row4.put("id", 4L);
        queryResult.add(row4);
        Map<String, Object> row3 = new HashMap<>();
        row3.put("id", 3L);
        queryResult.add(row3);

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        Set<Object> result = calculator.calculateAffectedKeys("orders", events);

        assertNotNull(result);
        assertEquals(3, result.size());
        Iterator<Object> iterator = result.iterator();
        assertEquals(5L, iterator.next());
        assertEquals(4L, iterator.next());
        assertEquals(3L, iterator.next());
    }

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
}

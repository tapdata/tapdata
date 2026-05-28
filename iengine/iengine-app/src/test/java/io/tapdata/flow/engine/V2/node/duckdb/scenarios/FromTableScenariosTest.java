package io.tapdata.flow.engine.V2.node.duckdb.scenarios;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.flow.engine.V2.node.duckdb.AffectedKeyCalculator;
import io.tapdata.flow.engine.V2.node.duckdb.AffectedKeyCalculatorTestBase;
import io.tapdata.flow.engine.V2.node.duckdb.FromTableConfig;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 子表JOIN场景测试
 * 覆盖：自定义查询、多表关联、非主键JOIN、链式JOIN、多JOIN键、异常处理等
 */
class FromTableScenariosTest extends AffectedKeyCalculatorTestBase {

    @Nested
    class OldModeTests {

        @Test
        void testFromTableWithCustomQuery() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            FromTableConfig fromTable = new FromTableConfig();
            fromTable.setTableName("orders");
            fromTable.setPrimaryKey("order_id");
            fromTables.add(fromTable);

            Map<String, String> customJoinQueries = new HashMap<>();
            customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator(fromTables, customJoinQueries);

            Map<String, Object> eventData = new HashMap<>();
            eventData.put("order_id", "ORD001");
            eventData.put("user_id", 1L);

            List<Map<String, Object>> queryResult = new ArrayList<>();
            Map<String, Object> row = new HashMap<>();
            row.put("id", 1L);
            queryResult.add(row);
            mockQueryReturns(queryResult);

            Set<Object> result = calculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));

            assertEquals(1, result.size());
            assertTrue(result.contains(1L));
        }

        @Test
        void testMultipleFromTables() throws SQLException {
            List<FromTableConfig> multiFromTables = new ArrayList<>();
            multiFromTables.add(new FromTableConfig("orders", "order_id"));
            multiFromTables.add(new FromTableConfig("payments", "payment_id"));

            Map<String, String> multiQueries = new HashMap<>();
            multiQueries.put("orders", "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");
            multiQueries.put("payments", "SELECT u.id FROM users u JOIN payments p ON u.id = p.user_id WHERE p.payment_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator("id", "users", "id", multiFromTables, multiQueries);

            Map<String, Object> eventData = new HashMap<>();
            eventData.put("payment_id", "PAY001");

            assertDoesNotThrow(() -> calculator.calculateAffectedKeys("payments", Collections.singletonList(eventData)));
        }

        @Test
        void testNonPrimaryKeyJoin() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("user_profiles", "profile_id"));

            Map<String, String> customJoinQueries = new HashMap<>();
            customJoinQueries.put("user_profiles", "SELECT DISTINCT u.id FROM users u INNER JOIN user_profiles p ON u.email = p.email WHERE p.profile_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator("id", "users", "id", fromTables, customJoinQueries);

            Map<String, Object> eventData = new HashMap<>();
            eventData.put("profile_id", "PROF001");

            List<Map<String, Object>> queryResult = Collections.singletonList(Map.of("id", 100L));
            mockQueryReturns(queryResult);

            Set<Object> result = calculator.calculateAffectedKeys("user_profiles", Collections.singletonList(eventData));

            assertEquals(1, result.size());
            assertTrue(result.contains(100L));
        }

        @Test
        void testMultiTableChainedJoin() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));
            fromTables.add(new FromTableConfig("order_items", "item_id"));

            Map<String, String> customJoinQueries = new HashMap<>();
            customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");
            customJoinQueries.put("order_items", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id INNER JOIN order_items i ON o.order_id = i.order_id WHERE i.item_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator("id", "users", "id", fromTables, customJoinQueries);

            Map<String, Object> eventData = new HashMap<>();
            eventData.put("item_id", "ITEM001");

            List<Map<String, Object>> queryResult = Collections.singletonList(Map.of("id", 200L));
            mockQueryReturns(queryResult);

            Set<Object> result = calculator.calculateAffectedKeys("order_items", Collections.singletonList(eventData));

            assertEquals(1, result.size());
            assertTrue(result.contains(200L));
        }

        @Test
        void testMultipleJoinKeys_SingleQuery() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));

            Map<String, String> customJoinQueries = new HashMap<>();
            customJoinQueries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id OR u.id = o.customer_id WHERE o.order_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator("id", "users", "id", fromTables, customJoinQueries);

            Map<String, Object> eventData = new HashMap<>();
            eventData.put("order_id", "ORD_MULTI_001");

            List<Map<String, Object>> queryResult = Arrays.asList(Map.of("id", 1001L), Map.of("id", 2002L));
            mockQueryReturns(queryResult);

            Set<Object> result = calculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));

            assertEquals(2, result.size());
            assertTrue(result.contains(1001L));
            assertTrue(result.contains(2002L));
        }

        @Test
        void testMultipleJoinKeys_DifferentConfigs() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));

            Map<String, String> queries1 = new HashMap<>();
            queries1.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");
            AffectedKeyCalculator calculator1 = createOldModeCalculator("id", "users", "id", fromTables, queries1);

            Map<String, String> queries2 = new HashMap<>();
            queries2.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.customer_id WHERE o.order_id IN (${pkValues})");
            AffectedKeyCalculator calculator2 = createOldModeCalculator("id", "users", "id", fromTables, queries2);

            Map<String, Object> eventData = new HashMap<>();
            eventData.put("order_id", "ORD_001");

            mockQueryReturns(Collections.singletonList(Map.of("id", 3001L)));
            Set<Object> result1 = calculator1.calculateAffectedKeys("orders", Collections.singletonList(eventData));
            assertEquals(1, result1.size());
            assertTrue(result1.contains(3001L));

            reset(mockDuckDbOperator);
            mockQueryReturns(Collections.singletonList(Map.of("id", 4002L)));
            Set<Object> result2 = calculator2.calculateAffectedKeys("orders", Collections.singletonList(eventData));
            assertEquals(1, result2.size());
            assertTrue(result2.contains(4002L));
        }

        @Test
        void testFromTableWithEmptyResult() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));
            Map<String, String> queries = new HashMap<>();
            queries.put("orders", "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator(fromTables, queries);

            Map<String, Object> eventData = new HashMap<>();
            eventData.put("order_id", "ORD002");

            mockQueryReturns(new ArrayList<>());

            Set<Object> result = calculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));

            assertTrue(result.isEmpty());
        }

        @Test
        void testFromTableQueryFails() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));
            Map<String, String> queries = new HashMap<>();
            queries.put("orders", "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator(fromTables, queries);

            Map<String, Object> eventData = new HashMap<>();
            eventData.put("order_id", "ORD003");

            mockQueryThrows(new SQLException("Query failed"));

            assertThrows(SQLException.class, () -> {
                calculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));
            });
        }

        @Test
        void testDuplicateJoinResults() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));
            Map<String, String> queries = new HashMap<>();
            queries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator(fromTables, queries);

            Map<String, Object> event1 = new HashMap<>();
            event1.put("order_id", "ORD_A");
            Map<String, Object> event2 = new HashMap<>();
            event2.put("order_id", "ORD_B");

            List<Map<String, Object>> queryResult = new ArrayList<>();
            queryResult.add(Map.of("id", 111L));
            queryResult.add(Map.of("id", 111L)); // duplicate
            mockQueryReturns(queryResult);

            Set<Object> result = calculator.calculateAffectedKeys("orders", Arrays.asList(event1, event2));

            assertEquals(1, result.size());
            assertTrue(result.contains(111L));
        }

        @Test
        void testPartialNullResults() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));
            Map<String, String> queries = new HashMap<>();
            queries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator(fromTables, queries);

            Map<String, Object> eventData = new HashMap<>();
            eventData.put("order_id", "ORD_001");

            List<Map<String, Object>> queryResult = new ArrayList<>();
            Map<String, Object> row1 = new HashMap<>();
            row1.put("id", 123L);
            queryResult.add(row1);
            Map<String, Object> row2 = new HashMap<>();
            row2.put("id", null);
            queryResult.add(row2);
            Map<String, Object> row3 = new HashMap<>();
            row3.put("id", 456L);
            queryResult.add(row3);
            mockQueryReturns(queryResult);

            Set<Object> result = calculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));

            assertEquals(2, result.size());
            assertTrue(result.contains(123L));
            assertTrue(result.contains(456L));
            assertFalse(result.contains(null));
        }

        @Test
        void testMixedPrimaryKeyTypes() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));
            Map<String, String> queries = new HashMap<>();
            queries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator(fromTables, queries);

            Map<String, Object> eventData1 = new HashMap<>();
            eventData1.put("order_id", "ORD_STR_1");
            Map<String, Object> eventData2 = new HashMap<>();
            eventData2.put("order_id", 12345);

            List<Map<String, Object>> queryResult = Arrays.asList(
                    Map.of("id", "USER_STR_1"),
                    Map.of("id", 67890)
            );
            mockQueryReturns(queryResult);

            Set<Object> result = calculator.calculateAffectedKeys("orders", Arrays.asList(eventData1, eventData2));

            assertEquals(2, result.size());
            assertTrue(result.contains("USER_STR_1"));
            assertTrue(result.contains(67890));
        }

        @Test
        void testNullCustomQueries() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));

            AffectedKeyCalculator calculator = createOldModeCalculator(fromTables, null);

            Map<String, Object> eventData = new HashMap<>();
            eventData.put("order_id", "ORD005");

            assertThrows(SQLException.class, () -> {
                calculator.calculateAffectedKeys("orders", Collections.singletonList(eventData));
            });
        }
    }

    @Nested
    class NewModeTests {

        private AffectedKeyCalculator createNewModeCalculatorWithFromTable(String tableName, String pkField, String querySql, List<String> fields) {
            List<FromTableConfig> fromTables = Collections.singletonList(
                    new FromTableConfig(tableName, pkField, querySql, fields)
            );
            return createNewModeCalculator("id", "users", "id", fromTables);
        }

        @Test
        void testFromTableWithCteSql() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithFromTable(
                    "orders", "id",
                    "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id",
                    Arrays.asList("id", "user_id")
            );

            List<Map<String, Object>> smartMergerEvents = new ArrayList<>();
            Map<String, Object> insert = new HashMap<>();
            insert.put("op", "INSERT");
            insert.put("id", 1L);
            insert.put("user_id", 1L);
            smartMergerEvents.add(insert);

            List<TapdataEvent> events = createTapdataEvents("orders", smartMergerEvents);
            mockQueryReturns(Collections.singletonList(Map.of("id", 1L)));

            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(events);

            assertEquals(1, afterKeys.size());
            assertTrue(afterKeys.contains(1L));
        }

        @Test
        void testMultipleFromTables() throws SQLException {
            List<FromTableConfig> fromTables = Arrays.asList(
                    new FromTableConfig("orders", "order_id", "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id", Arrays.asList("order_id")),
                    new FromTableConfig("payments", "payment_id", "SELECT u.id FROM users u JOIN payments p ON u.id = p.user_id", Arrays.asList("payment_id"))
            );
            AffectedKeyCalculator calculator = createNewModeCalculator("id", "users", "id", fromTables);

            List<Map<String, Object>> smartMergerEvents = createSmartMergerInsertEvents("payment_id", "PAY001");
            List<TapdataEvent> events = createTapdataEvents("payments", smartMergerEvents);
            mockQueryReturns(Collections.singletonList(Map.of("id", 1L)));

            assertDoesNotThrow(() -> calculator.calculateAffectedAfterKeys(events));
        }

        @Test
        void testNonPrimaryKeyJoin() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithFromTable(
                    "user_profiles", "id",
                    "SELECT u.id FROM users u JOIN user_profiles p ON u.email = p.email",
                    Arrays.asList("id", "email")
            );

            List<Map<String, Object>> smartMergerEvents = new ArrayList<>();
            Map<String, Object> insert = new HashMap<>();
            insert.put("op", "INSERT");
            insert.put("id", 100L);
            insert.put("email", "test@example.com");
            smartMergerEvents.add(insert);

            List<TapdataEvent> events = createTapdataEvents("user_profiles", smartMergerEvents);
            mockQueryReturns(Collections.singletonList(Map.of("id", 100L)));

            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(events);
            assertEquals(1, afterKeys.size());
            assertTrue(afterKeys.contains(100L));
        }

        @Test
        void testFromTableWithEmptyResult() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithFromTable(
                    "orders", "id",
                    "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id",
                    Arrays.asList("id")
            );

            List<Map<String, Object>> smartMergerEvents = createSmartMergerInsertEvents("id", 2L);
            List<TapdataEvent> events = createTapdataEvents("orders", smartMergerEvents);
            mockQueryReturns(new ArrayList<>());

            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(events);
            assertTrue(afterKeys.isEmpty());
        }

        @Test
        void testFromTableQueryFails() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithFromTable(
                    "orders", "id",
                    "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id",
                    Arrays.asList("id")
            );

            List<Map<String, Object>> smartMergerEvents = createSmartMergerInsertEvents("id", 3L);
            List<TapdataEvent> events = createTapdataEvents("orders", smartMergerEvents);
            mockQueryThrows(new SQLException("Query failed"));

            assertThrows(SQLException.class, () -> calculator.calculateAffectedAfterKeys(events));
        }

        @Test
        void testDuplicateJoinResults() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithFromTable(
                    "orders", "id",
                    "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id",
                    Arrays.asList("id")
            );

            List<Map<String, Object>> smartMergerEvents = createSmartMergerInsertEvents("id", 111L, 222L);
            List<TapdataEvent> events = createTapdataEvents("orders", smartMergerEvents);

            List<Map<String, Object>> queryResult = Arrays.asList(Map.of("id", 111L), Map.of("id", 111L));
            mockQueryReturns(queryResult);

            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(events);
            assertEquals(1, afterKeys.size());
            assertTrue(afterKeys.contains(111L));
        }

        @Test
        void testPartialNullResults() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithFromTable(
                    "orders", "id",
                    "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id",
                    Arrays.asList("id")
            );

            List<Map<String, Object>> smartMergerEvents = createSmartMergerInsertEvents("id", 123L);
            List<TapdataEvent> events = createTapdataEvents("orders", smartMergerEvents);

            List<Map<String, Object>> queryResult = new ArrayList<>();
            Map<String, Object> row1 = new HashMap<>();
            row1.put("id", 123L);
            queryResult.add(row1);
            Map<String, Object> row2 = new HashMap<>();
            row2.put("id", null);
            queryResult.add(row2);
            Map<String, Object> row3 = new HashMap<>();
            row3.put("id", 456L);
            queryResult.add(row3);
            mockQueryReturns(queryResult);

            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(events);
            assertEquals(2, afterKeys.size());
            assertTrue(afterKeys.contains(123L));
            assertTrue(afterKeys.contains(456L));
        }

        @Test
        void testMixedPrimaryKeyTypes() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithFromTable(
                    "orders", "id",
                    "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id",
                    Arrays.asList("id")
            );

            List<Map<String, Object>> smartMergerEvents = new ArrayList<>();
            Map<String, Object> e1 = new HashMap<>();
            e1.put("op", "INSERT");
            e1.put("id", "USER_STR_1");
            smartMergerEvents.add(e1);
            Map<String, Object> e2 = new HashMap<>();
            e2.put("op", "INSERT");
            e2.put("id", 67890);
            smartMergerEvents.add(e2);

            List<TapdataEvent> events = createTapdataEvents("orders", smartMergerEvents);

            List<Map<String, Object>> queryResult = Arrays.asList(
                    Map.of("id", "USER_STR_1"),
                    Map.of("id", 67890)
            );
            mockQueryReturns(queryResult);

            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(events);
            assertEquals(2, afterKeys.size());
            assertTrue(afterKeys.contains("USER_STR_1"));
            assertTrue(afterKeys.contains(67890));
        }
    }
}

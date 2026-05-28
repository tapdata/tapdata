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

/**
 * ABA问题场景测试
 * 覆盖：连续重复、间隔重复、跨批次重复、逆序重复、增删改组合等15个场景
 */
class ABAProblemScenariosTest extends AffectedKeyCalculatorTestBase {

    @Nested
    class OldModeTests {

        @Test
        void testABA_1_ContinuousDuplicate_InsertUpdateDelete() throws SQLException {
            testAbaOldMode(Arrays.asList(
                    createEvent("order_id", "ORD_1", "INSERT"),
                    createEvent("order_id", "ORD_1", "UPDATE"),
                    createEvent("order_id", "ORD_2", "INSERT"),
                    createEvent("order_id", "ORD_2", "UPDATE"),
                    createEvent("order_id", "ORD_3", "INSERT")
            ), Arrays.asList(1L, 2L, 3L));
        }

        @Test
        void testABA_2_IntervalDuplicate_InsertInsertDelete() throws SQLException {
            testAbaOldMode(Arrays.asList(
                    createEvent("order_id", "ORD_1", "INSERT"),
                    createEvent("order_id", "ORD_2", "INSERT"),
                    createEvent("order_id", "ORD_1", "DELETE"),
                    createEvent("order_id", "ORD_3", "INSERT"),
                    createEvent("order_id", "ORD_1", "UPDATE")
            ), Arrays.asList(1L, 2L, 3L));
        }

        @Test
        void testABA_3_CrossBatchDuplicate() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));
            Map<String, String> queries = new HashMap<>();
            queries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator(fromTables, queries);

            List<Map<String, Object>> events = new ArrayList<>();
            for (int i = 1; i <= 1000; i++) {
                events.add(createEvent("order_id", "ORD_" + (i % 3 + 1), "INSERT"));
            }
            for (int i = 1001; i <= 1100; i++) {
                events.add(createEvent("order_id", "ORD_" + ((i - 1000) % 3 + 4), "INSERT"));
            }

            List<Map<String, Object>> queryResult = new ArrayList<>();
            for (long id = 1; id <= 5; id++) {
                queryResult.add(Map.of("id", id));
            }
            mockQueryReturns(queryResult);

            Set<Object> result = calculator.calculateAffectedKeys("orders", events);

            assertEquals(5, result.size());
        }

        @Test
        void testABA_4_ReverseOrderDuplicate() throws SQLException {
            testAbaOldMode(Arrays.asList(
                    createEvent("order_id", "ORD_3", "INSERT"),
                    createEvent("order_id", "ORD_2", "INSERT"),
                    createEvent("order_id", "ORD_1", "INSERT"),
                    createEvent("order_id", "ORD_2", "UPDATE"),
                    createEvent("order_id", "ORD_3", "UPDATE")
            ), Arrays.asList(3L, 2L, 1L));
        }

        @Test
        void testABA_5_InsertDeleteInsert() throws SQLException {
            testAbaOldMode(Arrays.asList(
                    createEvent("order_id", "ORD_1", "INSERT"),
                    createEvent("order_id", "ORD_1", "DELETE"),
                    createEvent("order_id", "ORD_1", "INSERT")
            ), Arrays.asList(1L));
        }

        @Test
        void testABA_6_InsertUpdateDelete() throws SQLException {
            testAbaOldMode(Arrays.asList(
                    createEvent("order_id", "ORD_1", "INSERT"),
                    createEvent("order_id", "ORD_1", "UPDATE"),
                    createEvent("order_id", "ORD_1", "DELETE")
            ), Arrays.asList(1L));
        }

        @Test
        void testABA_7_UpdateDeleteUpdate() throws SQLException {
            testAbaOldMode(Arrays.asList(
                    createEvent("order_id", "ORD_1", "UPDATE"),
                    createEvent("order_id", "ORD_1", "DELETE"),
                    createEvent("order_id", "ORD_1", "UPDATE")
            ), Arrays.asList(1L));
        }

        @Test
        void testABA_8_DeleteInsertDelete() throws SQLException {
            testAbaOldMode(Arrays.asList(
                    createEvent("order_id", "ORD_1", "DELETE"),
                    createEvent("order_id", "ORD_1", "INSERT"),
                    createEvent("order_id", "ORD_1", "DELETE")
            ), Arrays.asList(1L));
        }

        @Test
        void testABA_9_InsertUpdateUpdateDelete() throws SQLException {
            testAbaOldMode(Arrays.asList(
                    createEvent("order_id", "ORD_1", "INSERT"),
                    createEvent("order_id", "ORD_1", "UPDATE"),
                    createEvent("order_id", "ORD_1", "UPDATE"),
                    createEvent("order_id", "ORD_1", "DELETE")
            ), Arrays.asList(1L));
        }

        @Test
        void testABA_10_JoinKeyUnchanged_PKDuplicate() throws SQLException {
            testAbaOldMode(Arrays.asList(
                    createEvent("order_id", "ORD_1", "INSERT"),
                    createEvent("order_id", "ORD_1", "UPDATE"),
                    createEvent("order_id", "ORD_1", "UPDATE")
            ), Arrays.asList(1L));
        }

        @Test
        void testABA_11_JoinKeyChanges_DifferentPKs() throws SQLException {
            testAbaOldMode(Arrays.asList(
                    createEvent("order_id", "ORD_1", "INSERT"),
                    createEvent("order_id", "ORD_2", "INSERT"),
                    createEvent("order_id", "ORD_3", "INSERT")
            ), Arrays.asList(1L, 2L, 3L));
        }

        @Test
        void testABA_12_JoinKeyFromNullToValue() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));
            Map<String, String> queries = new HashMap<>();
            queries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator(fromTables, queries);

            Map<String, Object> event1 = new HashMap<>();
            event1.put("order_id", "ORD_NULL");
            Map<String, Object> event2 = new HashMap<>();
            event2.put("order_id", "ORD_VALID");

            mockQueryReturns(Collections.singletonList(Map.of("id", 1L)));

            Set<Object> result = calculator.calculateAffectedKeys("orders", Arrays.asList(event1, event2));

            assertEquals(1, result.size());
            assertTrue(result.contains(1L));
        }

        @Test
        void testABA_13_JoinKeyAlternating() throws SQLException {
            testAbaOldMode(Arrays.asList(
                    createEvent("order_id", "ORD_A", "INSERT"),
                    createEvent("order_id", "ORD_B", "INSERT"),
                    createEvent("order_id", "ORD_A", "UPDATE"),
                    createEvent("order_id", "ORD_B", "UPDATE"),
                    createEvent("order_id", "ORD_A", "UPDATE")
            ), Arrays.asList(1L, 2L));
        }

        @Test
        void testABA_14_3Dimensional_Scenario1() throws SQLException {
            testAbaOldMode(Arrays.asList(
                    createEvent("order_id", "ORD_X", "INSERT"),
                    createEvent("order_id", "ORD_Y", "INSERT"),
                    createEvent("order_id", "ORD_Z", "INSERT"),
                    createEvent("order_id", "ORD_X", "UPDATE"),
                    createEvent("order_id", "ORD_Y", "DELETE")
            ), Arrays.asList(1L, 2L, 3L));
        }

        @Test
        void testABA_15_3Dimensional_Scenario2() throws SQLException {
            testAbaOldMode(Arrays.asList(
                    createEvent("order_id", "ORD_A", "INSERT"),
                    createEvent("order_id", "ORD_B", "INSERT"),
                    createEvent("order_id", "ORD_C", "INSERT"),
                    createEvent("order_id", "ORD_A", "DELETE"),
                    createEvent("order_id", "ORD_B", "UPDATE"),
                    createEvent("order_id", "ORD_C", "UPDATE")
            ), Arrays.asList(1L, 2L, 3L));
        }

        private void testAbaOldMode(List<Map<String, Object>> events, List<Long> expectedPks) throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));
            Map<String, String> queries = new HashMap<>();
            queries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator(fromTables, queries);

            List<Map<String, Object>> queryResult = new ArrayList<>();
            for (Long pk : expectedPks) {
                queryResult.add(Map.of("id", pk));
            }
            mockQueryReturns(queryResult);

            Set<Object> result = calculator.calculateAffectedKeys("orders", events);

            assertNotNull(result);
            assertEquals(expectedPks.size(), result.size());
            Iterator<Object> iterator = result.iterator();
            for (Long expected : expectedPks) {
                assertEquals(expected, iterator.next());
            }
        }
    }

    @Nested
    class NewModeTests {

        private AffectedKeyCalculator createNewModeCalculatorWithFromTable() {
            List<FromTableConfig> fromTables = Collections.singletonList(
                    new FromTableConfig("orders", "id",
                            "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id",
                            Arrays.asList("id"))
            );
            return createNewModeCalculator("id", "users", "id", fromTables);
        }

        @Test
        void testABA_1_ContinuousDuplicate() throws SQLException {
            testAbaNewMode(Arrays.asList(
                    Map.of("op", "INSERT", "id", 1),
                    Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("amount", 100)),
                    Map.of("op", "INSERT", "id", 2),
                    Map.of("op", "UPDATE", "o2", Map.of("id", 2), "updatedFields", Map.of("amount", 200)),
                    Map.of("op", "INSERT", "id", 3)
            ), Arrays.asList(1L, 2L, 3L));
        }

        @Test
        void testABA_5_InsertDeleteInsert() throws SQLException {
            testAbaNewMode(Arrays.asList(
                    Map.of("op", "INSERT", "id", 1),
                    Map.of("op", "DELETE", "o", Map.of("id", 1)),
                    Map.of("op", "INSERT", "id", 1)
            ), Arrays.asList(1L));
        }

        @Test
        void testABA_6_InsertUpdateDelete() throws SQLException {
            testAbaNewMode(Arrays.asList(
                    Map.of("op", "INSERT", "id", 1),
                    Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("amount", 100)),
                    Map.of("op", "DELETE", "o", Map.of("id", 1))
            ), Arrays.asList(1L));
        }

        @Test
        void testABA_9_InsertUpdateUpdateDelete() throws SQLException {
            testAbaNewMode(Arrays.asList(
                    Map.of("op", "INSERT", "id", 1),
                    Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("amount", 100)),
                    Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("amount", 200)),
                    Map.of("op", "DELETE", "o", Map.of("id", 1))
            ), Arrays.asList(1L));
        }

        @Test
        void testABA_10_JoinKeyUnchanged() throws SQLException {
            testAbaNewMode(Arrays.asList(
                    Map.of("op", "INSERT", "id", 1),
                    Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("amount", 100)),
                    Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("amount", 200))
            ), Arrays.asList(1L));
        }

        @Test
        void testABA_11_JoinKeyChanges_DifferentPKs() throws SQLException {
            testAbaNewMode(Arrays.asList(
                    Map.of("op", "INSERT", "id", 1),
                    Map.of("op", "INSERT", "id", 2),
                    Map.of("op", "INSERT", "id", 3)
            ), Arrays.asList(1L, 2L, 3L));
        }

        private void testAbaNewMode(List<Map<String, Object>> events, List<Long> expectedPks) throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithFromTable();

            List<TapdataEvent> tapdataEvents = createTapdataEvents("orders", events);

            List<Map<String, Object>> queryResult = new ArrayList<>();
            for (Long pk : expectedPks) {
                queryResult.add(Map.of("id", pk));
            }
            mockQueryReturns(queryResult);

            Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(tapdataEvents);
            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(tapdataEvents);

            Set<Object> allKeys = new LinkedHashSet<>();
            allKeys.addAll(beforeKeys);
            allKeys.addAll(afterKeys);

            assertEquals(expectedPks.size(), allKeys.size());
        }
    }
}

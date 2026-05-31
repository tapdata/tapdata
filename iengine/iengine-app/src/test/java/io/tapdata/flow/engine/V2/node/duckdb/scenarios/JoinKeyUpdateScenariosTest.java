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
 * JOIN KEY更新场景测试
 * 覆盖：JOIN KEY不变、JOIN KEY变化、JOIN KEY多次更新
 */
class JoinKeyUpdateScenariosTest extends AffectedKeyCalculatorTestBase {

    @Nested
    class OldModeTests {

        @Test
        void testJoinKeyUnchanged_PKDuplicate() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));
            Map<String, String> queries = new HashMap<>();
            queries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator(fromTables, queries);

            List<Map<String, Object>> events = Arrays.asList(
                    createEvent("order_id", "ORD_1", "INSERT"),
                    createEvent("order_id", "ORD_1", "UPDATE"),
                    createEvent("order_id", "ORD_1", "UPDATE")
            );

            mockQueryReturns(Collections.singletonList(Map.of("id", 1L)));

            Set<Object> result = calculator.calculateAffectedKeys("orders", events);

            assertEquals(1, result.size());
            assertTrue(result.contains(1L));
        }

        @Test
        void testJoinKeyChanges_DifferentPKs() throws SQLException {
            List<FromTableConfig> fromTables = new ArrayList<>();
            fromTables.add(new FromTableConfig("orders", "order_id"));
            Map<String, String> queries = new HashMap<>();
            queries.put("orders", "SELECT DISTINCT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.order_id IN (${pkValues})");

            AffectedKeyCalculator calculator = createOldModeCalculator(fromTables, queries);

            List<Map<String, Object>> events = Arrays.asList(
                    createEvent("order_id", "ORD_1", "INSERT"),
                    createEvent("order_id", "ORD_2", "INSERT"),
                    createEvent("order_id", "ORD_3", "INSERT")
            );

            List<Map<String, Object>> queryResult = Arrays.asList(
                    Map.of("id", 1L),
                    Map.of("id", 2L),
                    Map.of("id", 3L)
            );
            mockQueryReturns(queryResult);

            Set<Object> result = calculator.calculateAffectedKeys("orders", events);

            assertEquals(3, result.size());
            assertContainsKeys(result, 1L, 2L, 3L);
        }
    }

    @Nested
    class NewModeTests {

        private AffectedKeyCalculator createNewModeCalculatorWithFromTable() {
            List<FromTableConfig> fromTables = Collections.singletonList(
                    new FromTableConfig("orders", "order_id")),
                            "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id",
                            Arrays.asList("order_id", "user_id"))
            );
            return createNewModeCalculator("id", "users", "id", fromTables);
        }

        @Test
        void testJoinKeyUnchanged() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithFromTable();

            List<Map<String, Object>> smartMergerEvents = Arrays.asList(
                    Map.of("op", "INSERT", "id", 1, "user_id", 100),
                    Map.of("op", "UPDATE", "id", 1, "user_id", 100, "o2", Map.of("id", 1), "amount", 200)
            );
            List<TapdataEvent> events = createTapdataEvents("orders", smartMergerEvents);
            mockQueryReturns(Collections.singletonList(Map.of("id", 1L)));

            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(events);

            assertEquals(1, afterKeys.size());
            assertTrue(afterKeys.contains(1L));
        }

        @Test
        void testJoinKeyChanges() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithFromTable();

            List<Map<String, Object>> smartMergerEvents = Arrays.asList(
                    Map.of("op", "INSERT", "id", 1, "user_id", 100),
                    Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("id", 2)),
                    Map.of("op", "UPDATE", "o2", Map.of("id", 2), "updatedFields", Map.of("id", 3))
            );
            List<TapdataEvent> events = createTapdataEvents("orders", smartMergerEvents);

            List<Map<String, Object>> queryResult = Arrays.asList(
                    Map.of("id", 100L),
                    Map.of("id", 200L),
                    Map.of("id", 300L)
            );
            mockQueryReturns(queryResult);

            Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(events);
            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(events);

            Set<Object> allKeys = new LinkedHashSet<>();
            allKeys.addAll(beforeKeys);
            allKeys.addAll(afterKeys);

            assertEquals(3, allKeys.size());
            assertContainsKeys(allKeys, 100L, 200L, 300L);
        }

        @Test
        void testJoinKeyMultipleUpdates_beforeKeys() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithFromTable();

            List<Map<String, Object>> smartMergerEvents = Arrays.asList(
                    Map.of("op", "INSERT", "id", 1, "user_id", 100),
                    Map.of("op", "UPDATE", "o2", Map.of("id", 1), "updatedFields", Map.of("id", 2)),
                    Map.of("op", "UPDATE", "o2", Map.of("id", 2), "updatedFields", Map.of("id", 3)),
                    Map.of("op", "UPDATE", "o2", Map.of("id", 3), "updatedFields", Map.of("id", 4))
            );
            List<TapdataEvent> events = createTapdataEvents("orders", smartMergerEvents);

            List<Map<String, Object>> queryResult = Arrays.asList(
                    Map.of("id", 100L),
                    Map.of("id", 200L),
                    Map.of("id", 300L)
            );
            mockQueryReturns(queryResult);

            Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(events);

            assertEquals(3, beforeKeys.size());
            assertContainsKeys(beforeKeys, 100L, 200L, 300L);
        }

        @Test
        void testJoinKeyMultipleUpdates_afterKeys() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithFromTable();

            List<Map<String, Object>> smartMergerEvents = Arrays.asList(
                    Map.of("op", "INSERT", "id", 1, "user_id", 100),
                    Map.of("op", "UPDATE", "id", 4, "user_id", 400, "o2", Map.of("id", 3))
            );
            List<TapdataEvent> events = createTapdataEvents("orders", smartMergerEvents);

            mockQueryReturns(Collections.singletonList(Map.of("id", 400L)));

            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(events);

            assertEquals(1, afterKeys.size());
            assertTrue(afterKeys.contains(400L));
        }
    }
}

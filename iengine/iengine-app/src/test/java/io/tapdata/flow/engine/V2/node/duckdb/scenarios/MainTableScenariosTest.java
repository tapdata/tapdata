package io.tapdata.flow.engine.V2.node.duckdb.scenarios;

import io.tapdata.flow.engine.V2.node.duckdb.AffectedKeyCalculator;
import io.tapdata.flow.engine.V2.node.duckdb.AffectedKeyCalculatorTestBase;
import io.tapdata.flow.engine.V2.node.duckdb.FromTableConfig;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 主表操作场景测试
 * 覆盖：INSERT、UPDATE、DELETE、after/before字段提取、主键类型验证
 */
class MainTableScenariosTest extends AffectedKeyCalculatorTestBase {

    @Nested
    class OldModeTests {

        @Test
        void testMainTableInsert() throws SQLException {
            AffectedKeyCalculator calculator = createOldModeCalculator(
                    Collections.emptyList(),
                    Collections.emptyMap()
            );

            Map<String, Object> eventData = createInsertEvent("id", 1L);

            Set<Object> result = calculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

            assertEquals(1, result.size());
            assertTrue(result.contains(1L));
        }

        @Test
        void testMainTableUpdate() throws SQLException {
            AffectedKeyCalculator calculator = createOldModeCalculator(
                    Collections.emptyList(),
                    Collections.emptyMap()
            );

            Map<String, Object> eventData = createUpdateEvent("id", 2L);

            Set<Object> result = calculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

            assertEquals(1, result.size());
            assertTrue(result.contains(2L));
        }

        @Test
        void testMainTableDelete() throws SQLException {
            AffectedKeyCalculator calculator = createOldModeCalculator(
                    Collections.emptyList(),
                    Collections.emptyMap()
            );

            Map<String, Object> eventData = createDeleteEvent("id", 3L);

            Set<Object> result = calculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

            assertEquals(1, result.size());
            assertTrue(result.contains(3L));
        }

        @Test
        void testWithAfterField() throws SQLException {
            AffectedKeyCalculator calculator = createOldModeCalculator(
                    Collections.emptyList(),
                    Collections.emptyMap()
            );

            Map<String, Object> eventData = createEventWithAfter("id", 5L);

            Set<Object> result = calculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

            assertEquals(1, result.size());
            assertTrue(result.contains(5L));
        }

        @Test
        void testWithBeforeField() throws SQLException {
            AffectedKeyCalculator calculator = createOldModeCalculator(
                    Collections.emptyList(),
                    Collections.emptyMap()
            );

            Map<String, Object> eventData = createEventWithBefore("id", 6L);

            Set<Object> result = calculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

            assertEquals(1, result.size());
            assertTrue(result.contains(6L));
        }

        @Test
        void testPrimaryKeyInteger() throws SQLException {
            AffectedKeyCalculator calculator = createOldModeCalculator(
                    Collections.emptyList(),
                    Collections.emptyMap()
            );

            Map<String, Object> eventData = createInsertEvent("id", 123);

            Set<Object> result = calculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

            assertEquals(1, result.size());
            assertTrue(result.contains(123));
        }

        @Test
        void testPrimaryKeyString() throws SQLException {
            AffectedKeyCalculator calculator = createOldModeCalculator(
                    Collections.emptyList(),
                    Collections.emptyMap()
            );

            Map<String, Object> eventData = createInsertEvent("id", "user_001");

            Set<Object> result = calculator.calculateAffectedKeys("users", Collections.singletonList(eventData));

            assertEquals(1, result.size());
            assertTrue(result.contains("user_001"));
        }
    }

    @Nested
    class NewModeTests {

        private AffectedKeyCalculator createNewModeCalculatorWithMainTableQuery() {
            List<FromTableConfig> fromTables = Collections.singletonList(
                    new FromTableConfig("users", "id", "SELECT id FROM users", Arrays.asList("id"))
            );
            return createNewModeCalculator("id", "users", "id", fromTables);
        }

        @Test
        void testInsert_returnsEmptyBeforeKeys_returnsAfterKeys() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithMainTableQuery();

            List<Map<String, Object>> events = createSmartMergerInsertEvents("id", 100L, 200L);
            Map<String, List<Map<String, Object>>> eventsByTable = Map.of("users", events);

            mockQueryReturns(createQueryResult(100L, 200L));

            Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);
            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);

            assertEmptyKeys(beforeKeys);
            assertEquals(2, afterKeys.size());
            assertContainsKeys(afterKeys, 100L, 200L);
        }

        @Test
        void testUpdate_returnsBeforeKeys_returnsAfterKeys() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithMainTableQuery();

            List<Map<String, Object>> events = createSmartMergerUpdateEvents("id", 123L, 456L);
            Map<String, List<Map<String, Object>>> eventsByTable = Map.of("users", events);

            mockQueryReturns(createQueryResult(456L));

            Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);
            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);

            assertFalse(beforeKeys.isEmpty());
            assertFalse(afterKeys.isEmpty());
        }

        @Test
        void testDelete_returnsBeforeKeys_returnsEmptyAfterKeys() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithMainTableQuery();

            List<Map<String, Object>> events = createSmartMergerDeleteEvents("id", 100L, 200L);
            Map<String, List<Map<String, Object>>> eventsByTable = Map.of("users", events);

            mockQueryReturns(createQueryResult(100L, 200L));

            Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);
            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);

            assertEquals(2, beforeKeys.size());
            assertContainsKeys(beforeKeys, 100L, 200L);
            assertEmptyKeys(afterKeys);
        }

        @Test
        void testAfterFieldExtraction() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithMainTableQuery();

            // 新模式使用 SmartMerger 格式：INSERT 事件
            Map<String, Object> event = new HashMap<>();
            event.put("op", "INSERT");
            event.put("id", 5L);
            Map<String, List<Map<String, Object>>> eventsByTable = Map.of("users", Collections.singletonList(event));

            mockQueryReturns(createQueryResult(5L));

            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);

            assertEquals(1, afterKeys.size());
            assertTrue(afterKeys.contains(5L));
        }

        @Test
        void testBeforeFieldExtraction() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithMainTableQuery();

            // 新模式使用 SmartMerger 格式：先 INSERT 再 DELETE，提取 before
            List<Map<String, Object>> events = new ArrayList<>();
            Map<String, Object> insert = new HashMap<>();
            insert.put("op", "INSERT");
            insert.put("id", 6L);
            events.add(insert);
            Map<String, Object> delete = new HashMap<>();
            delete.put("op", "DELETE");
            delete.put("o", Map.of("id", 6L));
            delete.put("o2", Map.of("id", 6L));
            events.add(delete);

            Map<String, List<Map<String, Object>>> eventsByTable = Map.of("users", events);

            mockQueryReturns(createQueryResult(6L));

            Set<Object> beforeKeys = calculator.calculateAffectedBeforeKeys(eventsByTable);

            assertEquals(1, beforeKeys.size());
            assertTrue(beforeKeys.contains(6L));
        }

        @Test
        void testPrimaryKeyInteger() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithMainTableQuery();

            List<Map<String, Object>> events = createSmartMergerInsertEvents("id", 123);
            Map<String, List<Map<String, Object>>> eventsByTable = Map.of("users", events);

            mockQueryReturns(createQueryResult(123));

            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);

            assertEquals(1, afterKeys.size());
            assertTrue(afterKeys.contains(123));
        }

        @Test
        void testPrimaryKeyString() throws SQLException {
            AffectedKeyCalculator calculator = createNewModeCalculatorWithMainTableQuery();

            List<Map<String, Object>> events = createSmartMergerInsertEvents("id", "user_001");
            Map<String, List<Map<String, Object>>> eventsByTable = Map.of("users", events);

            List<Map<String, Object>> queryResult = Arrays.asList(
                    new HashMap<String, Object>() {{ put("id", "user_001"); }}
            );
            mockQueryReturns(queryResult);

            Set<Object> afterKeys = calculator.calculateAffectedAfterKeys(eventsByTable);

            assertEquals(1, afterKeys.size());
            assertTrue(afterKeys.contains("user_001"));
        }
    }
}

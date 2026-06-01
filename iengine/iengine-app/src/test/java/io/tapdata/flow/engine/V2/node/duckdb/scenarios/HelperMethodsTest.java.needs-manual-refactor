package io.tapdata.flow.engine.V2.node.duckdb.scenarios;

import io.tapdata.flow.engine.V2.node.duckdb.AffectedKeyCalculator;
import io.tapdata.flow.engine.V2.node.duckdb.AffectedKeyCalculatorTestBase;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 辅助方法验证测试
 * 覆盖：extractBeforePrimaryKey、extractAfterPrimaryKey、isPrimaryKeyUpdated
 */
class HelperMethodsTest extends AffectedKeyCalculatorTestBase {

    // ==================== extractBeforePrimaryKey 测试 ====================

    @Test
    void testExtractBeforePrimaryKey_fromBeforeField() {
        AffectedKeyCalculator calculator = createOldModeCalculator(
                Collections.emptyList(),
                Collections.emptyMap()
        );

        Map<String, Object> eventData = createEventWithBefore("id", 10L);

        Optional<Object> result = calculator.extractBeforePrimaryKey(eventData, "id");

        assertTrue(result.isPresent());
        assertEquals(10L, result.get());
    }

    @Test
    void testExtractBeforePrimaryKey_fromTopLevel() {
        AffectedKeyCalculator calculator = createOldModeCalculator(
                Collections.emptyList(),
                Collections.emptyMap()
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("id", 20L);

        Optional<Object> result = calculator.extractBeforePrimaryKey(eventData, "id");

        assertTrue(result.isPresent());
        assertEquals(20L, result.get());
    }

    @Test
    void testExtractBeforePrimaryKey_fromMongoO2() {
        AffectedKeyCalculator calculator = createOldModeCalculator(
                Collections.emptyList(),
                Collections.emptyMap()
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("o2", Map.of("id", 30L));

        Optional<Object> result = calculator.extractBeforePrimaryKey(eventData, "id");

        assertTrue(result.isPresent());
        assertEquals(30L, result.get());
    }

    @Test
    void testExtractBeforePrimaryKey_fromMongoO() {
        AffectedKeyCalculator calculator = createOldModeCalculator(
                Collections.emptyList(),
                Collections.emptyMap()
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("o", Map.of("id", 40L));

        Optional<Object> result = calculator.extractBeforePrimaryKey(eventData, "id");

        assertTrue(result.isPresent());
        assertEquals(40L, result.get());
    }

    @Test
    void testExtractBeforePrimaryKey_empty() {
        AffectedKeyCalculator calculator = createOldModeCalculator(
                Collections.emptyList(),
                Collections.emptyMap()
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("name", "Alice");

        Optional<Object> result = calculator.extractBeforePrimaryKey(eventData, "id");

        assertFalse(result.isPresent());
    }

    // ==================== extractAfterPrimaryKey 测试 ====================

    @Test
    void testExtractAfterPrimaryKey_fromAfterField() {
        AffectedKeyCalculator calculator = createOldModeCalculator(
                Collections.emptyList(),
                Collections.emptyMap()
        );

        Map<String, Object> eventData = createEventWithAfter("id", 50L);

        Optional<Object> result = calculator.extractAfterPrimaryKey(eventData, "id");

        assertTrue(result.isPresent());
        assertEquals(50L, result.get());
    }

    @Test
    void testExtractAfterPrimaryKey_fromTopLevel() {
        AffectedKeyCalculator calculator = createOldModeCalculator(
                Collections.emptyList(),
                Collections.emptyMap()
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("id", 60L);

        Optional<Object> result = calculator.extractAfterPrimaryKey(eventData, "id");

        assertTrue(result.isPresent());
        assertEquals(60L, result.get());
    }

    @Test
    void testExtractAfterPrimaryKey_empty() {
        AffectedKeyCalculator calculator = createOldModeCalculator(
                Collections.emptyList(),
                Collections.emptyMap()
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("name", "Bob");

        Optional<Object> result = calculator.extractAfterPrimaryKey(eventData, "id");

        assertFalse(result.isPresent());
    }

    // ==================== isPrimaryKeyUpdated 测试 ====================

    @Test
    void testIsPrimaryKeyUpdated_true() {
        AffectedKeyCalculator calculator = createOldModeCalculator(
                Collections.emptyList(),
                Collections.emptyMap()
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("before", Map.of("id", 100L));
        eventData.put("after", Map.of("id", 200L));

        boolean result = calculator.isPrimaryKeyUpdated(eventData, "id");

        assertTrue(result);
    }

    @Test
    void testIsPrimaryKeyUpdated_false_sameKey() {
        AffectedKeyCalculator calculator = createOldModeCalculator(
                Collections.emptyList(),
                Collections.emptyMap()
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("before", Map.of("id", 100L));
        eventData.put("after", Map.of("id", 100L));

        boolean result = calculator.isPrimaryKeyUpdated(eventData, "id");

        assertFalse(result);
    }

    @Test
    void testIsPrimaryKeyUpdated_false_insertOnly() {
        AffectedKeyCalculator calculator = createOldModeCalculator(
                Collections.emptyList(),
                Collections.emptyMap()
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("id", 100L);

        boolean result = calculator.isPrimaryKeyUpdated(eventData, "id");

        assertFalse(result);
    }

    @Test
    void testIsPrimaryKeyUpdated_false_deleteOnly() {
        AffectedKeyCalculator calculator = createOldModeCalculator(
                Collections.emptyList(),
                Collections.emptyMap()
        );

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("before", Map.of("id", 100L));

        boolean result = calculator.isPrimaryKeyUpdated(eventData, "id");

        assertFalse(result);
    }
}

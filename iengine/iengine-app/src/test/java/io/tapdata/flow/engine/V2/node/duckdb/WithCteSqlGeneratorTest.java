package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WithCteSqlGeneratorTest {

    private WithCteSqlGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new WithCteSqlGenerator();
    }

    @Test
    void testBuildValuesClause_SingleRow() {
        Map<String, Object> rowData = new LinkedHashMap<>();
        rowData.put("id", 123);
        rowData.put("name", "John");
        rowData.put("status", 1);

        String result = generator.buildValuesClause(rowData, Arrays.asList("id", "name", "status"));

        assertEquals("VALUES (123, 'John', 1)", result);
    }

    @Test
    void testBuildValuesClause_WithNull() {
        Map<String, Object> rowData = new LinkedHashMap<>();
        rowData.put("id", 123);
        rowData.put("name", null);
        rowData.put("status", 1);

        String result = generator.buildValuesClause(rowData, Arrays.asList("id", "name", "status"));

        assertEquals("VALUES (123, NULL, 1)", result);
    }

    @Test
    void testBuildValuesClause_WithSingleQuote() {
        Map<String, Object> rowData = new LinkedHashMap<>();
        rowData.put("name", "O'Brien");

        String result = generator.buildValuesClause(rowData, Arrays.asList("name"));

        assertEquals("VALUES ('O''Brien')", result);
    }

    @Test
    void testBuildValuesClause_WithDoubleValue() {
        Map<String, Object> rowData = new LinkedHashMap<>();
        rowData.put("amount", 99.9);

        String result = generator.buildValuesClause(rowData, Arrays.asList("amount"));

        assertEquals("VALUES (99.9)", result);
    }

    @Test
    void testBuildValuesClause_WithBoolean() {
        Map<String, Object> rowData = new LinkedHashMap<>();
        rowData.put("active", true);
        rowData.put("deleted", false);

        String result = generator.buildValuesClause(rowData, Arrays.asList("active", "deleted"));

        assertEquals("VALUES (TRUE, FALSE)", result);
    }

    @Test
    void testGenerateSingle_WithCte() {
        String sqlTemplate = "SELECT u.id, o.order_id FROM users u INNER JOIN orders o ON u.id = o.user_id";
        Map<String, Object> rowData = new LinkedHashMap<>();
        rowData.put("id", 123);
        rowData.put("name", "John");
        rowData.put("status", 1);

        String result = generator.generateSingle(sqlTemplate, "users", rowData,
                Arrays.asList("id", "name", "status"));

        assertTrue(result.startsWith("WITH users AS (VALUES (123, 'John', 1)) AS t(id, name, status)"));
        assertTrue(result.contains("SELECT u.id, o.order_id FROM users u"));
    }

    @Test
    void testGenerateBatch_WithCte() {
        String sqlTemplate = "SELECT u.id FROM users u";
        List<Map<String, Object>> rows = new ArrayList<>();

        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("id", 1);
        row1.put("name", "Alice");
        rows.add(row1);

        Map<String, Object> row2 = new LinkedHashMap<>();
        row2.put("id", 2);
        row2.put("name", "Bob");
        rows.add(row2);

        String result = generator.generateBatch(sqlTemplate, "users", rows,
                Arrays.asList("id", "name"));

        assertTrue(result.contains("VALUES (1, 'Alice'), (2, 'Bob')"));
        assertTrue(result.contains("AS t(id, name)"));
    }

    @Test
    void testGenerateBatch_EmptyRowsThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
            generator.generateBatch("SELECT * FROM users", "users", new ArrayList<>(), Arrays.asList("id"))
        );
    }
}

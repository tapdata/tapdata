package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class WideTableBatchSqlBuilderTest {

    @Test
    void testBuildDeleteSql_SingleKey() {
        String sql = WideTableBatchSqlBuilder.buildDeleteSql(
                "wide_table", "id", Collections.singletonList(123));

        assertTrue(sql.contains("DELETE FROM wide_table"));
        assertTrue(sql.contains("WHERE id IN"));
        assertTrue(sql.contains("(VALUES (123))"));
    }

    @Test
    void testBuildDeleteSql_MultipleKeys() {
        List<Object> keys = Arrays.asList(123, 456, 789);
        String sql = WideTableBatchSqlBuilder.buildDeleteSql("wide_table", "id", keys);

        assertTrue(sql.contains("(VALUES (123), (456), (789))"));
    }

    @Test
    void testBuildDeleteSql_StringKeys() {
        List<Object> keys = Arrays.asList("abc", "def'ghi");
        String sql = WideTableBatchSqlBuilder.buildDeleteSql("wide_table", "id", keys);

        // 验证字符串转义
        assertTrue(sql.contains("'abc'"));
        assertTrue(sql.contains("'def''ghi'"));
    }

    @Test
    void testBuildDeleteSql_EmptyKeys_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                WideTableBatchSqlBuilder.buildDeleteSql("wide_table", "id", Collections.emptyList()));
    }

    @Test
    void testBuildInsertSql_SingleRow() {
        List<String> columns = Arrays.asList("id", "name", "email");
        List<Map<String, Object>> rows = Collections.singletonList(createRow(1, "John", "john@example.com"));

        String sql = WideTableBatchSqlBuilder.buildInsertSql("wide_table", columns, rows);

        assertTrue(sql.contains("INSERT INTO wide_table (id, name, email)"));
        assertTrue(sql.contains("VALUES (1, 'John', 'john@example.com')"));
    }

    @Test
    void testBuildInsertSql_MultipleRows() {
        List<String> columns = Arrays.asList("id", "name");
        List<Map<String, Object>> rows = Arrays.asList(
                createRow(1, "John"),
                createRow(2, "Jane")
        );

        String sql = WideTableBatchSqlBuilder.buildInsertSql("wide_table", columns, rows);

        assertTrue(sql.contains("VALUES (1, 'John'), (2, 'Jane')"));
    }

    @Test
    void testBuildInsertSql_NullValues() {
        List<String> columns = Arrays.asList("id", "name");
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1);
        row.put("name", null);

        String sql = WideTableBatchSqlBuilder.buildInsertSql("wide_table", columns, Collections.singletonList(row));

        assertTrue(sql.contains("(1, NULL)"));
    }

    @Test
    void testBuildInsertSql_EmptyRows_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                WideTableBatchSqlBuilder.buildInsertSql("wide_table", Arrays.asList("id"), Collections.emptyList()));
    }

    @Test
    void testFormatValue_StringWithQuotes() {
        assertEquals("'it''s'", WideTableBatchSqlBuilder.formatValue("it's"));
    }

    @Test
    void testFormatValue_Boolean() {
        assertEquals("TRUE", WideTableBatchSqlBuilder.formatValue(true));
        assertEquals("FALSE", WideTableBatchSqlBuilder.formatValue(false));
    }

    @Test
    void testFormatValue_Null() {
        assertEquals("NULL", WideTableBatchSqlBuilder.formatValue(null));
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> createRow(Object id, String name, String email) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", name);
        row.put("email", email);
        return row;
    }

    private Map<String, Object> createRow(Object id, String name) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", name);
        return row;
    }
}

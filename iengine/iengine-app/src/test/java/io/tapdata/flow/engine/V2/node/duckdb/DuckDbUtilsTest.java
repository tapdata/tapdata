package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.flow.engine.V2.node.duckdb.utils.SqlJoinConverter;
import io.tapdata.flow.engine.V2.node.duckdb.utils.SqlJoinUtil;
import io.tapdata.flow.engine.V2.node.duckdb.utils.TablePkUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbUtilsTest {
    @Test
    void tablePkUtils_pkValueAndExists() {
        List<String> pks = List.of("id", "tenant");

        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("id", 1L);
        row1.put("tenant", "t1");
        row1.put("name", "Alice");

        Map<String, Object> row2 = new LinkedHashMap<>();
        row2.put("id", 2L);
        row2.put("tenant", "t1");
        row2.put("name", "Bob");

        List<Map<String, Object>> rows = List.of(row1, row2);
        List<Map<String, Object>> pkValues = TablePkUtils.pkValues(rows, pks);
        assertEquals(2, pkValues.size());

        Map<String, Object> pk1 = TablePkUtils.pkValue(row1, pks);
        assertEquals(Map.of("id", 1L, "tenant", "t1"), pk1);

        assertTrue(TablePkUtils.pkExists(pkValues, Map.of("tenant", "t1", "id", 2L)));
        assertFalse(TablePkUtils.pkExists(pkValues, Map.of("tenant", "t1", "id", 3L)));
        assertFalse(TablePkUtils.pkExists(List.of(), pk1));
        assertFalse(TablePkUtils.pkExists(pkValues, Map.of()));
    }

    @Test
    void sqlJoinUtil_forceInnerJoin_rewritesJoinKeywordForTargetTableOnly() {
        String sql = "SELECT * FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON a.id=c.id";
        String rewritten = SqlJoinUtil.forceInnerJoin(sql, "b");
        assertTrue(rewritten.contains("FROM a INNER JOIN b"));
        assertTrue(rewritten.contains("LEFT JOIN c"));
    }

    @Test
    void sqlJoinConverter_convertJoinToInner_rewritesParsedJoinForTargetTableOnly() throws Exception {
        String sql = "SELECT a.id, b.v FROM a LEFT JOIN b ON a.id=b.id LEFT JOIN c ON a.id=c.id";
        String converted = SqlJoinConverter.convertJoinToInner(sql, "b");
        String upper = converted.toUpperCase();
        assertTrue(upper.contains("INNER JOIN B"));
        assertTrue(upper.contains("LEFT JOIN C"));
    }

    @Test
    void querySqlProcessor_normalize_trimsAndRemovesTrailingSemicolon_preservesNewlines() {
        String sql = "  SELECT   a,   b  \n  FROM  t  ;  ";
        String normalized = QuerySqlProcessor.normalize(sql);
        assertEquals("SELECT a, b\nFROM t", normalized);
    }

    @Test
    void querySqlProcessor_validate_rejectsNonSelectAndForbiddenKeywords() {
        QuerySqlProcessor.ValidationResult notSelect = QuerySqlProcessor.validate("DELETE FROM t", Map.of());
        assertFalse(notSelect.isValid);
        assertTrue(notSelect.errorMessage.contains("SELECT"));

        QuerySqlProcessor.ValidationResult forbidden = QuerySqlProcessor.validate("SELECT * FROM t; DROP TABLE x", Map.of());
        assertFalse(forbidden.isValid);
    }

    @Test
    void querySqlProcessor_validate_withSchemaCache_extractsSelectFields() {
        Map<String, NodeSchemaInfo> dummySchemaCache = Map.of("n1", buildNodeSchema("n1", "t", List.of("id")));
        QuerySqlProcessor.ValidationResult result = QuerySqlProcessor.validate("SELECT a AS x, b FROM t", dummySchemaCache);
        assertTrue(result.isValid);
        assertTrue(result.fieldNames.contains("x"));
        assertTrue(result.fieldNames.contains("b"));
    }

    @Test
    void wideTableDdlGenerator_extractSelectFields_andQuoteIdentifier_andInferType() {
        List<String> fields = WideTableDdlGenerator.extractSelectFields("SELECT a AS x, b, COUNT(*) AS cnt FROM t");
        assertTrue(fields.contains("x"));
        assertTrue(fields.contains("b"));
        assertTrue(fields.contains("cnt"));

        assertEquals("id", WideTableDdlGenerator.quoteIdentifier("id"));
        assertEquals("\"select\"", WideTableDdlGenerator.quoteIdentifier("select"));
        assertEquals("\"has-dash\"", WideTableDdlGenerator.quoteIdentifier("has-dash"));
        assertEquals("\"null\"", WideTableDdlGenerator.quoteIdentifier(null));

        assertEquals("VARCHAR", WideTableDdlGenerator.inferDuckDbType(String.class));
        assertEquals("BIGINT", WideTableDdlGenerator.inferDuckDbType(Long.class));
        assertEquals("DOUBLE", WideTableDdlGenerator.inferDuckDbType(Double.class));
        assertEquals("BOOLEAN", WideTableDdlGenerator.inferDuckDbType(Boolean.class));
        assertEquals("BLOB", WideTableDdlGenerator.inferDuckDbType(byte[].class));
        assertEquals("VARCHAR", WideTableDdlGenerator.inferDuckDbType(Object.class));
    }

    @Test
    void wideTableDdlGenerator_generateCreateTableDdl_preservesDecimalTypeFromSchema() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "BIGINT");
        fields.put("amount", "DECIMAL(10,2)");
        fields.put("plain_decimal", "DECIMAL");
        fields.put("numeric_amount", "numeric(18, 4)");
        fields.put("name", "VARCHAR(32)");

        String ddl = WideTableDdlGenerator.generateCreateTableDdl(buildNodeSchema("n_decimal", "wide_decimal", List.of("id"), fields));

        assertTrue(ddl.contains("id BIGINT"));
        assertTrue(ddl.contains("amount DECIMAL(10,2)"));
        assertTrue(ddl.contains("plain_decimal DECIMAL"));
        assertTrue(ddl.contains("numeric_amount DECIMAL(18,4)"));
        assertTrue(ddl.contains("name VARCHAR"));
        assertFalse(ddl.contains("amount VARCHAR"));
        assertFalse(ddl.contains("amount DOUBLE"));
    }

    @Test
    void duckDbSqlValueFormatter_format_and_formatForCsv() {
        assertEquals("NULL", DuckDbSqlValueFormatter.format(null));
        assertEquals("'a''b'", DuckDbSqlValueFormatter.format("a'b"));
        assertEquals("TRUE", DuckDbSqlValueFormatter.format(true));
        assertEquals("FALSE", DuckDbSqlValueFormatter.format(false));
        assertEquals("1", DuckDbSqlValueFormatter.format(1));
        assertEquals("1", DuckDbSqlValueFormatter.format(new BigDecimal("1.0000")));
        assertEquals("20", DuckDbSqlValueFormatter.format(new BigDecimal("2E+1")));
        assertEquals("'2026-01-02'", DuckDbSqlValueFormatter.format(LocalDate.of(2026, 1, 2)));
        assertEquals("'2026-01-02 03:04:05.006'", DuckDbSqlValueFormatter.format(LocalDateTime.of(2026, 1, 2, 3, 4, 5, 6_000_000)));
        assertEquals("'2026-01-02 03:04:05.006'", DuckDbSqlValueFormatter.format(Timestamp.valueOf(LocalDateTime.of(2026, 1, 2, 3, 4, 5, 6_000_000))));
        assertEquals("'\\x01\\x0a\\xff'", DuckDbSqlValueFormatter.format(new byte[]{1, 10, (byte) 255}));

        assertEquals("", DuckDbSqlValueFormatter.formatForCsv(null));
        assertEquals("abc", DuckDbSqlValueFormatter.formatForCsv("abc"));
        assertEquals("\"a,b\"", DuckDbSqlValueFormatter.formatForCsv("a,b"));
        assertEquals("\"a\"\"b\"", DuckDbSqlValueFormatter.formatForCsv("a\"b"));
    }

    @Test
    void wideTableBatchSqlBuilder_buildDeleteSql_formatsNumericStringsAccordingToTargetType() {
        List<Map<String, Object>> keys = List.of(
                Map.of("id", "123"),
                Map.of("id", "abc"),
                Map.of("id", 456L)
        );
        String sql = WideTableBatchSqlBuilder.buildDeleteSql("wide_table", keys, Map.of("id", Long.class));
        assertTrue(sql.contains("id = 123"));
        assertTrue(sql.contains("id = 'abc'"));
        assertTrue(sql.contains("id = 456"));
    }

    @Test
    void wideTableBatchSqlBuilder_buildInsertSql_formatsValuesInColumnOrder() {
        List<String> columns = List.of("id", "name");
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(Map.of("id", 1, "name", "a'b"));
        rows.add(Map.of("id", 2, "name", "c"));
        String sql = WideTableBatchSqlBuilder.buildInsertSql("t", columns, rows);
        assertTrue(sql.startsWith("INSERT INTO t (id, name) VALUES "));
        assertTrue(sql.contains("(1, 'a''b')"));
        assertTrue(sql.contains("(2, 'c')"));
    }

    @Test
    void withCteSqlGenerator_generateBatch_validatesInputAndBuildsSql() {
        WithCteSqlGenerator generator = new WithCteSqlGenerator();
        assertThrows(IllegalArgumentException.class, () -> generator.generateBatch("SELECT * FROM t", "t", List.of(), List.of("id")));

        String sqlTemplate = "SELECT t.id FROM other o LEFT JOIN t ON t.id=o.id";
        List<Map<String, Object>> rows = List.of(
                Map.of("id", 1L, "name", "Alice"),
                Map.of("id", 2L, "name", "Bob")
        );
        String sql = generator.generateBatch(sqlTemplate, "t", rows, List.of("id", "name"));
        String upper = sql.toUpperCase();
        assertTrue(upper.contains("WITH T (ID, NAME) AS"));
        assertTrue(upper.contains("VALUES"));
        assertTrue(upper.contains("INNER JOIN T"));
    }

    private static NodeSchemaInfo buildNodeSchema(String nodeId, String tableName, List<String> pks) {
        Map<String, io.tapdata.entity.schema.TapField> fieldMap = new LinkedHashMap<>();
        io.tapdata.entity.schema.TapField id = new io.tapdata.entity.schema.TapField();
        id.setName("id");
        id.setOriginalFieldName("id");
        id.setDataType("BIGINT");
        id.setPrimaryKey(true);
        fieldMap.put("id", id);

        io.tapdata.entity.schema.TapField a = new io.tapdata.entity.schema.TapField();
        a.setName("a");
        a.setOriginalFieldName("a");
        a.setDataType("VARCHAR");
        fieldMap.put("a", a);

        io.tapdata.entity.schema.TapField b = new io.tapdata.entity.schema.TapField();
        b.setName("b");
        b.setOriginalFieldName("b");
        b.setDataType("VARCHAR");
        fieldMap.put("b", b);

        return new NodeSchemaInfo(nodeId, tableName, tableName, pks, fieldMap, new io.tapdata.entity.schema.TapTable(), null);
    }

    private static NodeSchemaInfo buildNodeSchema(String nodeId, String tableName, List<String> pks, LinkedHashMap<String, String> fields) {
        Map<String, io.tapdata.entity.schema.TapField> fieldMap = new LinkedHashMap<>();
        List<org.apache.arrow.vector.types.pojo.Field> arrowFields = new ArrayList<>();
        int pos = 1;
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            io.tapdata.entity.schema.TapField tapField = new io.tapdata.entity.schema.TapField();
            tapField.setName(entry.getKey());
            tapField.setOriginalFieldName(entry.getKey());
            tapField.setDataType(entry.getValue());
            tapField.setPos(pos++);
            tapField.setPrimaryKey(pks != null && pks.contains(entry.getKey()));
            fieldMap.put(entry.getKey(), tapField);
            arrowFields.add(new org.apache.arrow.vector.types.pojo.Field(
                    entry.getKey(),
                    org.apache.arrow.vector.types.pojo.FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()),
                    null));
        }

        return new NodeSchemaInfo(
                nodeId,
                tableName,
                tableName,
                pks,
                fieldMap,
                new io.tapdata.entity.schema.TapTable(),
                new org.apache.arrow.vector.types.pojo.Schema(arrowFields));
    }
}

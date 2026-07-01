package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.duckdb.DuckDBAppender;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DuckDbOperatorImplAndArrowTest {
    @Test
    void duckDbQueryEngine_timeoutBehavior() {
        DuckDbQueryEngine engine = new DuckDbQueryEngine(100, 1000);
        assertThrows(Exception.class, () -> engine.executeQueryWithTimeout("SELECT 1", 10));
        assertDoesNotThrow(() -> engine.executeQueryWithTimeout("SELECT 1", 100));
    }

    @Test
    void sanitizeIdentifier_replacesInvalidCharsAndPrefixesDigit() {
        assertEquals("a_b", DuckDbOperatorImpl.sanitizeIdentifier("a-b"));
        assertEquals("_1x", DuckDbOperatorImpl.sanitizeIdentifier("1x"));
        assertEquals("a_b", DuckDbOperator.sanitizeIdentifier("a-b"));
        assertThrows(IllegalArgumentException.class, () -> DuckDbOperatorImpl.sanitizeIdentifier(" "));
    }

    @Test
    void duckDbOperatorImpl_executeQueryAndUpdate_inMemory() throws Exception {
        try (DuckDbOperatorImpl operator = new DuckDbOperatorImpl()) {
            operator.executeUpdate("CREATE TABLE t(id BIGINT, name VARCHAR)");
            operator.executeUpdate("INSERT INTO t VALUES (1, 'a'), (2, 'b')");

            List<Map<String, Object>> rows = operator.executeQuery("SELECT id, name FROM t ORDER BY id");
            assertEquals(2, rows.size());
            assertEquals(1L, ((Number) rows.get(0).get("id")).longValue());
            assertEquals("a", rows.get(0).get("name"));

            DuckDbOperator.ExecuteResult result = operator.execute("SELECT count(*) AS c FROM t");
            assertTrue(result.isHasResultSet());
            assertEquals(1, result.getResultSet().size());
            assertTrue(result.getResultSet().get(0).containsKey("c"));
        }
    }

    @Test
    void arrowValueHandler_setVectorValue_setsTypedValuesAndNulls() {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             BigIntVector v = new BigIntVector("id", allocator);
             VarCharVector s = new VarCharVector("name", allocator)) {
            v.allocateNew(2);
            s.allocateNew(2);

            ArrowValueHandler.setVectorValue(v, 0, 123L);
            ArrowValueHandler.setVectorValue(v, 1, null);
            v.setValueCount(2);

            ArrowValueHandler.setVectorValue(s, 0, "a");
            ArrowValueHandler.setVectorValue(s, 1, null);
            s.setValueCount(2);

            assertEquals(123L, v.get(0));
            assertTrue(v.isNull(1));
            assertEquals("a", s.getObject(0).toString());
            assertTrue(s.isNull(1));
        }
    }

    @Test
    void arrowValueHandler_appendToAppender_routesByType() throws Exception {
        DuckDBAppender appender = mock(DuckDBAppender.class);

        ArrowValueHandler.appendToAppender(appender, null);
        verify(appender).append((String) null);

        ArrowValueHandler.appendToAppender(appender, 1);
        verify(appender).append(1L);

        ArrowValueHandler.appendToAppender(appender, 1L);
        verify(appender).append(1L);

        ArrowValueHandler.appendToAppender(appender, "x");
        verify(appender).append("x");
    }

    @Test
    void arrowWriter_writeWithArrow_insertsRows() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE t(id BIGINT, name VARCHAR)");
            TapTable tapTable = new TapTable();
            LinkedHashMap<String, TapField> fields = new LinkedHashMap<>();

            TapField id = new TapField();
            id.setName("id");
            id.setOriginalFieldName("id");
            id.setDataType("BIGINT");
            id.setTapType(new TapNumber());
            id.setPrimaryKey(true);
            fields.put("id", id);

            TapField name = new TapField();
            name.setName("name");
            name.setOriginalFieldName("name");
            name.setDataType("VARCHAR");
            fields.put("name", name);

            tapTable.setNameFieldMap(fields);

            ArrowWriter writer = new ArrowWriter(connection, false, DuckLakeConfig.disabled());
            writer.writeWithArrow(
                    List.of(
                            Map.of("id", 1L, "name", "a"),
                            Map.of("id", 2L, "name", "b")
                    ),
                    "t",
                    tapTable
            );

            List<Map<String, Object>> rows;
            try (DuckDbOperatorImpl operator = new DuckDbOperatorImpl(connection, false, 1000, 5000, DuckLakeConfig.disabled())) {
                rows = operator.executeQuery("SELECT count(*) AS c FROM t");
            }
            assertEquals(1, rows.size());
            assertEquals(2L, ((Number) rows.get(0).get("c")).longValue());
        }
    }
}

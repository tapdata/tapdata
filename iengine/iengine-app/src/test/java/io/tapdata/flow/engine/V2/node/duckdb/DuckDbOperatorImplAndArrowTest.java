package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.observable.logging.ObsLogger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBAppender;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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
        try (DuckDbOperatorImpl operator = new DuckDbOperatorImpl(mock(ObsLogger.class))) {
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
    void ensureTableExists_andDeleteByIds_coverSingleAndCompositePrimaryKeys() throws Exception {
        try (DuckDbOperatorImpl operator = new DuckDbOperatorImpl(mock(ObsLogger.class))) {
            TapField singleIdField = field("id", new TapNumber(), null);
            TapField singleNameField = field("name", new TapString(), null);
            NodeSchemaInfo singlePkSchema = schema(
                    "node_single",
                    "single_pk_table",
                    List.of("id"),
                    singleIdField,
                    singleNameField
            );

            operator.ensureTableExists("single_pk_table", List.of(singleIdField, singleNameField), List.of("id"), false);
            operator.insert("single_pk_table", Map.of("id", 1L, "name", "a"));
            operator.ensureTableExists("single_pk_table", List.of(singleIdField, singleNameField), List.of("id"), false);
            assertEquals(1L, operator.getRowCount("single_pk_table"));

            int deleted = operator.deleteByIds(List.of(Map.of("id", "1")), singlePkSchema);
            assertEquals(1, deleted);
            assertEquals(0L, operator.getRowCount("single_pk_table"));

            operator.insert("single_pk_table", Map.of("id", 2L, "name", "b"));
            operator.ensureTableExists("single_pk_table", List.of(singleIdField, singleNameField), List.of("id"), true);
            assertEquals(0L, operator.getRowCount("single_pk_table"));

            TapField compositeIdField = field("id", new TapNumber(), null);
            TapField compositeCodeField = field("code", new TapString(), null);
            TapField compositeFlagField = field("flag", new TapBoolean(), null);
            NodeSchemaInfo compositePkSchema = schema(
                    "node_composite",
                    "composite_pk_table",
                    List.of("id", "code"),
                    compositeIdField,
                    compositeCodeField,
                    compositeFlagField
            );
            operator.ensureTableExists(
                    "composite_pk_table",
                    List.of(compositeIdField, compositeCodeField, compositeFlagField),
                    List.of("id", "code"),
                    false
            );
            operator.insert("composite_pk_table", Map.of("id", 10L, "code", "A", "flag", true));
            operator.insert("composite_pk_table", Map.of("id", 11L, "code", "B", "flag", false));

            int compositeDeleted = operator.deleteByIds(
                    List.of(Map.of("id", "10", "code", "A")),
                    compositePkSchema
            );
            assertEquals(1, compositeDeleted);
            assertEquals(1L, operator.getRowCount("composite_pk_table"));
        }
    }

    @Test
    void buildCreateTableSql_andMapToDuckDbType_coverTypeBranches() {
        List<TapField> fields = new ArrayList<>();
        fields.add(field("pk-id", new TapNumber(), null));
        fields.add(field("event_time", new TapDateTime(), null));
        fields.add(field("payload", new TapBinary(), null));
        fields.add(field("enabled", new TapBoolean(), null));
        fields.add(field("amount", new TapNumber().fixed(true).precision(12).scale(2), null));
        fields.add(field("score", new TapNumber().bit(32).fixed(false), null));
        fields.add(field("age", new TapNumber().bit(32), null));
        fields.add(field("birth_date", new TapDate(), null));
        fields.add(field("remark", null, null));
        fields.add(new TapField());

        String createSql = DuckDbOperatorImpl.buildCreateTableSql(
                "test-table",
                fields,
                List.of("pk-id", "remark")
        );

        assertTrue(createSql.contains("CREATE TABLE test_table"));
        assertTrue(createSql.contains("pk_id BIGINT NOT NULL"));
        assertTrue(createSql.contains("event_time TIMESTAMP"));
        assertTrue(createSql.contains("payload BLOB"));
        assertTrue(createSql.contains("enabled BOOLEAN"));
        assertTrue(createSql.contains("amount DECIMAL(12,2)"));
        assertTrue(createSql.contains("score FLOAT"));
        assertTrue(createSql.contains("age INTEGER"));
        assertTrue(createSql.contains("birth_date DATE"));
        assertTrue(createSql.contains("remark VARCHAR NOT NULL"));
        assertTrue(createSql.contains("PRIMARY KEY (pk_id, remark)"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                DuckDbOperatorImpl.buildCreateTableSql("empty-table", List.of(new TapField()), List.of("id"))
        );
        assertTrue(ex.getMessage().contains("no valid columns"));
    }

    private static NodeSchemaInfo schema(String nodeId, String tableName, List<String> primaryKeys, TapField... tapFields) {
        TapTable tapTable = new TapTable(tableName);
        LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();
        for (TapField tapField : tapFields) {
            tapTable.add(tapField);
            fieldMap.put(tapField.getName(), tapField);
        }
        tapTable.setNameFieldMap(fieldMap);
        return new NodeSchemaInfo(nodeId, tableName, tableName, primaryKeys, fieldMap, tapTable, null);
    }

    private static NodeSchemaInfo schemaWithArrow(String nodeId, String tableName, List<String> primaryKeys, TapField... tapFields) {
        TapTable tapTable = new TapTable(tableName);
        LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();
        List<org.apache.arrow.vector.types.pojo.Field> arrowFields = new ArrayList<>();
        for (TapField tapField : tapFields) {
            tapTable.add(tapField);
            fieldMap.put(tapField.getName(), tapField);
            arrowFields.add(new org.apache.arrow.vector.types.pojo.Field(
                    tapField.getName(),
                    FieldType.nullable(new ArrowType.Utf8()),
                    null
            ));
        }
        tapTable.setNameFieldMap(fieldMap);
        return new NodeSchemaInfo(nodeId, tableName, tableName, primaryKeys, fieldMap, tapTable, new Schema(arrowFields));
    }

    private static TapField field(String name, Object tapType, String dataType) {
        TapField field = new TapField();
        field.setName(name);
        field.setOriginalFieldName(name);
        if (tapType instanceof io.tapdata.entity.schema.type.TapType typed) {
            field.setTapType(typed);
        }
        field.setDataType(dataType);
        return field;
    }

    private static TapdataEvent wrapInsert(String tableName, Map<String, Object> after) {
        io.tapdata.entity.event.dml.TapInsertRecordEvent event = io.tapdata.entity.event.dml.TapInsertRecordEvent.create().table(tableName).after(after);
        TapdataEvent tapdataEvent = new TapdataEvent();
        tapdataEvent.setTapEvent(event);
        return tapdataEvent;
    }

    private static Object invokePrivate(Object target, String methodName, Class<?>[] parameterTypes, Object[] args) throws Exception {
        java.lang.reflect.Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }
}

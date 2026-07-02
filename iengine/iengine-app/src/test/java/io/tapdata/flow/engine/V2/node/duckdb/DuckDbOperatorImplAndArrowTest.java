package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
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

    @Test
    void executeInTransaction_queryForMap_andBatchInsert_coverSuccessAndRollback() throws Exception {
        try (DuckDbOperatorImpl operator = new DuckDbOperatorImpl()) {
            operator.executeUpdate("CREATE TABLE txn_t(id BIGINT PRIMARY KEY, name VARCHAR, updated_at TIMESTAMP)");

            operator.executeInTransaction(() -> operator.insert("txn_t", Map.of(
                    "id", 1L,
                    "name", "ok",
                    "updated_at", LocalDateTime.of(2026, 7, 2, 10, 30, 45)
            )));

            SQLException failure = assertThrows(SQLException.class, () -> operator.executeInTransaction(() -> {
                operator.insert("txn_t", Map.of("id", 2L, "name", "rollback", "updated_at", LocalDateTime.of(2026, 7, 2, 10, 30, 45)));
                throw new SQLException("boom");
            }));
            assertEquals("boom", failure.getMessage());

            Map<Object, Map<String, Object>> result = operator.queryForMap("SELECT id, name FROM txn_t ORDER BY id", "id");
            assertEquals(1, result.size());
            assertEquals("ok", result.get(1L).get("name"));

            int inserted = operator.batchInsert("txn_t", List.of(
                    Map.of("id", 3L, "name", "b1", "updated_at", LocalDateTime.of(2026, 7, 2, 11, 0, 0)),
                    Map.of("id", 4L, "name", "b2", "updated_at", LocalDateTime.of(2026, 7, 2, 11, 5, 0))
            ));
            assertEquals(2, inserted);
            assertEquals(3L, operator.getRowCount("txn_t"));
            assertEquals(0, operator.batchInsert("txn_t", List.of()));
        }
    }

    @Test
    void ensureTableExists_andDeleteByIds_coverSingleAndCompositePrimaryKeys() throws Exception {
        try (DuckDbOperatorImpl operator = new DuckDbOperatorImpl()) {
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

            int deleted = operator.deleteByIds(List.of("1"), singlePkSchema);
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
        assertTrue(createSql.contains("remark VARCHAR NOT NULL"));
        assertTrue(createSql.contains("PRIMARY KEY (pk_id, remark)"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                DuckDbOperatorImpl.buildCreateTableSql("empty-table", List.of(new TapField()), List.of("id"))
        );
        assertTrue(ex.getMessage().contains("no valid columns"));
    }

    @Test
    void coversBatchQueryDdlDmlMetadataAndFormattingHelpers() throws Exception {
        try (DuckDbOperatorImpl operator = new DuckDbOperatorImpl()) {
            TapTable tapTable = new TapTable("crud_table");
            TapField idField = field("id", new TapNumber(), "BIGINT");
            idField.setPrimaryKey(true);
            TapField nameField = field("name", new TapString(), "VARCHAR");
            tapTable.setNameFieldMap(new LinkedHashMap<>(Map.of("id", idField, "name", nameField)));
            operator.createTable(tapTable);
            assertTrue(operator.tableExists("crud_table"));

            operator.insert("crud_table", Map.of("id", 1L, "name", "A"));
            operator.insert("crud_table", Map.of("id", 2L, "name", "B"));
            assertEquals(1, operator.update("crud_table", Map.of("name", "A1"), "id = 1"));
            assertEquals(1, operator.delete("crud_table", "id = 2"));

            operator.upsert("crud_table", Map.of("id", 1L, "name", "A2"));
            assertEquals("A2", operator.executeQuery("SELECT name FROM crud_table WHERE id = 1").get(0).get("name"));

            TapTableDto dto = new TapTableDto();
            dto.setName("dto_table");
            TapFieldDto dtoId = new TapFieldDto();
            dtoId.setName("id");
            dtoId.setDataType("BIGINT");
            dtoId.setIsPrimaryKey(true);
            TapFieldDto dtoName = new TapFieldDto();
            dtoName.setName("name");
            dtoName.setDataType("VARCHAR");
            dto.setFields(List.of(dtoId, dtoName));
            operator.createTable(dto);
            operator.addColumn("dto_table", "extra", "VARCHAR");
            operator.createIndex("dto_table", "idx_dto_name", List.of("name"), false);
            assertFalse(operator.getTableColumns("dto_table").isEmpty());
            assertTrue(operator.listTables().contains("crud_table"));
            operator.dropIndex("dto_table", "idx_dto_name");

            NodeSchemaInfo nodeSchemaInfo = schemaWithArrow(
                    "node_arrow",
                    "arrow_table",
                    List.of("id"),
                    field("id", new TapNumber(), "BIGINT"),
                    field("name", new TapString(), "VARCHAR")
            );
            operator.createTable(nodeSchemaInfo);
            operator.createTempTable(nodeSchemaInfo, true);

            List<TapdataEvent> events = List.of(
                    wrapInsert("arrow_table", Map.of("id", 1L, "name", "x")),
                    wrapInsert("arrow_table", Map.of("id", 2L, "name", "y"))
            );
            operator.insertBatch("arrow_table", events);
            operator.insertBatch(nodeSchemaInfo, List.of(wrapInsert("arrow_table", Map.of("id", 3L, "name", "z"))));
            assertEquals(3L, operator.getRowCount("arrow_table"));

            AtomicBoolean finalFlag = new AtomicBoolean(true);
            List<Map<String, Object>> collected = new ArrayList<>();
            operator.executeQueryInBatches(
                    "SELECT id, name FROM arrow_table ORDER BY id",
                    1,
                    ignored -> collected.size() < 1,
                    collected::add,
                    finalFlag::set
            );
            assertFalse(finalFlag.get());
            assertEquals(1, collected.size());

            String dateLiteral = (String) invokePrivate(
                    operator,
                    "formatPkValueForColumn",
                    new Class[]{Object.class, String.class, NodeSchemaInfo.class},
                    new Object[]{"2026-07-02", "date_col", schemaWithArrow(
                            "node_date",
                            "date_table",
                            List.of("date_col"),
                            field("date_col", null, "DATE")
                    )}
            );
            String timestampLiteral = (String) invokePrivate(
                    operator,
                    "formatPkValueForColumn",
                    new Class[]{Object.class, String.class, NodeSchemaInfo.class},
                    new Object[]{LocalDateTime.of(2026, 7, 2, 15, 0, 0), "ts_col", schemaWithArrow(
                            "node_ts",
                            "ts_table",
                            List.of("ts_col"),
                            field("ts_col", new TapDateTime(), "TIMESTAMP")
                    )}
            );
            String blobLiteral = (String) invokePrivate(
                    operator,
                    "formatPkValueForColumn",
                    new Class[]{Object.class, String.class, NodeSchemaInfo.class},
                    new Object[]{new byte[]{0x0A, 0x0B}, "blob_col", schemaWithArrow(
                            "node_blob",
                            "blob_table",
                            List.of("blob_col"),
                            field("blob_col", new TapBinary(), "BLOB")
                    )}
            );
            assertEquals("DATE '2026-07-02'", dateLiteral);
            assertTrue(timestampLiteral.startsWith("TIMESTAMP '2026-07-02 15:00:00'"));
            assertEquals("'\\x0a\\x0b'::BLOB", blobLiteral);
        }
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

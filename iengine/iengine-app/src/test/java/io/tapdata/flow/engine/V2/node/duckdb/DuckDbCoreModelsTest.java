package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.flow.engine.V2.node.duckdb.utils.TablePkUtils;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DuckDbCoreModelsTest {
    @AfterEach
    void resetDuckDbSqlConfig() {
        DuckDbSqlConfig.resetToDefault();
    }

    @Test
    void duckLakeConfig_basicBehavior() {
        DuckLakeConfig disabled = DuckLakeConfig.disabled();
        assertFalse(disabled.isEnabled());
        assertFalse(disabled.isLocalStorage());
        assertFalse(disabled.isS3Storage());

        DuckLakeConfig local = new DuckLakeConfig(true, "LOCAL", "/tmp/ducklake", "jdbc:postgresql://x/y");
        assertTrue(local.isEnabled());
        assertTrue(local.isLocalStorage());
        assertFalse(local.isS3Storage());
        assertTrue(local.toString().contains("LOCAL"));

        DuckLakeConfig s3 = new DuckLakeConfig(true, "S3", "s3://bucket/path", null);
        assertTrue(s3.isS3Storage());
        assertFalse(s3.isLocalStorage());
    }

    @Test
    void duckDbSqlConfig_settersAndResetToDefault() {
        DuckDbSqlConfig.setUseNewWideTableUpdater(false);
        DuckDbSqlConfig.setDbPath("/tmp/test.duckdb");
        assertFalse(DuckDbSqlConfig.isUseNewWideTableUpdater());
        assertEquals("/tmp/test.duckdb", DuckDbSqlConfig.getDbPath());

        DuckDbSqlConfig.resetToDefault();

        boolean expectedUseNew = envBool("DUCKDB_USE_NEW_WIDE_TABLE_UPDATER", true);
        String expectedDbPath = System.getenv("DUCKDB_DB_PATH");
        boolean expectedDuckLakeEnabled = envBool("DUCKDB_DUCKLAKE_ENABLED", false);
        String expectedStorageType = Objects.requireNonNullElse(System.getenv("DUCKDB_DUCKLAKE_STORAGE_TYPE"), "LOCAL");
        String expectedStoragePath = Objects.requireNonNullElse(System.getenv("DUCKDB_DUCKLAKE_STORAGE_PATH"), "/tmp/ducklake");
        String expectedMetadataUrl = System.getenv("DUCKDB_DUCKLAKE_METADATA_URL");

        assertEquals(expectedUseNew, DuckDbSqlConfig.isUseNewWideTableUpdater());
        assertEquals(expectedDbPath, DuckDbSqlConfig.getDbPath());
        assertEquals(expectedDuckLakeEnabled, DuckDbSqlConfig.isDuckLakeEnabled());
        assertEquals(expectedStorageType, DuckDbSqlConfig.getDuckLakeStorageType());
        assertEquals(expectedStoragePath, DuckDbSqlConfig.getDuckLakeStoragePath());
        assertEquals(expectedMetadataUrl, DuckDbSqlConfig.getDuckLakeMetadataDbUrl());
    }

    @Test
    void fromTableConfig_validation() {
        assertThrows(IllegalArgumentException.class, () -> new FromTableConfig("", "t1"));
        assertThrows(IllegalArgumentException.class, () -> new FromTableConfig("n1", "  "));

        FromTableConfig cfg = new FromTableConfig("n1", "t1");
        assertEquals("n1", cfg.getPreNodeId());
        assertEquals("t1", cfg.getTableNameInSql());

        cfg.setPreNodeId(null);
        cfg.setTableNameInSql(null);
        assertNull(cfg.getPreNodeId());
        assertNull(cfg.getTableNameInSql());

        assertThrows(IllegalArgumentException.class, () -> cfg.setPreNodeId(" "));
        assertThrows(IllegalArgumentException.class, () -> cfg.setTableNameInSql("\t"));
    }

    @Test
    void nodeSchemaInfo_primaryKeyAndFieldLookupAndTargetTableName() {
        NodeSchemaInfo schemaInfo = buildNodeSchema("node-1", "my table", List.of("id"));
        assertTrue(schemaInfo.isValid());
        assertTrue(schemaInfo.hasField("id"));
        assertFalse(schemaInfo.hasField("missing"));
        assertTrue(schemaInfo.isPrimaryKey("id"));
        assertFalse(schemaInfo.isPrimaryKey("name"));

        assertEquals("my_table", schemaInfo.getTargetTableName());
        assertEquals(2, schemaInfo.getFieldCount());
        assertEquals("TapTable", schemaInfo.getTapTable().getClass().getSimpleName());
    }

    @Test
    void wideTableSourceRegistry_buildsDescriptorsAndResolvesMainTable() {
        NodeSchemaInfo users = buildNodeSchema("n_users", "users", List.of("id"));
        NodeSchemaInfo orders = buildNodeSchema("n_orders", "orders", List.of("id"));
        Map<String, NodeSchemaInfo> nodeSchemaCache = Map.of(
                "pre1", users,
                "pre2", orders
        );
        List<FromTableConfig> fromTables = List.of(
                new FromTableConfig("pre1", "u"),
                new FromTableConfig("pre2", "o")
        );

        WideTableSourceRegistry registry = WideTableSourceRegistry.from("u", fromTables, nodeSchemaCache);
        assertFalse(registry.isEmpty());
        assertEquals("users", registry.getMainSourceTableName());
        assertTrue(registry.containsSourceTableName("users"));
        assertTrue(registry.containsSourceTableName("\"USERS\""));
        assertTrue(registry.getDescriptor("users").isMainTable());
        assertFalse(registry.getDescriptor("orders").isMainTable());

        WideTableSourceRegistry fallbackRegistry = WideTableSourceRegistry.from("unknown", fromTables, nodeSchemaCache);
        assertEquals("users", fallbackRegistry.getMainSourceTableName());
        assertTrue(fallbackRegistry.getDescriptor("users").isMainTable());
    }

    @Test
    void wideTableSourceDescriptor_getters() {
        NodeSchemaInfo users = buildNodeSchema("n_users", "users", List.of("id"));
        WideTableSourceDescriptor d = new WideTableSourceDescriptor("users", "u", true, users);
        assertEquals("users", d.getSourceTableName());
        assertEquals("u", d.getSqlAlias());
        assertTrue(d.isMainTable());
        assertSame(users, d.getSchemaInfo());
    }

    @Test
    void perSourceContext_bufferAndSchemaAccess() {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        NodeSchemaInfo schema = buildNodeSchema("n1", "t", List.of("id"));
        PerSourceContext ctx = new PerSourceContext("k", operator, "s1", "t1", schema);

        assertTrue(ctx.hasSchema());
        assertEquals("t", ctx.getTableNameFromSchema());
        assertEquals("t", ctx.getQualifiedNameFromSchema());
        assertTrue(ctx.isPrimaryKeyField("id"));
        assertNotNull(ctx.getFieldType("id"));

        TapdataEvent e1 = new TapdataEvent();
        TapdataEvent e2 = new TapdataEvent();
        ctx.setBatchSize(1);
        ctx.addEvent(e1);
        assertTrue(ctx.needAccept());
        ctx.addEvent(e2);
        List<TapdataEvent> drained = ctx.drainBuffer();
        assertEquals(2, drained.size());
        assertEquals(0, ctx.getBatchBuffer().size());
    }

    @Test
    void outputBuffer_addAndFlush() {
        OutputBuffer buffer = new OutputBuffer(2);
        assertFalse(buffer.isReadyToEmit());
        buffer.addResult(Map.of("id", 1));
        assertFalse(buffer.isReadyToEmit());
        buffer.addResult(Map.of("id", 2));
        assertTrue(buffer.isReadyToEmit());
        List<Map<String, Object>> batch = buffer.flushBatch();
        assertEquals(2, batch.size());
        assertFalse(buffer.isReadyToEmit());
    }

    @Test
    void errorHandler_shouldStopTask_byCountAndRate() {
        ErrorHandler byCount = new ErrorHandler(2, 1.0);
        byCount.recordEvent();
        byCount.recordError(Map.of("id", 1), new RuntimeException("x"));
        assertFalse(byCount.shouldStopTask());
        byCount.recordEvent();
        byCount.recordError(Map.of("id", 2), new RuntimeException("y"));
        assertTrue(byCount.shouldStopTask());

        ErrorHandler byRate = new ErrorHandler(100, 0.4);
        byRate.recordEvent();
        byRate.recordError(Map.of(), new RuntimeException("x"));
        assertTrue(byRate.shouldStopTask());
    }

    @Test
    void schemaResolver_prefersAfterThenBeforeThenMetadata() throws SQLException {
        DuckDbOperator operator = mock(DuckDbOperator.class);
        SchemaResolver resolver = new SchemaResolver(operator);

        List<String> fromAfter = resolver.resolveFields("t", Map.of("after", Map.of("a", 1, "b", 2)));
        assertEquals(2, fromAfter.size());
        assertTrue(fromAfter.contains("a"));
        assertTrue(fromAfter.contains("b"));

        List<String> fromBefore = resolver.resolveFields("t", Map.of("before", Map.of("c", 1)));
        assertEquals(List.of("c"), fromBefore);

        when(operator.executeQuery(anyString())).thenReturn(List.of(
                Map.of("column_name", "id"),
                Map.of("column_name", "name")
        ));
        List<String> fromMeta = resolver.resolveFields("t", Map.of());
        assertEquals(List.of("id", "name"), fromMeta);

        Mockito.reset(operator);
        when(operator.executeQuery(anyString())).thenThrow(new SQLException("x"));
        List<String> fromMetaFailure = resolver.resolveFields("t", Map.of());
        assertTrue(fromMetaFailure.isEmpty());
    }

    @Test
    void schemaTypeCache_getOrCompute_cachesAndExpires() {
        SchemaTypeCache cache = SchemaTypeCache.getInstance();
        cache.clearAll();
        cache.setCacheExpireTime(60_000);
        cache.setCleanupInterval(60_000);

        AtomicInteger counter = new AtomicInteger();
        ArrowType t1 = cache.getOrComputeArrowType("t", "f", () -> {
            counter.incrementAndGet();
            return new ArrowType.Int(64, true);
        });
        ArrowType t2 = cache.getOrComputeArrowType("t", "f", () -> {
            counter.incrementAndGet();
            return new ArrowType.Int(64, true);
        });
        assertEquals(1, counter.get());
        assertEquals(t1, t2);

        cache.clearAll();
        cache.setCacheExpireTime(-1);
        cache.setCleanupInterval(0);
        AtomicInteger counter2 = new AtomicInteger();
        cache.getOrComputeDuckDbType("t", "f", () -> {
            counter2.incrementAndGet();
            return "BIGINT";
        });
        cache.getOrComputeDuckDbType("t", "f", () -> {
            counter2.incrementAndGet();
            return "BIGINT";
        });
        assertEquals(2, counter2.get());
    }

    @Test
    void typeConverter_convertsToArrowAndDuckDbTypes() {
        assertTrue(TypeConverter.fromDataType("int") instanceof ArrowType.Int);
        assertEquals("INTEGER", TypeConverter.toDuckDbType("int"));
        assertEquals("BIGINT", TypeConverter.toDuckDbType(new ArrowType.Int(64, true)));
        assertEquals("BOOLEAN", TypeConverter.toDuckDbType(new ArrowType.Bool()));

        TapField tapField = new TapField();
        tapField.setName("id");
        tapField.setOriginalFieldName("id");
        tapField.setDataType("BIGINT");
        tapField.setTapType(new TapNumber());
        assertEquals("BIGINT", TypeConverter.getDuckDbType(tapField));
    }

    @Test
    void wideTableRowTypeNormalizer_normalizesNumericsAndBooleans() {
        NodeSchemaInfo schema = buildNodeSchema("n1", "t", List.of("id"));
        List<Map<String, Object>> rows = List.of(
                Map.of("id", "123", "name", "x"),
                Map.of("id", 456, "name", "y")
        );
        List<Map<String, Object>> normalized = WideTableRowTypeNormalizer.normalizeRows(rows, schema);
        assertEquals(123L, normalized.get(0).get("id"));
        assertEquals(456L, normalized.get(1).get("id"));
    }

    private static boolean envBool(String key, boolean defaultValue) {
        String value = System.getenv(key);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    private static NodeSchemaInfo buildNodeSchema(String nodeId, String tableName, List<String> pks) {
        TapTable tapTable = new TapTable();
        LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();

        TapField id = new TapField();
        id.setName("id");
        id.setOriginalFieldName("id");
        id.setDataType("BIGINT");
        id.setPrimaryKey(true);
        id.setTapType(new TapNumber());
        fieldMap.put("id", id);

        TapField name = new TapField();
        name.setName("name");
        name.setOriginalFieldName("name");
        name.setDataType("VARCHAR");
        fieldMap.put("name", name);

        tapTable.setNameFieldMap(fieldMap);
        return new NodeSchemaInfo(nodeId, tableName, tableName, pks, fieldMap, tapTable, null);
    }
}


package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.process.converter.TmSchemaConverter;
import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import com.tapdata.tm.commons.dag.process.duck.JoinField;
import com.tapdata.tm.commons.dag.process.duck.JoinInfo;
import com.tapdata.tm.commons.dag.process.duck.JoinKeyPair;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.util.DuckDbSqlPrimaryKeyAnalyzer;
import io.github.openlg.graphlib.Graph;
import org.junit.jupiter.api.Test;
import org.bson.types.ObjectId;
import org.mockito.MockedStatic;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

/**
 * DuckDbSqlNode 基础单元测试，覆盖重构后的关键行为。
 */
public class DuckDbSqlNodeTest {

    @Test
    void duckDbSqlNodeHasStableType() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        assertEquals("duckdb_sql_processor", node.getType());
    }

    @Test
    void snapshotCdcBarrierEnableAlwaysReturnsTrue() {
        assertTrue(new DuckDbSqlNode().snapshotCdcBarrierEnable());
    }

    @Test
    void testPreNodeTapTablesDefaultValue() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        List<TapTableDto> tables = node.getPreNodeTapTables();
        assertNotNull(tables, "preNodeTapTables should not be null");
        assertTrue(tables.isEmpty(), "preNodeTapTables should be empty by default");
    }

    @Test
    void testSetAndGetPreNodeTapTables() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        
        TapTableDto table = new TapTableDto()
                .id("src_1")
                .name("test_table")
                .addField(new TapFieldDto().name("col1").dataType("INT"));

        List<TapTableDto> tables = new ArrayList<>();
        tables.add(table);
        node.setPreNodeTapTables(tables);

        List<TapTableDto> result = node.getPreNodeTapTables();
        assertEquals(1, result.size());
        assertEquals("src_1", result.get(0).getId());
        assertEquals("test_table", result.get(0).getName());
    }

    @Test
    void testSetPreNodeTapTablesToNull_EqualityStillWorks() {
        DuckDbSqlNode node1 = new DuckDbSqlNode();
        DuckDbSqlNode node2 = new DuckDbSqlNode();
        
        node1.setPreNodeTapTables(null);
        node2.setPreNodeTapTables(null);
        
        // 两个节点的 preNodeTapTables 都为 null，eqField 比较应通过
        assertEquals(node1, node2,
            "Two nodes with null preNodeTapTables should be equal via @EqField comparison");
    }

    @Test
    void testEqFieldComparisonWithPreNodeTapTables() {
        DuckDbSqlNode node1 = new DuckDbSqlNode();
        DuckDbSqlNode node2 = new DuckDbSqlNode();

        // 设置相同配置
        TapTableDto table = new TapTableDto().id("src").name("tbl");
        node1.setPreNodeTapTables(Collections.singletonList(table));
        node2.setPreNodeTapTables(Collections.singletonList(
                new TapTableDto().id("src").name("tbl")));

        // 通过 @EqField 比较（equals 方法由 DuckDbSqlNode 的反射实现）
        assertEquals(node1, node2,
            "Two nodes with same TapTableDto content should be equal");
    }

    @Test
    void testHashCodeMatchesEqualsForEqFields() {
        DuckDbSqlNode node1 = new DuckDbSqlNode();
        DuckDbSqlNode node2 = new DuckDbSqlNode();

        node1.setQuerySql("SELECT id FROM users");
        node2.setQuerySql("SELECT id FROM users");
        node1.setPreNodeTapTables(Collections.singletonList(
                new TapTableDto().id("src").name("tbl")));
        node2.setPreNodeTapTables(Collections.singletonList(
                new TapTableDto().id("src").name("tbl")));

        assertEquals(node1, node2);
        assertEquals(node1.hashCode(), node2.hashCode(),
                "Equal DuckDbSqlNode instances should have the same hashCode");
    }

    @Test
    void testEqualsReturnsTrueForSameInstanceAndFalseForOtherType() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        assertEquals(node, node);
        assertNotEquals(node, "not a DuckDbSqlNode");
    }

    @Test
    void testEqFieldComparisonDifferentPreNodeTapTables() {
        DuckDbSqlNode node1 = new DuckDbSqlNode();
        DuckDbSqlNode node2 = new DuckDbSqlNode();

        node1.setPreNodeTapTables(Collections.singletonList(
                new TapTableDto().id("src_1").name("table_a")));
        node2.setPreNodeTapTables(Collections.singletonList(
                new TapTableDto().id("src_2").name("table_b")));

        // 不同配置应不相等
        assertNotEquals(node1, node2,
            "Two nodes with different TapTableDto should not be equal");
    }

    @Test
    void testEqFieldComparisonOneHasPreNodeOneDoesNot() {
        DuckDbSqlNode node1 = new DuckDbSqlNode();
        DuckDbSqlNode node2 = new DuckDbSqlNode();

        node1.setPreNodeTapTables(Collections.singletonList(
                new TapTableDto().id("src").name("tbl")));
        // node2 保留默认空列表

        assertNotEquals(node1, node2,
            "Node with preNodeTapTables should not equal node with empty preNodeTapTables");
    }

    @Test
    void testDefaultBatchSizeConstant() {
        assertEquals(2000, DuckDbSqlNode.DEFAULT_BATCH_SIZE);
    }

    @Test
    void testAnalyzePrimaryKeysUsesJoinKeyProjection() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setMainTableName("u");
        node.setMainTablePrimaryKey("id");

        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT upper(o.user_id) AS wide_user_id, o.amount " +
                        "FROM users u LEFT JOIN orders o ON u.id = o.user_id"
        );

        assertEquals(Collections.singletonList("wide_user_id"), primaryKeys);
    }

    @Test
    void findFirstFromTableSchemaReturnsNullWhenNoInputs() throws Exception {
        DuckDbSqlNode node = new DuckDbSqlNode();
        assertNull(invokeFindFirstFromTableSchema(node, Collections.emptyList()));
    }

    @Test
    void findFirstFromTableSchemaReturnsFirstSchemaWhenNoFromTablesConfigured() throws Exception {
        DuckDbSqlNode node = new DuckDbSqlNode();
        Schema first = buildSchema("pre_1", "users", List.of(pkField("id")));
        Schema second = buildSchema("pre_2", "orders", List.of(pkField("order_id")));

        assertSame(first, invokeFindFirstFromTableSchema(node, List.of(first, second)));
    }

    @Test
    void findFirstFromTableSchemaMatchesByPreNodeId() throws Exception {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setFromTables(List.of(new FromTableConfig("pre_orders", "orders")));
        Schema users = buildSchema("pre_users", "users", List.of(pkField("id")));
        Schema orders = buildSchema("pre_orders", "orders", List.of(pkField("order_id")));

        assertSame(orders, invokeFindFirstFromTableSchema(node, List.of(users, orders)));
    }

    @Test
    void findFirstFromTableSchemaMatchesBySchemaName() throws Exception {
        DuckDbSqlNode node = new DuckDbSqlNode();
        FromTableConfig fromTableConfig = new FromTableConfig();
        fromTableConfig.setPreNodeId("unknown");
        fromTableConfig.setTableNameInSql("orders");
        node.setFromTables(List.of(fromTableConfig));

        Schema users = buildSchema("pre_users", "users", List.of(pkField("id")));
        Schema orders = buildSchema("pre_orders", "orders", List.of(pkField("order_id")));

        assertSame(orders, invokeFindFirstFromTableSchema(node, List.of(users, orders)));
    }

    @Test
    void findFirstFromTableSchemaMatchesByOriginalName() throws Exception {
        DuckDbSqlNode node = new DuckDbSqlNode();
        FromTableConfig fromTableConfig = new FromTableConfig();
        fromTableConfig.setPreNodeId("unknown");
        fromTableConfig.setTableNameInSql("orders_original");
        node.setFromTables(List.of(fromTableConfig));

        Schema users = buildSchema("pre_users", "users", List.of(pkField("id")));
        Schema orders = buildSchema("pre_orders", "orders_alias", List.of(pkField("order_id")));
        orders.setOriginalName("orders_original");

        assertSame(orders, invokeFindFirstFromTableSchema(node, List.of(users, orders)));
    }

    @Test
    void findFirstFromTableSchemaMatchesByQualifiedName() throws Exception {
        DuckDbSqlNode node = new DuckDbSqlNode();
        FromTableConfig fromTableConfig = new FromTableConfig();
        fromTableConfig.setPreNodeId("unknown");
        fromTableConfig.setTableNameInSql("db.orders");
        node.setFromTables(List.of(fromTableConfig));

        Schema users = buildSchema("pre_users", "users", List.of(pkField("id")));
        Schema orders = buildSchema("pre_orders", "orders_alias", List.of(pkField("order_id")));
        orders.setOriginalName("orders_original");
        orders.setQualifiedName("db.orders");

        assertSame(orders, invokeFindFirstFromTableSchema(node, List.of(users, orders)));
    }

    @Test
    void findFirstFromTableSchemaFallsBackToFirstSchemaWhenNothingMatches() throws Exception {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setFromTables(List.of(new FromTableConfig("missing", "not_exists")));
        Schema first = buildSchema("pre_1", "users", List.of(pkField("id")));
        Schema second = buildSchema("pre_2", "orders", List.of(pkField("order_id")));

        assertSame(first, invokeFindFirstFromTableSchema(node, List.of(first, second)));
    }

    @Test
    void resolveMainTableInfoResolvesMainInfoAndWidePkFromSqlAnalysis() throws Exception {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setQuerySql("SELECT upper(o.user_id) AS wide_user_id, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id");
        node.setFromTables(List.of(new FromTableConfig("pre_users", "users")));

        Schema users = buildSchema("pre_users", "users", List.of(pkField("id"), normalField("name")));

        invokeResolveMainTableInfo(node, List.of(users));

        assertEquals("users", node.getMainTableName());
        assertEquals("id", node.getMainTablePrimaryKey());
        assertEquals("wide_users", node.getWideTableName());
        assertEquals("wide_user_id", node.getWideTablePrimaryKey());
    }

    @Test
    void resolveMainTableInfoRejectsNullFirstFromTable() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setQuerySql("SELECT 1");
        node.setFromTables(Arrays.asList((FromTableConfig) null));

        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> invokeResolveMainTableInfo(node, Collections.emptyList()));
        assertInstanceOf(IllegalStateException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("First fromTableConfig is null"));
    }

    @Test
    void resolveMainTableInfoRejectsBlankTableNameInSql() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setQuerySql("SELECT 1");
        FromTableConfig fromTableConfig = new FromTableConfig();
        fromTableConfig.setPreNodeId("pre_1");
        fromTableConfig.setTableNameInSql(" ");
        node.setFromTables(List.of(fromTableConfig));

        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> invokeResolveMainTableInfo(node, Collections.emptyList()));
        assertInstanceOf(IllegalStateException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("no tableNameInSql"));
    }

    @Test
    void resolveMainTableInfoRejectsSchemaWithoutPrimaryKey() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setQuerySql("SELECT 1");
        node.setFromTables(List.of(new FromTableConfig("pre_1", "users")));

        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> invokeResolveMainTableInfo(node, List.of(buildSchema("pre_1", "users", List.of(normalField("name"))))));
        assertInstanceOf(IllegalStateException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("no primary keys defined"));
    }

    @Test
    void resolveMainTableInfoRejectsNullSchemaReturnedFromLookup() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setQuerySql("SELECT 1");
        node.setMainTableName("users");

        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> invokeResolveMainTableInfo(node, Arrays.asList((Schema) null)));
        assertInstanceOf(IllegalStateException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("no schema found for mainTableName"));
    }

    @Test
    void resolveMainTableInfoRejectsBlankResolvedPrimaryKeyName() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setQuerySql("SELECT 1");
        node.setFromTables(List.of(new FromTableConfig("pre_1", "users")));

        Field blankPk = pkField(" ");
        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> invokeResolveMainTableInfo(node, List.of(buildSchema("pre_1", "users", List.of(blankPk)))));
        assertInstanceOf(IllegalStateException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("mainTablePrimaryKey is blank"));
    }

    @Test
    void resolveMainTableInfoRejectsBlankWideTableNameWhenMainTableMissing() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setQuerySql("SELECT 1");

        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> invokeResolveMainTableInfo(node, Collections.emptyList()));
        assertInstanceOf(IllegalStateException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("Cannot resolve wideTableName"));
    }

    @Test
    void resolveMainTableInfoRejectsBlankWideTablePrimaryKeyWhenSqlAnalysisProducesNothing() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setQuerySql("SELECT 1");
        node.setMainTableName("users");
        node.setMainTablePrimaryKey("id");

        InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> invokeResolveMainTableInfo(node, Collections.emptyList()));
        assertInstanceOf(IllegalStateException.class, ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("wideTablePrimaryKey is blank"));
    }

    @Test
    void mergeSchemaRejectsEmptyInputSchemas() {
        DuckDbSqlNode node = newDuckDbSqlNodeWithDag("node_1");
        node.setQuerySql("SELECT 1");
        node.setMainTableName("t");
        node.setWideTablePrimaryKey("id");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                node.mergeSchema(Collections.emptyList(), null, new DAG.Options())
        );
        assertTrue(ex.getMessage().contains("inputSchemas is empty"));
    }

    @Test
    void mergeSchemaRejectsBlankQuerySql() {
        DuckDbSqlNode node = newDuckDbSqlNodeWithDag("node_1");
        node.setMainTableName("users");
        node.setWideTablePrimaryKey("id");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                node.mergeSchema(List.of(buildSchema("pre_1", "users", List.of(pkField("id"), normalField("name")))), null, new DAG.Options())
        );
        assertTrue(ex.getMessage().contains("querySql is blank"));
    }

    @Test
    void mergeSchemaUsesProvidedSchemaMetaTypeAndSourceType() {
        DuckDbSqlNode node = newDuckDbSqlNodeWithDag("node_1");
        node.setMainTableName("users");
        node.setWideTablePrimaryKey("id");
        node.setQuerySql("SELECT u.id AS id FROM users u");

        Schema input = buildSchema("pre_1", "users", List.of(pkField("id")));
        Schema existing = new Schema();
        existing.setMetaType("manual_meta");
        existing.setSourceType("manual_source");

        Schema merged = node.mergeSchema(List.of(input), existing, new DAG.Options());

        assertEquals("manual_meta", merged.getMetaType());
        assertEquals("manual_source", merged.getSourceType());
    }

    @Test
    void mergeSchemaAllowsNullTaskIdWhenDagTaskIdMissing() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setId("node_without_task_id");
        node.setDag(new DAG(new Graph<>()));
        node.setMainTableName("users");
        node.setWideTablePrimaryKey("id");
        node.setQuerySql("SELECT u.id AS id FROM users u");

        Schema input = buildSchema("pre_1", "users", List.of(pkField("id")));
        Schema merged = node.mergeSchema(List.of(input), null, new DAG.Options());

        assertNull(merged.getTaskId());
        assertNotNull(merged.getQualifiedName());
    }

    @Test
    void mergeSchemaResolvesMainTableFromFromTablesAndBuildsWideSchema() {
        DuckDbSqlNode node = newDuckDbSqlNodeWithDag("node_1");
        node.setQuerySql("SELECT u.id AS id, u.name AS name FROM users u");
        node.setWideTablePrimaryKey("id");
        node.setFromTables(List.of(new FromTableConfig("pre_1", "users")));

        Schema input = buildSchema("pre_1", "users", List.of(pkField("id"), normalField("name")));
        input.setDatabaseId("db_1");

        Schema merged = node.mergeSchema(List.of(input), null, new DAG.Options());

        assertEquals("wide_users", merged.getName());
        assertEquals("users", merged.getOriginalName());
        assertEquals("db_1", merged.getDatabaseId());
        assertNotNull(merged.getQualifiedName());
        assertNotNull(merged.getTaskId());
        assertEquals("node_1", merged.getNodeId());

        assertNotNull(merged.getFields());
        assertEquals(2, merged.getFields().size());

        Field id = merged.getFields().stream().filter(f -> "id".equals(f.getFieldName())).findFirst().orElseThrow();
        assertEquals("id", id.getFieldName());
        assertEquals(Field.SOURCE_JOB_ANALYZE, id.getSource());
        assertEquals("users", id.getTableName());
        assertNotNull(id.getId());
        assertNotNull(id.getOriginalFieldName());

        Field name = merged.getFields().stream().filter(f -> "name".equals(f.getFieldName())).findFirst().orElseThrow();
        assertEquals("name", name.getFieldName());
        assertEquals(Field.SOURCE_JOB_ANALYZE, name.getSource());
        assertEquals("users", name.getTableName());
        assertNotNull(name.getId());
        assertNotNull(name.getOriginalFieldName());

        assertNotNull(node.getPreNodeTapTables());
        assertTrue(node.getPreNodeTapTables().size() >= 2);
    }

    @Test
    void mergeSchemaSupportsComputedColumnAliases() {
        DuckDbSqlNode node = newDuckDbSqlNodeWithDag("node_1");
        node.setMainTableName("users");
        node.setWideTablePrimaryKey("upper_name");
        node.setQuerySql("SELECT upper(u.name) AS upper_name FROM users u");

        Schema input = buildSchema("pre_1", "users", List.of(pkField("id"), normalField("name")));
        Schema merged = node.mergeSchema(List.of(input), null, new DAG.Options());

        Field field = merged.getFields().get(0);
        assertEquals("upper_name", field.getFieldName());
        assertNotNull(field.getOriginalFieldName());
        assertFalse(field.getOriginalFieldName().isBlank());
    }

    @Test
    void mergeSchemaRejectsWhenSqlParserReturnsNoFields() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setId("node_1");
        node.setMainTableName("users");
        node.setWideTablePrimaryKey("id");
        node.setQuerySql("SELECT id FROM users");

        Schema input = buildSchema("pre_1", "users", List.of(pkField("id")));
        try (MockedStatic<com.tapdata.tm.commons.util.SqlParserUtil> sqlParserUtil = mockStatic(com.tapdata.tm.commons.util.SqlParserUtil.class)) {
            sqlParserUtil.when(() -> com.tapdata.tm.commons.util.SqlParserUtil.parseSelectFields(
                    anyString(), anyList(), anyList(), anyString(), anyList(), anyList(), anyMap()
            )).thenReturn(Collections.emptyList());

            IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                    node.mergeSchema(List.of(input), null, new DAG.Options()));
            assertTrue(ex.getMessage().contains("no fields parsed"));
        }
    }

    @Test
    void mergeSchemaFillsBlankOriginalFieldNameWhenSqlParserDoesNotProvideIt() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setId("node_1");
        node.setDag(new DAG(new Graph<>()));
        node.setMainTableName("users");
        node.setWideTableName("wide_users");
        node.setMainTablePrimaryKey("id");
        node.setWideTablePrimaryKey("id");
        node.setQuerySql("SELECT id FROM users");

        Field parsedField = new Field();
        parsedField.setFieldName("id");
        parsedField.setOriginalFieldName(" ");
        parsedField.setPrimaryKey(true);
        parsedField.setDataType("INT");
        parsedField.setJavaType("Integer");

        Schema input = buildSchema("pre_1", "users", List.of(pkField("id")));
        try (MockedStatic<com.tapdata.tm.commons.util.SqlParserUtil> sqlParserUtil = mockStatic(com.tapdata.tm.commons.util.SqlParserUtil.class)) {
            sqlParserUtil.when(() -> com.tapdata.tm.commons.util.SqlParserUtil.parseSelectFields(
                    anyString(), anyList(), anyList(), anyString(), anyList(), anyList(), anyMap()
            )).thenReturn(List.of(parsedField));

            Schema merged = node.mergeSchema(List.of(input), null, new DAG.Options());
            assertEquals("id", merged.getFields().get(0).getOriginalFieldName());
        }
    }

    @Test
    void mergeSchemaUsesSqlAnalysisToDeriveWideTablePrimaryKeyWhenJoinPresent() {
        DuckDbSqlNode node = newDuckDbSqlNodeWithDag("node_1");
        node.setMainTableName("users");
        node.setQuerySql("SELECT upper(o.user_id) AS wide_user_id, o.amount AS amount " +
                "FROM users u LEFT JOIN orders o ON u.id = o.user_id");

        Schema users = buildSchema("pre_users", "users", List.of(pkField("id"), normalField("name")));
        Schema orders = buildSchema("pre_orders", "orders", List.of(pkField("order_id"), normalField("user_id"), normalField("amount")));

        Schema merged = node.mergeSchema(List.of(users, orders), null, new DAG.Options());

        assertEquals("wide_users", merged.getName());
        assertNotNull(merged.getFields());
        assertTrue(merged.getFields().stream().anyMatch(f -> "wide_user_id".equals(f.getFieldName())));

        assertNotNull(merged.getIndices());
        assertFalse(merged.getIndices().isEmpty());
        assertEquals("wide_user_id", merged.getIndices().get(0).getColumns().get(0).getColumnName());
    }

    @Test
    void mergeSchemaRejectsWideTablePrimaryKeyNotInParsedFields() {
        DuckDbSqlNode node = newDuckDbSqlNodeWithDag("node_1");
        node.setMainTableName("users");
        node.setQuerySql("SELECT id AS id, name AS name FROM users");
        node.setWideTablePrimaryKey("missing_pk");

        Schema input = buildSchema("pre_1", "users", List.of(pkField("id"), normalField("name")));
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                node.mergeSchema(List.of(input), null, new DAG.Options())
        );
        assertTrue(ex.getMessage().contains("wideTablePrimaryKey column"));
        assertTrue(ex.getMessage().contains("missing_pk"));
    }

    @Test
    void mergeSchemaRejectsSelectStar() {
        DuckDbSqlNode node = newDuckDbSqlNodeWithDag("node_1");
        node.setMainTableName("users");
        node.setQuerySql("SELECT * FROM users");
        node.setWideTablePrimaryKey("id");

        Schema input = buildSchema("pre_1", "users", List.of(pkField("id"), normalField("name")));
        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                node.mergeSchema(List.of(input), null, new DAG.Options())
        );
        assertTrue(ex.getMessage().contains("failed to parse SQL query"));
    }

    @Test
    void collectIndexCreatesIndexWhenSchemaHasNoIndices() {
        TestableDuckDbSqlNode node = new TestableDuckDbSqlNode();
        Schema schema = new Schema();

        node.callCollectIndex(schema, List.of("id", "tenant_id"));

        assertNotNull(schema.getIndices());
        assertEquals(1, schema.getIndices().size());
        TableIndex index = schema.getIndices().get(0);
        assertEquals("INDEX_TABLE_DUCK_id_tenant_id", index.getIndexName());
        assertEquals(2, index.getColumns().size());
        assertEquals("id", index.getColumns().get(0).getColumnName());
        assertEquals(1, index.getColumns().get(0).getColumnPosition());
        assertEquals("tenant_id", index.getColumns().get(1).getColumnName());
        assertEquals(2, index.getColumns().get(1).getColumnPosition());
    }

    @Test
    void collectIndexAppendsToExistingIndices() {
        TestableDuckDbSqlNode node = new TestableDuckDbSqlNode();
        Schema schema = new Schema();
        schema.setIndices(new ArrayList<>(List.of(new TableIndex())));

        node.callCollectIndex(schema, List.of("id"));

        assertEquals(2, schema.getIndices().size());
        assertEquals("INDEX_TABLE_DUCK_id", schema.getIndices().get(1).getIndexName());
    }

    @Test
    void initTableAttrIfNeedInitializesAndPreservesExistingMap() {
        TestableDuckDbSqlNode node = new TestableDuckDbSqlNode();
        Schema schema = new Schema();

        node.callInitTableAttrIfNeed(schema);
        assertNotNull(schema.getTableAttr());

        Map<String, Object> existing = new HashMap<>();
        schema.setTableAttr(existing);
        node.callInitTableAttrIfNeed(schema);
        assertSame(existing, schema.getTableAttr());
    }

    @Test
    void collectPkStoresPrimaryKeysInConfiguredOrder() {
        TestableDuckDbSqlNode node = new TestableDuckDbSqlNode();
        Schema merged = new Schema();
        merged.setTableAttr(new HashMap<>());
        Schema input = buildSchema("pre_1", "users", List.of(orderedPkField("tenant_id", 2), orderedPkField("id", 1), normalField("name")));

        node.callCollectPk(merged, List.of(input));

        Map<String, Object> pkInfo = nestedInfoMap(merged, JoinInfo.PK_INFO);
        Map<String, Object> usersInfo = castMap(pkInfo.get("users"));
        assertEquals(List.of("id", "tenant_id"), usersInfo.get(JoinInfo.PK));
    }

    @Test
    void collectJoinKeyStoresJoinKeyMap() {
        TestableDuckDbSqlNode node = new TestableDuckDbSqlNode();
        Schema merged = new Schema();
        merged.setTableAttr(new HashMap<>());

        JoinField left = new JoinField();
        left.setTable("users");
        left.setField("id");
        JoinField right = new JoinField();
        right.setTable("orders");
        right.setField("user_id");
        JoinKeyPair pair = new JoinKeyPair();
        pair.setLeft(left);
        pair.setRight(right);
        JoinInfo joinInfo = new JoinInfo();
        joinInfo.setTable("orders");
        joinInfo.setJoinKeys(List.of(pair));

        node.callCollectJoinKey(merged, List.of(joinInfo));

        Map<String, Object> joinKeyInfo = nestedInfoMap(merged, JoinInfo.JOIN_KEY_INFO);
        assertTrue(joinKeyInfo.containsKey("orders"));
        assertEquals(List.of("user_id"), JoinInfo.getJoinKeys(merged.getTableAttr(), "orders"));
        assertEquals(List.of("id"), JoinInfo.getJoinKeys(merged.getTableAttr(), "users"));
    }

    @Test
    void iniTableAttrCreatesSeparateMapsUnderSameTableProps() {
        TestableDuckDbSqlNode node = new TestableDuckDbSqlNode();
        Schema merged = new Schema();
        merged.setTableAttr(new HashMap<>());

        Map<String, Object> pkMap = node.callIniTableAttr(merged, JoinInfo.PK_INFO);
        pkMap.put("users", Collections.singletonMap("value", "pk"));
        Map<String, Object> joinMap = node.callIniTableAttr(merged, JoinInfo.JOIN_KEY_INFO);
        joinMap.put("orders", Collections.singletonMap("value", "join"));

        Map<String, Object> tableProps = castMap(merged.getTableAttr().get(JoinInfo.TABLE_PROPS));
        assertSame(pkMap, tableProps.get(JoinInfo.PK_INFO));
        assertSame(joinMap, tableProps.get(JoinInfo.JOIN_KEY_INFO));
    }

    @Test
    void convertSchemasToTapTableDtosReturnsEmptyListForNullInput() throws Exception {
        DuckDbSqlNode node = new DuckDbSqlNode();
        List<TapTableDto> result = invokeConvertSchemasToTapTableDtos(node, null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void convertSchemasToTapTableDtosUsesInjectedSchemaConverter() throws Exception {
        DuckDbSqlNode node = new DuckDbSqlNode();
        TrackingSchemaConverter converter = new TrackingSchemaConverter();
        setSchemaConverter(node, converter);

        List<TapTableDto> result = invokeConvertSchemasToTapTableDtos(node,
                List.of(buildSchema("pre_1", "users", List.of(pkField("id")))));

        assertEquals(1, converter.convertCalls);
        assertEquals(1, result.size());
        assertEquals("tracked", result.get(0).getId());
        assertEquals("tracked", result.get(0).getName());
    }

    private static DuckDbSqlNode newDuckDbSqlNodeWithDag(String nodeId) {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setId(nodeId);
        DAG dag = new DAG(new Graph<>());
        dag.setTaskId(new ObjectId());
        node.setDag(dag);
        return node;
    }

    private static Schema buildSchema(String preNodeId, String tableName, List<Field> fields) {
        Schema schema = new Schema();
        schema.setNodeId(preNodeId);
        schema.setName(tableName);
        schema.setOriginalName(tableName);
        schema.setQualifiedName(tableName);
        schema.setMetaType("collection");
        schema.setSourceType("source");
        schema.setFields(fields);
        return schema;
    }

    private static Field pkField(String name) {
        Field field = new Field();
        field.setFieldName(name);
        field.setPrimaryKey(true);
        field.setPrimaryKeyPosition(1);
        field.setDataType("INT");
        field.setJavaType("Integer");
        field.setOriginalFieldName(name);
        return field;
    }

    private static Field orderedPkField(String name, int position) {
        Field field = pkField(name);
        field.setPrimaryKeyPosition(position);
        return field;
    }

    private static Field normalField(String name) {
        Field field = new Field();
        field.setFieldName(name);
        field.setPrimaryKey(false);
        field.setDataType("VARCHAR");
        field.setJavaType("String");
        field.setOriginalFieldName(name);
        return field;
    }

    private static void invokeResolveMainTableInfo(DuckDbSqlNode node, List<Schema> inputSchemas) throws Exception {
        Method method = DuckDbSqlNode.class.getDeclaredMethod("resolveMainTableInfo", List.class);
        method.setAccessible(true);
        method.invoke(node, inputSchemas);
    }

    private static Schema invokeFindFirstFromTableSchema(DuckDbSqlNode node, List<Schema> inputSchemas) throws Exception {
        Method method = DuckDbSqlNode.class.getDeclaredMethod("findFirstFromTableSchema", List.class);
        method.setAccessible(true);
        return (Schema) method.invoke(node, inputSchemas);
    }

    @SuppressWarnings("unchecked")
    private static List<TapTableDto> invokeConvertSchemasToTapTableDtos(DuckDbSqlNode node, List<Schema> inputSchemas) throws Exception {
        Method method = DuckDbSqlNode.class.getDeclaredMethod("convertSchemasToTapTableDtos", List.class);
        method.setAccessible(true);
        return (List<TapTableDto>) method.invoke(node, inputSchemas);
    }

    private static void setSchemaConverter(DuckDbSqlNode node, TmSchemaConverter converter) throws Exception {
        java.lang.reflect.Field field = DuckDbSqlNode.class.getDeclaredField("schemaConverter");
        field.setAccessible(true);
        field.set(node, converter);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> nestedInfoMap(Schema merged, String key) {
        Map<String, Object> tableProps = castMap(merged.getTableAttr().get(JoinInfo.TABLE_PROPS));
        return castMap(tableProps.get(key));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object value) {
        return (Map<String, Object>) value;
    }

    private static class TestableDuckDbSqlNode extends DuckDbSqlNode {
        void callCollectIndex(Schema schema, List<String> wideTablePkColumns) {
            collectIndex(schema, wideTablePkColumns);
        }

        void callInitTableAttrIfNeed(Schema merged) {
            initTableAttrIfNeed(merged);
        }

        void callCollectPk(Schema merged, List<Schema> inputSchemas) {
            collectPk(merged, inputSchemas);
        }

        void callCollectJoinKey(Schema merged, List<JoinInfo> joinKeyInfo) {
            collectJoinKey(merged, joinKeyInfo);
        }

        Map<String, Object> callIniTableAttr(Schema merged, String key) {
            return iniTableAttr(merged, key);
        }
    }

    private static class TrackingSchemaConverter extends TmSchemaConverter {
        private int convertCalls;

        @Override
        public List<TapTableDto> convert(List<Schema> schemas) {
            convertCalls++;
            return List.of(new TapTableDto().id("tracked").name("tracked"));
        }
    }
}

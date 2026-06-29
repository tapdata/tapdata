package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.util.DuckDbSqlPrimaryKeyAnalyzer;
import io.github.openlg.graphlib.Graph;
import org.junit.jupiter.api.Test;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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

    private static Field normalField(String name) {
        Field field = new Field();
        field.setFieldName(name);
        field.setPrimaryKey(false);
        field.setDataType("VARCHAR");
        field.setJavaType("String");
        field.setOriginalFieldName(name);
        return field;
    }
}

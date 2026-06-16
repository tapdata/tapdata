package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import com.tapdata.tm.commons.util.DuckDbSqlPrimaryKeyAnalyzer;
import org.junit.jupiter.api.Test;

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
        assertEquals(1000, DuckDbSqlNode.DEFAULT_BATCH_SIZE);
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
}

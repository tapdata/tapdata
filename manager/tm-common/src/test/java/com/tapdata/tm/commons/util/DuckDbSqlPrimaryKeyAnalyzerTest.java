package com.tapdata.tm.commons.util;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.parser.SimpleNode;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DuckDbSqlPrimaryKeyAnalyzerTest {

    @Test
    void shouldResolveJoinKeyAliasAsWideTablePrimaryKey() {
        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT o.user_id AS wide_user_id, o.amount " +
                        "FROM users u LEFT JOIN orders o ON u.id = o.user_id"
        );

        assertEquals(List.of("wide_user_id"), primaryKeys);
    }

    @Test
    void shouldPreferMainTableFieldWhenMainAndJoinKeyAreBothSelected() {
        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT o.user_id AS order_user_id, u.id AS user_id " +
                        "FROM users u LEFT JOIN orders o ON u.id = o.user_id"
        );

        assertEquals(List.of("user_id"), primaryKeys);
    }

    @Test
    void shouldResolveCompositePrimaryKeysIncludingFunctionAlias() {
        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT o.tenant_id, upper(o.user_id) AS user_id_upper " +
                        "FROM users u LEFT JOIN orders o " +
                        "ON u.tenant_id = o.tenant_id AND u.id = o.user_id"
        );

        assertEquals(List.of("tenant_id", "user_id_upper"), primaryKeys);
    }

    @Test
    void shouldResolveJoinKeysAcrossMultipleJoinsAndDeduplicate() {
        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT u.id AS uid, p.order_id AS oid, p.order_id AS oid_dup " +
                        "FROM users u " +
                        "JOIN orders o ON u.id = o.user_id " +
                        "JOIN payments p ON o.id = p.order_id"
        );

        assertEquals(List.of("uid", "oid"), primaryKeys);
    }

    @Test
    void shouldSkipJoinPairWhenEitherColumnWasCheckedBefore() {
        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT l.ol_w_id AS warehouse_id, s.s_i_id AS item_id " +
                        "FROM orders o " +
                        "JOIN lineitem l ON o.o_w_id = l.ol_w_id " +
                        "JOIN stock s ON s.s_w_id = o.o_w_id " +
                        "JOIN item i ON s.s_i_id = i.i_id"
        );

        assertEquals(List.of("warehouse_id", "item_id"), primaryKeys);
    }

    @Test
    void shouldReturnEmptyForBlankOrInvalidSql() {
        assertEquals(List.of(), DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(" "));
        assertEquals(List.of(), DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys("SELECT FROM"));
        assertEquals(List.of(), DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys("UPDATE users SET id = 1"));
        assertEquals(List.of(), DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT id FROM users UNION SELECT id FROM orders"
        ));
    }

    @Test
    void shouldReturnEmptyWhenNoJoinExists() {
        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT u.id, u.name FROM users u"
        );

        assertEquals(List.of(), primaryKeys);
    }

    @Test
    void shouldReturnEmptyWhenJoinExistsButProjectionHasNoReferencedColumns() {
        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT 1 AS one FROM users u LEFT JOIN orders o ON u.id = o.user_id"
        );

        assertEquals(List.of(), primaryKeys);
    }

    @Test
    void shouldIgnoreSelectExpressionsWithoutAliasOrDirectColumnName() {
        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT upper(o.user_id), o.amount " +
                        "FROM users u LEFT JOIN orders o ON u.id = o.user_id"
        );

        assertEquals(List.of(), primaryKeys);
    }

    @Test
    void shouldIgnoreJoinConditionWithoutColumnPair() {
        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT o.user_id AS wide_user_id FROM users u LEFT JOIN orders o ON u.id = 1"
        );

        assertEquals(List.of(), primaryKeys);
    }

    @Test
    void shouldIgnoreJoinConditionWithoutTableQualifier() {
        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT user_id AS wide_user_id FROM users u LEFT JOIN orders o ON id = user_id"
        );

        assertEquals(List.of(), primaryKeys);
    }

    @Test
    void shouldHandleParenthesizedJoinConditionAndUnselectedJoinKeys() {
        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT o.amount FROM users u LEFT JOIN orders o ON (u.id = o.user_id AND u.tenant_id = o.tenant_id)"
        );

        assertEquals(List.of(), primaryKeys);
    }

    @Test
    void shouldSkipSelectStarItemsWhenBuildingProjections() {
        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT *, o.user_id AS wide_user_id FROM users u LEFT JOIN orders o ON u.id = o.user_id"
        );

        assertEquals(List.of("wide_user_id"), primaryKeys);
    }

    @Test
    void privateHelpersShouldHandleNullAndEmptyInputs() throws Exception {
        assertEquals(List.of(), invokePrivate("buildSelectProjections", new Class[]{List.class, Map.class}, null, Map.of()));
        assertNull(invokePrivate("findSelectFieldName", new Class[]{List.class, String.class}, null, "users.id"));
        assertNull(invokePrivate("findSelectFieldName", new Class[]{List.class, String.class}, Collections.emptyList(), "users.id"));
        assertNull(invokePrivate("findSelectFieldName", new Class[]{List.class, String.class}, Collections.emptyList(), " "));
        assertEquals(List.of(), invokePrivate("extractJoinKeyPairs", new Class[]{net.sf.jsqlparser.expression.Expression.class, Map.class}, null, Map.of()));
        invokePrivate("collectJoinKeyPairs", new Class[]{net.sf.jsqlparser.expression.Expression.class, Map.class, List.class}, null, Map.of(), new java.util.ArrayList<>());
        assertEquals(Collections.emptySet(), invokePrivate("extractNormalizedColumns", new Class[]{net.sf.jsqlparser.expression.Expression.class, Map.class}, null, Map.of()));
        assertNull(invokePrivate("normalizeColumn", new Class[]{Column.class, Map.class}, null, Map.of()));
        assertNull(invokePrivate("normalizeColumn", new Class[]{Column.class, Map.class}, new Column(" "), Map.of()));
        assertNull(invokePrivate("buildColumnKey", new Class[]{String.class, String.class}, "users", " "));
    }

    @Test
    void privateHelpersShouldHandleNonExpressionSelectItemsAndNoMatchProjection() throws Exception {
        SelectItem customItem = new SelectItem() {
            @Override
            public void accept(SelectItemVisitor selectItemVisitor) {
            }

            @Override
            public SimpleNode getASTNode() {
                return null;
            }

            @Override
            public void setASTNode(SimpleNode simpleNode) {
            }
        };

        assertEquals(List.of(), invokePrivate("buildSelectProjections", new Class[]{List.class, Map.class}, List.of(customItem), Map.of()));

        Object projection = newSelectProjection("id", Collections.emptySet());
        assertNull(invokePrivate("findSelectFieldName", new Class[]{List.class, String.class}, List.of(projection), "users.id"));

        java.util.ArrayList<Object> pairs = new java.util.ArrayList<>();
        invokePrivate("collectJoinKeyPairs", new Class[]{net.sf.jsqlparser.expression.Expression.class, Map.class, List.class},
                new EqualsTo(new LongValue(1), new LongValue(1)), Map.of(), pairs);
        assertTrue(pairs.isEmpty());
    }

    @Test
    void privateNormalizeHelpersShouldHandleBlankTables() throws Exception {
        assertNull(invokePrivate("normalizeColumn", new Class[]{Column.class, Map.class}, new Column("id"), Map.of()));
        assertNull(invokePrivate("normalizeTableName", new Class[]{String.class, Map.class}, " ", Map.of()));
        assertEquals("users.id", invokePrivate("buildColumnKey", new Class[]{String.class, String.class}, " Users ", " ID "));
    }

    private static Object invokePrivate(String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = DuckDbSqlPrimaryKeyAnalyzer.class.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(null, args);
    }

    private static Object newSelectProjection(String outputFieldName, java.util.Set<String> referencedColumnKeys) throws Exception {
        Class<?> cls = Class.forName(DuckDbSqlPrimaryKeyAnalyzer.class.getName() + "$SelectProjection");
        Constructor<?> constructor = cls.getDeclaredConstructor(String.class, java.util.Set.class);
        constructor.setAccessible(true);
        return constructor.newInstance(outputFieldName, referencedColumnKeys);
    }
}

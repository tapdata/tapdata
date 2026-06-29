package com.tapdata.tm.commons.util;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
    }

    @Test
    void shouldReturnEmptyWhenNoJoinExists() {
        List<String> primaryKeys = DuckDbSqlPrimaryKeyAnalyzer.analyzePrimaryKeys(
                "SELECT u.id, u.name FROM users u"
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
}

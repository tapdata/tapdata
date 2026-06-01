package com.tapdata.tm.commons.dag.process;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DuckDbSqlNodeTest {

    @Test
    void duckDbSqlNodeHasStableType() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        assertEquals("duckdb_sql_processor", node.getType());
    }

}

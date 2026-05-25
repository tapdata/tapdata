package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FromTableConfigTest {

    @Test
    void testDefaultConstructor() {
        FromTableConfig config = new FromTableConfig();
        assertNull(config.getTableName());
        assertNull(config.getPrimaryKey());
    }

    @Test
    void testParameterizedConstructor() {
        FromTableConfig config = new FromTableConfig("test_table", "id");
        assertEquals("test_table", config.getTableName());
        assertEquals("id", config.getPrimaryKey());
    }

    @Test
    void testSetTableName() {
        FromTableConfig config = new FromTableConfig();
        config.setTableName("new_table");
        assertEquals("new_table", config.getTableName());
    }

    @Test
    void testSetPrimaryKey() {
        FromTableConfig config = new FromTableConfig();
        config.setPrimaryKey("primary_key");
        assertEquals("primary_key", config.getPrimaryKey());
    }

    @Test
    void testNullTableName() {
        FromTableConfig config = new FromTableConfig(null, "id");
        assertNull(config.getTableName());
    }

    @Test
    void testNullPrimaryKey() {
        FromTableConfig config = new FromTableConfig("test_table", null);
        assertNull(config.getPrimaryKey());
    }

    @Test
    void testEmptyTableName() {
        FromTableConfig config = new FromTableConfig("", "id");
        assertEquals("", config.getTableName());
    }

    @Test
    void testEmptyPrimaryKey() {
        FromTableConfig config = new FromTableConfig("test_table", "");
        assertEquals("", config.getPrimaryKey());
    }
}

package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FromTableConfigTest {

    @Test
    void testConstructorWithPreNodeIdAndTableNameInSql() {
        FromTableConfig config = new FromTableConfig("node_mysql_1", "t1");
        
        assertEquals("node_mysql_1", config.getPreNodeId());
        assertEquals("t1", config.getTableNameInSql());
    }

    @Test
    void testDefaultConstructor() {
        FromTableConfig config = new FromTableConfig();
        
        assertNull(config.getPreNodeId());
        assertNull(config.getTableNameInSql());
    }

    @Test
    void testSettersAndGetters() {
        FromTableConfig config = new FromTableConfig();
        
        config.setPreNodeId("node_pg_1");
        config.setTableNameInSql("users_alias");
        
        assertEquals("node_pg_1", config.getPreNodeId());
        assertEquals("users_alias", config.getTableNameInSql());
    }

    @Test
    void testRejectBlankPreNodeId() {
        assertThrows(IllegalArgumentException.class, () -> {
            new FromTableConfig("", "t1");
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            new FromTableConfig("   ", "t1");
        });
    }

    @Test
    void testRejectBlankTableNameInSql() {
        assertThrows(IllegalArgumentException.class, () -> {
            new FromTableConfig("node_1", "");
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            new FromTableConfig("node_1", "   ");
        });
    }

    @Test
    void testValidComplexAliases() {
        FromTableConfig config1 = new FromTableConfig("node_1", "t1");
        FromTableConfig config2 = new FromTableConfig("node_2", "order_table");
        FromTableConfig config3 = new FromTableConfig("node_3", "_private_table");
        
        assertEquals("t1", config1.getTableNameInSql());
        assertEquals("order_table", config2.getTableNameInSql());
        assertEquals("_private_table", config3.getTableNameInSql());
    }
}

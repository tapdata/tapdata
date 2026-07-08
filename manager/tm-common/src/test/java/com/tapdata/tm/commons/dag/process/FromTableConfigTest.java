package com.tapdata.tm.commons.dag.process;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FromTableConfigTest {

    @Test
    void defaultConstructorAndSettersShouldWork() {
        FromTableConfig config = new FromTableConfig();

        config.setPreNodeId("pre");
        config.setTableNameInSql("orders");

        Assertions.assertEquals("pre", config.getPreNodeId());
        Assertions.assertEquals("orders", config.getTableNameInSql());
    }

    @Test
    void constructorShouldRejectBlankPreNodeId() {
        IllegalArgumentException nullEx = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new FromTableConfig(null, "orders"));
        IllegalArgumentException blankEx = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new FromTableConfig("  ", "orders"));

        Assertions.assertEquals("preNodeId must not be blank", nullEx.getMessage());
        Assertions.assertEquals("preNodeId must not be blank", blankEx.getMessage());
    }

    @Test
    void constructorShouldRejectBlankTableNameInSql() {
        IllegalArgumentException nullEx = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new FromTableConfig("pre", null));
        IllegalArgumentException blankEx = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new FromTableConfig("pre", "  "));

        Assertions.assertEquals("tableNameInSql must not be blank", nullEx.getMessage());
        Assertions.assertEquals("tableNameInSql must not be blank", blankEx.getMessage());
    }

    @Test
    void constructorShouldSetFields() {
        FromTableConfig config = new FromTableConfig("pre", "orders");

        Assertions.assertEquals("pre", config.getPreNodeId());
        Assertions.assertEquals("orders", config.getTableNameInSql());
    }
}

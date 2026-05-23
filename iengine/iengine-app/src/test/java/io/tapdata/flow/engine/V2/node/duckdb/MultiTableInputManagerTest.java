package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MultiTableInputManagerTest {
    @Test
    void multiTableInputManagerRegistersSchema() {
        MultiTableInputManager manager = new MultiTableInputManager();
        Map<String, Object> schema = Map.of("id", "int", "name", "string");
        manager.registerTable("users", schema);
        assertNotNull(manager.getTableMetadata("users"));
        assertEquals(schema, manager.getTableMetadata("users"));
    }
}

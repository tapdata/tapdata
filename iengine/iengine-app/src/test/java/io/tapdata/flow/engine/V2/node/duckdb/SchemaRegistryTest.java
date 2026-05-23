package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SchemaRegistryTest {
    @Test
    void schemaRegistryStoresAndRetrievesSchema() {
        SchemaRegistry registry = new SchemaRegistry();
        Map<String, Object> schema = Map.of("field1", "type1", "field2", "type2");
        registry.registerSchema("table_1", schema);
        assertEquals(schema, registry.getSchema("table_1"));
    }
}

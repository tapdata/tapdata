package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.HashMap;
import java.util.Map;

public class SchemaRegistry {
    private final Map<String, Map<String, Object>> schemas = new HashMap<>();
    
    public void registerSchema(String tableId, Map<String, Object> schema) {
        schemas.put(tableId, schema);
    }
    
    public Map<String, Object> getSchema(String tableId) {
        return schemas.get(tableId);
    }
}

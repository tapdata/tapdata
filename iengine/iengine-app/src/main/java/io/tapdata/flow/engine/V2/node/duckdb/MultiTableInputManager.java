package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.HashMap;
import java.util.Map;

public class MultiTableInputManager {
    private final Map<String, Object> tableMetadata = new HashMap<>();
    
    public void registerTable(String tableName, Map<String, Object> schema) {
        tableMetadata.put(tableName, schema);
    }
    
    public Map<String, Object> getTableMetadata(String tableName) {
        return (Map<String, Object>) tableMetadata.get(tableName);
    }
}

package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SyncStageTracker {
    private final Map<String, Boolean> tableStageMap = new ConcurrentHashMap<>();
    
    public void updateTableStage(String tableName, boolean incremental) {
        tableStageMap.put(tableName, incremental);
    }
    
    public boolean allTablesIncremental() {
        return !tableStageMap.isEmpty() && tableStageMap.values().stream().allMatch(Boolean::booleanValue);
    }
}

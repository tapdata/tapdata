package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OutputBuffer {
    private final int batchSize;
    private final List<Map<String, Object>> buffer = new ArrayList<>();
    
    public OutputBuffer(int batchSize) {
        this.batchSize = batchSize;
    }
    
    public void addResult(Map<String, Object> record) {
        buffer.add(record);
    }
    
    public boolean isReadyToEmit() {
        return buffer.size() >= batchSize;
    }
    
    public List<Map<String, Object>> flushBatch() {
        List<Map<String, Object>> copy = new ArrayList<>(buffer);
        buffer.clear();
        return copy;
    }
}

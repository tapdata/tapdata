package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.Map;

public class ErrorHandler {
    private int errorCount = 0;
    private final int maxErrorCount;
    private final double maxErrorRate;
    
    public ErrorHandler(int maxErrorCount, double maxErrorRate) {
        this.maxErrorCount = maxErrorCount;
        this.maxErrorRate = maxErrorRate;
    }
    
    public void recordError(Map<String, Object> sourceData, Exception error) {
        errorCount++;
    }
    
    public boolean shouldStopTask() {
        return errorCount >= maxErrorCount;
    }
}

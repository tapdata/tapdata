package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.Map;

public class ErrorHandler {
    private int errorCount = 0;
    private int totalEvents = 0;
    private final int maxErrorCount;
    private final double maxErrorRate;
    
    public ErrorHandler(int maxErrorCount, double maxErrorRate) {
        this.maxErrorCount = maxErrorCount;
        this.maxErrorRate = maxErrorRate;
    }
    
    public void recordError(Map<String, Object> sourceData, Exception error) {
        errorCount++;
    }
    
    public void recordEvent() {
        totalEvents++;
    }
    
    public boolean shouldStopTask() {
        // Stop if absolute error count threshold is reached
        if (errorCount >= maxErrorCount) {
            return true;
        }
        
        // Stop if error rate exceeds threshold
        if (totalEvents > 0 && (double) errorCount / totalEvents > maxErrorRate) {
            return true;
        }
        
        return false;
    }
}

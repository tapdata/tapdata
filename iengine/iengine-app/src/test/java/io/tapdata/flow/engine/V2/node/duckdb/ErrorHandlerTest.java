package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ErrorHandlerTest {
    @Test
    void errorHandlerRecordsErrors() {
        ErrorHandler handler = new ErrorHandler(5, 0.1);
        Map<String, Object> sourceData = Map.of("id", 1);
        handler.recordError(sourceData, new Exception("Test error"));
        assertFalse(handler.shouldStopTask());
    }

    @Test
    void errorHandlerStopsWhenThresholdExceeded() {
        ErrorHandler handler = new ErrorHandler(2, 0.1);
        Map<String, Object> sourceData = Map.of("id", 1);
        handler.recordError(sourceData, new Exception("Error 1"));
        handler.recordError(sourceData, new Exception("Error 2"));
        assertTrue(handler.shouldStopTask());
    }
}

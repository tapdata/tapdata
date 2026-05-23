package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OutputBufferTest {
    @Test
    void outputBufferFlushesWhenReady() {
        OutputBuffer buffer = new OutputBuffer(2);
        buffer.addResult(Map.of("id", 1));
        assertFalse(buffer.isReadyToEmit());
        buffer.addResult(Map.of("id", 2));
        assertTrue(buffer.isReadyToEmit());
    }
}

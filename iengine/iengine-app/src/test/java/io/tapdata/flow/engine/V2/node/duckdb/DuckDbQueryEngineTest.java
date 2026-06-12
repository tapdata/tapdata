package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DuckDbQueryEngineTest {
    @Test
    void queryEngineTimesOutLongQueries() {
        DuckDbQueryEngine engine = new DuckDbQueryEngine(512, 10);
        assertThrows(Exception.class, () -> engine.executeQueryWithTimeout("SELECT 1", 10));
    }
}

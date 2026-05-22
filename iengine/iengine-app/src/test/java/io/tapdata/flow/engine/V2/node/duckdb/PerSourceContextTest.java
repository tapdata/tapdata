package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class PerSourceContextTest {

    @Test
    void constructorInitializesCoreState() {
        DuckDbOperator operator = mock(DuckDbOperator.class);

        PerSourceContext context = new PerSourceContext("sourceA:tableA", operator);

        assertEquals("sourceA:tableA", context.getKey());
        assertSame(operator, context.getOperator());
        assertEquals(1000, context.getBatchSize());
        assertFalse(context.isTableInitialized());
    }

    @Test
    void addRecordTracksBufferAndCount() {
        PerSourceContext context = new PerSourceContext("sourceA:tableA", mock(DuckDbOperator.class));
        Map<String, Object> record = new HashMap<>();
        record.put("id", 1);

        context.addRecord(record);

        assertEquals(1, context.getAccumulatedRecordCount().get());
        assertEquals(1, context.getBatchBuffer().size());
        assertSame(record, context.getBatchBuffer().get(0));
    }

    @Test
    void drainBufferReturnsAndClearsRecords() {
        PerSourceContext context = new PerSourceContext("sourceA:tableA", mock(DuckDbOperator.class));
        Map<String, Object> first = new HashMap<>();
        first.put("id", 1);
        Map<String, Object> second = new HashMap<>();
        second.put("id", 2);
        context.addRecord(first);
        context.addRecord(second);

        List<Map<String, Object>> drained = context.drainBuffer();

        assertEquals(2, drained.size());
        assertEquals(first, drained.get(0));
        assertEquals(second, drained.get(1));
        assertTrue(context.getBatchBuffer().isEmpty());
        assertEquals(0, context.getAccumulatedRecordCount().get());
    }

    @Test
    void tableInitializedCanBeToggled() {
        PerSourceContext context = new PerSourceContext("sourceA:tableA", mock(DuckDbOperator.class));

        context.setTableInitialized(true);

        assertTrue(context.isTableInitialized());
    }
}

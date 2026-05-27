package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
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
    void addEventTracksBufferAndCount() {
        PerSourceContext context = new PerSourceContext("sourceA:tableA", mock(DuckDbOperator.class));
        TapdataEvent event = createMockTapdataEvent();

        context.addEvent(event);

        assertEquals(1, context.getAccumulatedRecordCount().get());
        assertEquals(1, context.getBatchBuffer().size());
        assertSame(event, context.getBatchBuffer().get(0));
    }

    @Test
    void drainBufferReturnsAndClearsEvents() {
        PerSourceContext context = new PerSourceContext("sourceA:tableA", mock(DuckDbOperator.class));
        TapdataEvent first = createMockTapdataEvent();
        TapdataEvent second = createMockTapdataEvent();
        context.addEvent(first);
        context.addEvent(second);

        List<TapdataEvent> drained = context.drainBuffer();

        assertEquals(2, drained.size());
        assertSame(first, drained.get(0));
        assertSame(second, drained.get(1));
        assertTrue(context.getBatchBuffer().isEmpty());
        assertEquals(0, context.getAccumulatedRecordCount().get());
    }

    @Test
    void tableInitializedCanBeToggled() {
        PerSourceContext context = new PerSourceContext("sourceA:tableA", mock(DuckDbOperator.class));

        context.setTableInitialized(true);

        assertTrue(context.isTableInitialized());
    }

    private TapdataEvent createMockTapdataEvent() {
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapInsertRecordEvent insertEvent = new TapInsertRecordEvent();
        insertEvent.setTableId("test_table");
        Map<String, Object> after = new HashMap<>();
        after.put("id", 1);
        insertEvent.setAfter(after);
        tapdataEvent.setTapEvent(insertEvent);
        return tapdataEvent;
    }
}

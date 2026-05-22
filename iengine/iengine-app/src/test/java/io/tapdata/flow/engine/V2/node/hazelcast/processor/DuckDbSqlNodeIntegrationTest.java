package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperator;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class DuckDbSqlNodeIntegrationTest {

    @Test
    public void testFlushUsesSmartMergerAndCallsUpsertBatch() throws Exception {
        ProcessorBaseContext ctx = mock(ProcessorBaseContext.class);
        DuckDbSqlNode node = new DuckDbSqlNode(ctx);

        // set currentTableName
        Field tableField = DuckDbSqlNode.class.getDeclaredField("currentTableName");
        tableField.setAccessible(true);
        tableField.set(node, "test_table");

        // mock operator and set to node
        DuckDbOperator operator = mock(DuckDbOperator.class);
        Field opField = DuckDbSqlNode.class.getDeclaredField("duckDbOperator");
        opField.setAccessible(true);
        opField.set(node, operator);

        // access batchBuffer and add events that should be merged (same id, last wins)
        Field bufferField = DuckDbSqlNode.class.getDeclaredField("batchBuffer");
        bufferField.setAccessible(true);
        List<Map<String, Object>> buffer = (List<Map<String, Object>>) bufferField.get(node);

        Map<String, Object> a1 = new HashMap<>();
        a1.put("id", 1);
        a1.put("val", "first");

        Map<String, Object> a2 = new HashMap<>();
        a2.put("id", 1);
        a2.put("val", "second");

        buffer.add(a1);
        buffer.add(a2);

        // call private flushBatch
        Method flush = DuckDbSqlNode.class.getDeclaredMethod("flushBatch");
        flush.setAccessible(true);
        flush.invoke(node);

        // merged should be last-wins -> only a2 present
        // verify upsertBatch called with merged data containing only a2
        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> expected = Collections.singletonList(a2);
        verify(operator, times(1)).upsertBatch(eq("test_table"), argThat(list -> list != null && list.size() == 1 && list.get(0).get("val").equals("second")));

        // buffer should be empty after successful flush
        assertTrue(buffer.isEmpty());
    }

    @Test
    public void testFlushRestoresBufferOnOperatorFailure() throws Exception {
        ProcessorBaseContext ctx = mock(ProcessorBaseContext.class);
        DuckDbSqlNode node = new DuckDbSqlNode(ctx);

        Field tableField = DuckDbSqlNode.class.getDeclaredField("currentTableName");
        tableField.setAccessible(true);
        tableField.set(node, "test_table");

        // mock operator to throw
        DuckDbOperator operator = mock(DuckDbOperator.class);
        doThrow(new RuntimeException("boom")).when(operator).upsertBatch(anyString(), anyList());
        Field opField = DuckDbSqlNode.class.getDeclaredField("duckDbOperator");
        opField.setAccessible(true);
        opField.set(node, operator);

        Field bufferField = DuckDbSqlNode.class.getDeclaredField("batchBuffer");
        bufferField.setAccessible(true);
        List<Map<String, Object>> buffer = (List<Map<String, Object>>) bufferField.get(node);

        Map<String, Object> r1 = new HashMap<>();
        r1.put("id", 2);
        r1.put("val", "one");

        Map<String, Object> r2 = new HashMap<>();
        r2.put("id", 3);
        r2.put("val", "two");

        buffer.add(r1);
        buffer.add(r2);

        Method flush = DuckDbSqlNode.class.getDeclaredMethod("flushBatch");
        flush.setAccessible(true);
        flush.invoke(node);

        // operator should have been called and thrown
        verify(operator, times(1)).upsertBatch(eq("test_table"), anyList());

        // buffer should have been restored to original content (in same order)
        assertEquals(2, buffer.size());
        assertEquals(r1, buffer.get(0));
        assertEquals(r2, buffer.get(1));
    }
}

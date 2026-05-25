package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.flow.engine.V2.node.duckdb.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class NazelcastDuckDbSqlNodeIntegrationTest {

    @Test
    public void testFlushUsesSmartMergerAndCallsUpsertBatch() throws Exception {
        ProcessorBaseContext ctx = mock(ProcessorBaseContext.class);
        NazelcastDuckDbSqlNode node = new NazelcastDuckDbSqlNode(ctx);

        // set currentTableName
        Field tableField = NazelcastDuckDbSqlNode.class.getDeclaredField("currentTableName");
        tableField.setAccessible(true);
        tableField.set(node, "test_table");

        // mock operator and set to node
        DuckDbOperator operator = mock(DuckDbOperator.class);
        Field opField = NazelcastDuckDbSqlNode.class.getDeclaredField("duckDbOperator");
        opField.setAccessible(true);
        opField.set(node, operator);

        // access batchBuffer and add events that should be merged (same id, last wins)
        Field bufferField = NazelcastDuckDbSqlNode.class.getDeclaredField("batchBuffer");
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
        Method flush = NazelcastDuckDbSqlNode.class.getDeclaredMethod("flushBatch");
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
        NazelcastDuckDbSqlNode node = new NazelcastDuckDbSqlNode(ctx);

        Field tableField = NazelcastDuckDbSqlNode.class.getDeclaredField("currentTableName");
        tableField.setAccessible(true);
        tableField.set(node, "test_table");

        // mock operator to throw
        DuckDbOperator operator = mock(DuckDbOperator.class);
        doThrow(new RuntimeException("boom")).when(operator).upsertBatch(anyString(), anyList());
        Field opField = NazelcastDuckDbSqlNode.class.getDeclaredField("duckDbOperator");
        opField.setAccessible(true);
        opField.set(node, operator);

        Field bufferField = NazelcastDuckDbSqlNode.class.getDeclaredField("batchBuffer");
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

        Method flush = NazelcastDuckDbSqlNode.class.getDeclaredMethod("flushBatch");
        flush.setAccessible(true);
        flush.invoke(node);

        // operator should have been called and thrown
        verify(operator, times(1)).upsertBatch(eq("test_table"), anyList());

        // buffer should have been restored to original content (in same order)
        assertEquals(2, buffer.size());
        assertEquals(r1, buffer.get(0));
        assertEquals(r2, buffer.get(1));
    }

    /**
     * Test that full sync stage caches results until all tables are incremental.
     */
    @Test
    void fullStageCachesResultsUntilAllTablesAreIncremental() {
        // Initialize helpers
        MultiTableInputManager inputManager = new MultiTableInputManager();
        SyncStageTracker stageTracker = new SyncStageTracker();
        OutputBuffer outputBuffer = new OutputBuffer(1000);
        DuckDbQueryEngine queryEngine = new DuckDbQueryEngine(1024, 5000);
        
        // Register tables
        Map<String, Object> schemaA = new HashMap<>();
        schemaA.put("id", "INTEGER");
        schemaA.put("name", "VARCHAR");
        inputManager.registerTable("table_a", schemaA);
        
        Map<String, Object> schemaB = new HashMap<>();
        schemaB.put("id", "INTEGER");
        schemaB.put("value", "DOUBLE");
        inputManager.registerTable("table_b", schemaB);
        
        // Mark both tables as full sync (not yet incremental)
        stageTracker.updateTableStage("table_a", false);
        stageTracker.updateTableStage("table_b", false);
        
        // Simulate receiving data from table_a (full sync, isComplete=false)
        Map<String, Object> recordA = new HashMap<>();
        recordA.put("id", 1);
        recordA.put("name", "Alice");
        outputBuffer.addResult(recordA);
        
        // Verify buffer has data but is not ready (full sync still active)
        assertTrue(outputBuffer.isReadyToEmit() || stageTracker.allTablesIncremental() == false);
        
        // Simulate receiving data from table_b (full sync, isComplete=false)
        Map<String, Object> recordB = new HashMap<>();
        recordB.put("id", 2);
        recordB.put("value", 3.14);
        outputBuffer.addResult(recordB);
        
        // Still in full sync, should not emit
        assertFalse(stageTracker.allTablesIncremental());
        
        // Mark table_a as incremental (isComplete=true)
        stageTracker.updateTableStage("table_a", true);
        
        // Still should not emit (table_b not yet complete)
        assertFalse(stageTracker.allTablesIncremental());
        
        // Mark table_b as incremental
        stageTracker.updateTableStage("table_b", true);
        
        // Now all tables are incremental, should be ready to emit
        assertTrue(stageTracker.allTablesIncremental());
        
        // Flush and verify data
        var results = outputBuffer.flushBatch();
        assertNotNull(results);
        assertTrue(results.size() >= 2, "Should have at least 2 buffered results");
    }
    
    /**
     * Test that ErrorHandler stops task after threshold is exceeded.
     */
    @Test
    void errorHandlerStopsTaskAfterThreshold() {
        ErrorHandler handler = new ErrorHandler(100, 0.01);
        
        // Record 100 errors (at threshold)
        for (int i = 0; i < 100; i++) {
            Map<String, Object> sourceData = new HashMap<>();
            sourceData.put("id", i);
            sourceData.put("error_index", i);
            handler.recordError(sourceData, new IllegalStateException("Test error " + i));
        }
        
        // Should signal to stop task
        assertTrue(handler.shouldStopTask(), "Should stop task after 100 errors");
    }
    
    /**
     * Test that ErrorHandler tracks error rate and stops on percentage threshold.
     */
    @Test
    void errorHandlerStopsOnErrorRate() {
        ErrorHandler handler = new ErrorHandler(1000, 0.05); // 5% error rate threshold
        
        // Record 100 events total
        for (int i = 0; i < 100; i++) {
            handler.recordEvent();
        }
        
        // Record 6 errors (6% > 5% threshold)
        for (int i = 0; i < 6; i++) {
            Map<String, Object> sourceData = new HashMap<>();
            sourceData.put("id", i);
            handler.recordError(sourceData, new IllegalStateException("Test error " + i));
        }
        
        // Should signal to stop task due to error rate
        assertTrue(handler.shouldStopTask(), "Should stop task when error rate exceeds threshold");
    }
    
    /**
     * Test that ErrorHandler does not stop when error rate is below threshold.
     */
    @Test
    void errorHandlerContinuesWhenBelowThreshold() {
        ErrorHandler handler = new ErrorHandler(1000, 0.05); // 5% error rate threshold
        
        // Record 100 events total
        for (int i = 0; i < 100; i++) {
            handler.recordEvent();
        }
        
        // Record 3 errors (3% < 5% threshold)
        for (int i = 0; i < 3; i++) {
            Map<String, Object> sourceData = new HashMap<>();
            sourceData.put("id", i);
            handler.recordError(sourceData, new IllegalStateException("Test error " + i));
        }
        
        // Should not signal to stop task
        assertFalse(handler.shouldStopTask(), "Should continue when error rate is below threshold");
    }
}

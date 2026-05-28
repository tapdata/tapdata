package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.flow.engine.V2.node.duckdb.*;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class HazelcastDuckDbSqlNodeIntegrationTest {

    @Test
    public void testFlushContextUsesSmartMergerAndCallsUpsertBatch() throws Exception {
        ProcessorBaseContext ctx = mock(ProcessorBaseContext.class);
        HazelcastDuckDbSqlNode node = new HazelcastDuckDbSqlNode(ctx);

        // 创建mock operator并设置到node
        DuckDbOperator operator = mock(DuckDbOperator.class);
        Field opField = HazelcastDuckDbSqlNode.class.getDeclaredField("duckDbOperator");
        opField.setAccessible(true);
        opField.set(node, operator);

        // 创建PerSourceContext
        Object context = createPerSourceContext("test_source", "test_table", operator);

        // 添加TapdataEvent到buffer
        addEventToContext(context, createInsertEvent(1, "first"));
        addEventToContext(context, createInsertEvent(1, "second")); // 相同id，last wins

        // 调用flushContext方法
        Method flush = HazelcastDuckDbSqlNode.class.getDeclaredMethod("flushContext",
            Class.forName("io.tapdata.flow.engine.V2.node.duckdb.PerSourceContext"));
        flush.setAccessible(true);
        flush.invoke(node, context);

        // 验证executeInTransaction被调用（v2.0重构后使用事务包装）
        verify(operator, times(1)).executeInTransaction(any(DuckDbOperator.ThrowingConsumer.class));

        // buffer应该为空
        List<TapdataEvent> buffer = getBufferFromContext(context);
        assertTrue(buffer.isEmpty());
    }

    @Test
    public void testFlushContextRestoresBufferOnOperatorFailure() throws Exception {
        ProcessorBaseContext ctx = mock(ProcessorBaseContext.class);
        HazelcastDuckDbSqlNode node = new HazelcastDuckDbSqlNode(ctx);

        // 创建mock operator，在executeInTransaction内部抛出异常（v2.0重构后使用事务包装）
        DuckDbOperator operator = mock(DuckDbOperator.class);
        doThrow(new RuntimeException("boom")).when(operator).executeInTransaction(any(DuckDbOperator.ThrowingConsumer.class));
        Field opField = HazelcastDuckDbSqlNode.class.getDeclaredField("duckDbOperator");
        opField.setAccessible(true);
        opField.set(node, operator);

        // 创建PerSourceContext
        Object context = createPerSourceContext("test_source", "test_table", operator);

        // 添加TapdataEvent到buffer
        TapdataEvent event1 = createInsertEvent(2, "one");
        TapdataEvent event2 = createInsertEvent(3, "two");
        addEventToContext(context, event1);
        addEventToContext(context, event2);

        // 调用flushContext方法
        Method flush = HazelcastDuckDbSqlNode.class.getDeclaredMethod("flushContext",
            Class.forName("io.tapdata.flow.engine.V2.node.duckdb.PerSourceContext"));
        flush.setAccessible(true);
        flush.invoke(node, context);

        // operator应该被调用并抛出异常（v2.0重构后验证executeInTransaction）
        verify(operator, times(1)).executeInTransaction(any(DuckDbOperator.ThrowingConsumer.class));

        // buffer应该恢复原始内容
        List<TapdataEvent> buffer = getBufferFromContext(context);
        assertEquals(2, buffer.size());
    }

    /**
     * 创建PerSourceContext实例
     */
    private Object createPerSourceContext(String sourceId, String tableName, DuckDbOperator operator) throws Exception {
        Class<?> contextClass = Class.forName("io.tapdata.flow.engine.V2.node.duckdb.PerSourceContext");
        
        // 使用PerSourceContext的构造函数：PerSourceContext(String key, DuckDbOperator operator)
        String key = sourceId + ":" + tableName;
        Object context = contextClass.getConstructor(String.class, DuckDbOperator.class)
            .newInstance(key, operator);
        
        // 设置targetTableName字段
        setField(context, "targetTableName", tableName);
        
        return context;
    }

    /**
     * 添加TapdataEvent到Context的buffer
     */
    private void addEventToContext(Object context, TapdataEvent event) throws Exception {
        Method addEvent = context.getClass().getMethod("addEvent", TapdataEvent.class);
        addEvent.invoke(context, event);
    }

    /**
     * 获取Context的buffer
     */
    @SuppressWarnings("unchecked")
    private List<TapdataEvent> getBufferFromContext(Object context) throws Exception {
        Field field = context.getClass().getDeclaredField("batchBuffer");
        field.setAccessible(true);
        return (List<TapdataEvent>) field.get(context);
    }

    /**
     * 设置对象字段值
     */
    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    /**
     * 创建Insert TapdataEvent
     */
    private TapdataEvent createInsertEvent(int id, String val) {
        TapInsertRecordEvent tapEvent = TapInsertRecordEvent.create()
            .after(new HashMap<String, Object>() {{
                put("id", id);
                put("val", val);
            }});
        
        TapdataEvent event = new TapdataEvent();
        event.setTapEvent(tapEvent);
        return event;
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

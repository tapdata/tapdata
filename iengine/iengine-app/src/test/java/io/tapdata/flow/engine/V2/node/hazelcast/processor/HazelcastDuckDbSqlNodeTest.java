package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.flow.engine.V2.node.duckdb.PerSourceContext;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * DuckDbSqlNode 单元测试
 */
class HazelcastDuckDbSqlNodeTest {

    @Mock
    private ProcessorBaseContext processorBaseContext;

    @Mock
    private TaskDto taskDto;

    @Mock
    private Node node;

    private HazelcastDuckDbSqlNode hazelcastDuckDbSqlNode;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
        when(processorBaseContext.getNode()).thenReturn(node);
        when(node.getId()).thenReturn("test_node");
        when(node.getName()).thenReturn("TestNode");
        when(taskDto.isNormalTask()).thenReturn(true);
        
        hazelcastDuckDbSqlNode = new HazelcastDuckDbSqlNode(processorBaseContext);
        hazelcastDuckDbSqlNode.setBatchSize(10);
    }

    @Test
    void testBatchSize_SetAndGet() {
        hazelcastDuckDbSqlNode.setBatchSize(100);
        assertEquals(100, hazelcastDuckDbSqlNode.getBatchSize());
    }

    @Test
    void testExecuteQuery_WithOperator() throws SQLException {
        DuckDbOperator mockOperator = mock(DuckDbOperator.class);
        List<Map<String, Object>> expected = List.of(new HashMap<>());
        when(mockOperator.executeQuery(anyString())).thenReturn(expected);
        
        // 使用反射设置 duckDbOperator
        setDuckDbOperator(mockOperator);
        
        List<Map<String, Object>> result = hazelcastDuckDbSqlNode.executeQuery("SELECT * FROM test");
        
        assertNotNull(result);
        assertEquals(expected, result);
    }

    @Test
    void testExecuteUpdate_WithOperator() throws SQLException {
        DuckDbOperator mockOperator = mock(DuckDbOperator.class);
        when(mockOperator.executeUpdate(anyString())).thenReturn(10);
        
        // 使用反射设置 duckDbOperator
        setDuckDbOperator(mockOperator);
        
        int result = hazelcastDuckDbSqlNode.executeUpdate("DELETE FROM test");
        
        assertEquals(10, result);
    }

    @Test
    void testInitialSyncUsesInsertBatch() throws Exception {
        DuckDbOperator mockOperator = mock(DuckDbOperator.class);
        doAnswer(invocation -> {
            DuckDbOperator.ThrowingConsumer action = invocation.getArgument(0);
            action.accept();
            return null;
        }).when(mockOperator).executeInTransaction(any());
        doNothing().when(mockOperator).insertBatch(anyString(), anyList());
        setDuckDbOperator(mockOperator);

        java.lang.reflect.Method method = HazelcastDuckDbSqlNode.class.getDeclaredMethod(
                "processInitialSyncStage",
                PerSourceContext.class,
                List.class
        );
        method.setAccessible(true);

        PerSourceContext context = new PerSourceContext("ctx", mockOperator);
        context.setTargetTableName("target_table");
        context.setTableInitialized(true);
        List<TapdataEvent> events = new ArrayList<>();
        events.add(createInsertEvent("target_table", Map.of("id", 1L, "name", "Alice")));

        method.invoke(hazelcastDuckDbSqlNode, context, events);

        verify(mockOperator).insertBatch(eq("target_table"), anyList());
        verify(mockOperator, never()).executeUpdate(contains("DELETE FROM"));
    }

    @Test
    void testTryProcess_NullEvent() {
        AtomicInteger consumerCallCount = new AtomicInteger(0);
        
        assertDoesNotThrow(() -> 
            hazelcastDuckDbSqlNode.tryProcess(null, (event, result) -> consumerCallCount.incrementAndGet())
        );
        
        assertEquals(0, consumerCallCount.get());
    }

    @Test
    void testTryProcess_WithNonRecordEvent() {
        TapEvent nonRecordEvent = mock(TapEvent.class);
        TapdataEvent tapdataEvent = new TapdataEvent();
        tapdataEvent.setTapEvent(nonRecordEvent);
        
        AtomicInteger consumerCallCount = new AtomicInteger(0);
        
        assertDoesNotThrow(() -> 
            hazelcastDuckDbSqlNode.tryProcess(tapdataEvent, (event, result) -> consumerCallCount.incrementAndGet())
        );
        
        assertEquals(1, consumerCallCount.get());
    }

    /**
     * 使用反射设置 duckDbOperator
     */
    private void setDuckDbOperator(DuckDbOperator operator) {
        try {
            java.lang.reflect.Field opField = HazelcastDuckDbSqlNode.class.getDeclaredField("duckDbOperator");
            opField.setAccessible(true);
            opField.set(hazelcastDuckDbSqlNode, operator);
        } catch (Exception e) {
            fail("Failed to set duckDbOperator field: " + e.getMessage());
        }
    }

    private TapdataEvent createInsertEvent(String tableName, Map<String, Object> after) {
        TapInsertRecordEvent insertRecordEvent = new TapInsertRecordEvent();
        insertRecordEvent.setTableId(tableName);
        insertRecordEvent.setAfter(after);
        TapdataEvent tapdataEvent = new TapdataEvent();
        tapdataEvent.setTapEvent(insertRecordEvent);
        return tapdataEvent;
    }

    // ========== dbPath 拼接测试 ==========
    
    @Test
    void testBuildDbPathWithNodeId_NormalPath() {
        // 测试正常路径（无分隔符）
        String dbPath = "/data/duckdb";
        String nodeId = "abc123";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals("/data/duckdb_abc123", result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_PathEndsWithForwardSlash() {
        // 测试路径以 / 结尾
        String dbPath = "/data/duckdb/";
        String nodeId = "abc123";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals("/data/duckdb_abc123", result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_PathEndsWithBackwardSlash() {
        // 测试路径以 \ 结尾
        String dbPath = "C:\\data\\duckdb\\";
        String nodeId = "abc123";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals("C:\\data\\duckdb_abc123", result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_PathEndsWithMultipleSlashes() {
        // 测试路径以多个分隔符结尾
        String dbPath = "/data/duckdb///";
        String nodeId = "abc123";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals("/data/duckdb_abc123", result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_PathEndsWithMixedSlashes() {
        // 测试路径以混合分隔符结尾
        String dbPath = "/data/duckdb\\/";
        String nodeId = "abc123";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals("/data/duckdb_abc123", result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_EmptyDbPath_ThrowsException() {
        // 测试空 dbPath，应该抛异常
        String dbPath = "";
        String nodeId = "abc123";
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> 
            HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId)
        );
        assertEquals("dbPath cannot be null or empty", exception.getMessage());
    }
    
    @Test
    void testBuildDbPathWithNodeId_NullDbPath_ThrowsException() {
        // 测试 null dbPath，应该抛异常
        String nodeId = "abc123";
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> 
            HazelcastDuckDbSqlNode.buildDbPathWithNodeId(null, nodeId)
        );
        assertEquals("dbPath cannot be null or empty", exception.getMessage());
    }
    
    @Test
    void testBuildDbPathWithNodeId_EmptyNodeId_ThrowsException() {
        // 测试空 nodeId，应该抛异常
        String dbPath = "/data/duckdb";
        String nodeId = "";
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> 
            HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId)
        );
        assertEquals("nodeId cannot be null or empty", exception.getMessage());
    }
    
    @Test
    void testBuildDbPathWithNodeId_NullNodeId_ThrowsException() {
        // 测试 null nodeId，应该抛异常
        String dbPath = "/data/duckdb";
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> 
            HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, null)
        );
        assertEquals("nodeId cannot be null or empty", exception.getMessage());
    }
    
    @Test
    void testBuildDbPathWithNodeId_RelativePath() {
        // 测试相对路径
        String dbPath = "data/duckdb";
        String nodeId = "test-node-456";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals("data/duckdb_test-node-456", result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_RelativePathEndsWithSlash() {
        // 测试相对路径以分隔符结尾
        String dbPath = "./data/duckdb/";
        String nodeId = "test-node-789";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals("./data/duckdb_test-node-789", result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_JustDriveLetter() {
        // 测试只有盘符的情况
        String dbPath = "C:\\";
        String nodeId = "drive-node";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals("C:_drive-node", result);
    }

}

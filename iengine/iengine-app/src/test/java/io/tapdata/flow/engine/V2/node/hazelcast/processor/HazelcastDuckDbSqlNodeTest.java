package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.HashMap;
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
}

package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

class HazelcastDuckDbSqlNodeMultiSourceTest {

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
        hazelcastDuckDbSqlNode.setBatchSize(100);
    }

    @Test
    void routesEventsToIndependentPerSourceContexts() throws Exception {
        AtomicInteger consumerCalls = new AtomicInteger(0);

        hazelcastDuckDbSqlNode.tryProcess(buildInsertEvent("source-a", "orders", 1), (event, result) -> consumerCalls.incrementAndGet());
        hazelcastDuckDbSqlNode.tryProcess(buildInsertEvent("source-b", "orders", 2), (event, result) -> consumerCalls.incrementAndGet());

        assertEquals(2, consumerCalls.get());

        Map<String, ?> contexts = getContexts();
        assertEquals(2, contexts.size());
        assertTrue(contexts.containsKey("source-a:orders"));
        assertTrue(contexts.containsKey("source-b:orders"));

        Object firstContext = contexts.get("source-a:orders");
        Object secondContext = contexts.get("source-b:orders");

        assertEquals(1, getBatchBuffer(firstContext).size());
        assertEquals(1, getBatchBuffer(secondContext).size());
        assertNotEquals(getTargetTableName(firstContext), getTargetTableName(secondContext));
    }

    @SuppressWarnings("unchecked")
    private Map<String, ?> getContexts() throws Exception {
        Field field = HazelcastDuckDbSqlNode.class.getDeclaredField("sourceContexts");
        field.setAccessible(true);
        return (Map<String, ?>) field.get(hazelcastDuckDbSqlNode);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getBatchBuffer(Object context) throws Exception {
        Field field = context.getClass().getDeclaredField("batchBuffer");
        field.setAccessible(true);
        return (List<Map<String, Object>>) field.get(context);
    }

    private String getTargetTableName(Object context) throws Exception {
        Field field = context.getClass().getDeclaredField("targetTableName");
        field.setAccessible(true);
        return (String) field.get(context);
    }

    private TapdataEvent buildInsertEvent(String sourceId, String tableId, int id) {
        TapInsertRecordEvent event = TapInsertRecordEvent.create().table(tableId);
        event.setAssociateId(sourceId);
        Map<String, Object> after = new HashMap<>();
        after.put("id", id);
        after.put("name", sourceId + "-" + id);
        event.setAfter(after);

        TapdataEvent tapdataEvent = new TapdataEvent();
        tapdataEvent.setTapEvent(event);
        return tapdataEvent;
    }
}

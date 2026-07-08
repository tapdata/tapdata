package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperator;
import io.tapdata.flow.engine.V2.node.duckdb.NodeSchemaInfo;
import io.tapdata.observable.logging.ObsLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HazelcastDuckDbSqlNodeMultiSourceTest {

    @Mock
    private ProcessorBaseContext processorBaseContext;

    @Mock
    private TaskDto taskDto;

    @Mock
    private Node node;

    private HazelcastDuckDbSqlNode hazelcastDuckDbSqlNode;
    ObsLogger obsLogger;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
        when(processorBaseContext.getNode()).thenReturn(node);
        when(node.getId()).thenReturn("test_node");
        when(node.getName()).thenReturn("TestNode");
        when(taskDto.isNormalTask()).thenReturn(true);
        obsLogger = mock(ObsLogger.class);

        hazelcastDuckDbSqlNode = new HazelcastDuckDbSqlNode(processorBaseContext);
        hazelcastDuckDbSqlNode.setBatchSize(100);
        ReflectionTestUtils.setField(hazelcastDuckDbSqlNode, "obsLogger", obsLogger);
    }

    @Test
    void routesEventsToIndependentPerSourceContexts() throws Exception {
        DuckDbOperator sharedOperator = mock(DuckDbOperator.class);
        ReflectionTestUtils.setField(hazelcastDuckDbSqlNode, "duckDbOperator", sharedOperator);
        ReflectionTestUtils.setField(hazelcastDuckDbSqlNode, "dbPath", "");

        NodeSchemaInfo schema = buildSchema("source-a", "orders");
        invokeGetOrCreateContext("source-a:orders", "orders", "source-a", "orders", schema);
        invokeGetOrCreateContext("source-b:orders", "orders", "source-b", "orders", schema);

        Map<String, ?> contexts = getContexts();
        assertEquals(2, contexts.size());
        assertTrue(contexts.containsKey("source-a:orders"));
        assertTrue(contexts.containsKey("source-b:orders"));

        Object firstContext = contexts.get("source-a:orders");
        Object secondContext = contexts.get("source-b:orders");

        assertEquals(0, getBatchBuffer(firstContext).size());
        assertEquals(0, getBatchBuffer(secondContext).size());
        assertEquals(getTargetTableName(firstContext), getTargetTableName(secondContext));
        assertSame(sharedOperator, getOperator(firstContext));
        assertSame(sharedOperator, getOperator(secondContext));
    }

    @SuppressWarnings("unchecked")
    private Map<String, ?> getContexts() throws Exception {
        Field field = HazelcastDuckDbSqlNode.class.getDeclaredField("sourceContexts");
        field.setAccessible(true);
        return (Map<String, ?>) field.get(hazelcastDuckDbSqlNode);
    }

    @SuppressWarnings("unchecked")
    private List<TapdataEvent> getBatchBuffer(Object context) throws Exception {
        Field field = context.getClass().getDeclaredField("batchBuffer");
        field.setAccessible(true);
        return (List<TapdataEvent>) field.get(context);
    }

    private String getTargetTableName(Object context) throws Exception {
        Field field = context.getClass().getDeclaredField("targetTableName");
        field.setAccessible(true);
        return (String) field.get(context);
    }

    private Object getOperator(Object context) throws Exception {
        Field field = context.getClass().getDeclaredField("operator");
        field.setAccessible(true);
        return field.get(context);
    }

    private Object invokeGetOrCreateContext(String contextKey, String targetTableName, String sourceId,
                                            String tableId, NodeSchemaInfo schemaInfo) throws Exception {
        java.lang.reflect.Method method = HazelcastDuckDbSqlNode.class.getDeclaredMethod(
                "getOrCreateContext",
                String.class,
                String.class,
                String.class,
                String.class,
                NodeSchemaInfo.class
        );
        method.setAccessible(true);
        return method.invoke(hazelcastDuckDbSqlNode, contextKey, targetTableName, sourceId, tableId, schemaInfo);
    }

    private NodeSchemaInfo buildSchema(String nodeId, String tableName) {
        io.tapdata.entity.schema.TapTable tapTable = new io.tapdata.entity.schema.TapTable(tableName);
        io.tapdata.entity.schema.TapField field = new io.tapdata.entity.schema.TapField();
        field.setName("id");
        field.setOriginalFieldName("id");
        tapTable.add(field);
        return new NodeSchemaInfo(nodeId, tableName, tableName, List.of("id"), new HashMap<>(Map.of("id", field)), tapTable, null);
    }
}

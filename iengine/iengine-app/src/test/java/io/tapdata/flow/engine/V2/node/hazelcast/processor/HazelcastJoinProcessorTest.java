package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import io.tapdata.entity.codec.filter.impl.AllLayerMapIterator;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.join.HazelcastJoinProcessor;
import io.tapdata.schema.TapTableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.mockito.Mockito.*;


public class HazelcastJoinProcessorTest extends BaseHazelcastNodeTest {
    HazelcastJoinProcessor hazelcastJoinProcessor;
    JoinProcessorNode joinProcessorNode;
    @BeforeEach
    void beforeEach() {
        super.allSetup();
        joinProcessorNode = new JoinProcessorNode();
        joinProcessorNode.setId("node_test");
        joinProcessorNode.setJoinType("left");
        joinProcessorNode.setEmbeddedMode(false);
        JoinProcessorNode.JoinExpression joinExpression = new JoinProcessorNode.JoinExpression();
        joinExpression.setExpression("test");
        joinExpression.setLeft("left");
        joinExpression.setRight("right");
        joinProcessorNode.setJoinExpressions(Arrays.asList(joinExpression));
        joinProcessorNode.setLeftNodeId("leftNodeId");
        joinProcessorNode.setRightNodeId("rightNodeId");
        joinProcessorNode.setLeftPrimaryKeys(new ArrayList<>());
        joinProcessorNode.setRightPrimaryKeys(new ArrayList<>());
        TapTableMap<String, TapTable> tapTableMap = mock(TapTableMap.class);
        when(processorBaseContext.getTapTableMap()).thenReturn(tapTableMap);
        when(tapTableMap.keySet()).thenReturn(new HashSet<>(Arrays.asList("left")));
        when(tapTableMap.get("left")).thenReturn(mock(TapTable.class));
        hazelcastJoinProcessor = spy(new HazelcastJoinProcessor(processorBaseContext));
    }

    @Test
    void testDoInit(){
        doNothing().when(hazelcastJoinProcessor).initNode();
        hazelcastJoinProcessor.doInit(jetContext);
        Assertions.assertNotNull(ReflectionTestUtils.getField(hazelcastJoinProcessor,"mapIterator"));
    }
    @Test
    void testTransformDateTime(){
        ReflectionTestUtils.setField(hazelcastJoinProcessor,"mapIterator",new AllLayerMapIterator());
        Map<String, Object> before = new HashMap<>();
        before.put("date",new DateTime(new Date()));
        before.put("text","text");
        Map<String, Object> after = new HashMap<>();
        after.put("date",new DateTime(new Date()));
        after.put("text","text");
        hazelcastJoinProcessor.transformDateTime(before,after);
        Assertions.assertInstanceOf(Date.class, before.get("date"));
        Assertions.assertInstanceOf(Date.class, after.get("date"));
    }
}

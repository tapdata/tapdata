package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.Processor;
import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.pdk.core.api.ConnectorNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HazelcastSampleSourcePdkDataNodeTest {
    HazelcastSampleSourcePdkDataNode node;
    Processor.Context jetContext;
    DataProcessorContext dataProcessorContext;

    @BeforeEach
    void init() {
        node = mock(HazelcastSampleSourcePdkDataNode.class);
        jetContext = mock( Processor.Context.class);
        dataProcessorContext = mock(DataProcessorContext.class);

        ReflectionTestUtils.setField(node, "jetContext", jetContext);
        ReflectionTestUtils.setField(node, "dataProcessorContext", dataProcessorContext);
    }

    @Nested
    class InitNodeTest {
        HazelcastInstance hazelcastInstance;
        ConnectorNode connectorNode;
        TapCodecsFilterManager codecsFilterManager;

        @BeforeEach
        void init() {
            hazelcastInstance = mock(HazelcastInstance.class);
            connectorNode = mock(ConnectorNode.class);
            codecsFilterManager = mock(TapCodecsFilterManager.class);

            doNothing().when(node).createPdkConnectorNode(dataProcessorContext, hazelcastInstance);
            doNothing().when(node).connectorNodeInit(dataProcessorContext);
            when(node.getConnectorNode()).thenReturn(connectorNode);
            when(connectorNode.getCodecsFilterManager()).thenReturn(codecsFilterManager);
            when(jetContext.hazelcastInstance()).thenReturn(hazelcastInstance);
            when(node.initNode()).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(node::initNode);
            verify(node).createPdkConnectorNode(dataProcessorContext, hazelcastInstance);
            verify(node).connectorNodeInit(dataProcessorContext);
            verify(node).getConnectorNode();
            verify(connectorNode).getCodecsFilterManager();
            verify(jetContext).hazelcastInstance();
        }
    }
}
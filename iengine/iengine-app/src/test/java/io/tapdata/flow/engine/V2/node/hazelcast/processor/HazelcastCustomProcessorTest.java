package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.process.AddDateFieldProcessorNode;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import io.tapdata.error.TaskInspectExCode_27;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class HazelcastCustomProcessorTest extends BaseHazelcastNodeTest {
    HazelcastCustomProcessor hazelcastCustomProcessor;

    @BeforeEach
    protected void beforeEach() {
        super.allSetup();
        hazelcastCustomProcessor = new HazelcastCustomProcessor(dataProcessorContext);
    }
    @Test
    void test1(){
        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        customProcessorNode.setCustomNodeId("customNodeId");
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();
        ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
        ReflectionTestUtils.setField(hazelcastCustomProcessor,"clientMongoOperator",clientMongoOperator);
        try{
            hazelcastCustomProcessor.doInit(jetContext);
        }catch (TapCodeException e){
            Assertions.assertEquals(TaskProcessorExCode_11.CUSTOM_NODE_NOT_FOUND, e.getCode());
        }
    }

}

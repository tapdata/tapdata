package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.error.ScriptProcessorExCode_30;
import com.tapdata.tm.commons.customNode.CustomNodeTempDto;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import javax.script.ScriptException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class HazelcastCustomProcessorTest extends BaseHazelcastNodeTest {
    HazelcastCustomProcessor hazelcastCustomProcessor;
    ClientMongoOperator clientMongoOperator;

    @BeforeEach
    protected void beforeEach() {
        super.allSetup();
//        HazelcastCustomProcessor hazelcastCustomProcessor1 = new HazelcastCustomProcessor(dataProcessorContext);
        hazelcastCustomProcessor = mock(HazelcastCustomProcessor.class);
        ReflectionTestUtils.setField(hazelcastCustomProcessor,"processorBaseContext",dataProcessorContext);
        clientMongoOperator = mock(ClientMongoOperator.class);
        ReflectionTestUtils.setField(hazelcastCustomProcessor,"clientMongoOperator",clientMongoOperator);
        doCallRealMethod().when(hazelcastCustomProcessor).doInit(any());
    }
    @DisplayName("test doInit method for exception CUSTOM_NODE_NOT_FOUND")
    @Test
    void test1(){
        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        customProcessorNode.setCustomNodeId("customNodeId");
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();
        try{
            hazelcastCustomProcessor.doInit(jetContext);
        }catch (TapCodeException e){
            assertEquals(TaskProcessorExCode_11.CUSTOM_NODE_NOT_FOUND, e.getCode());
        }
    }
    @DisplayName("test doInit method for exception CUSTOM_PROCESSOR_GET_SCRIPT_ENGINE_FAILED")
    @Test
    void test2() {
        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        customProcessorNode.setCustomNodeId("customNodeId");
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();
        CustomNodeTempDto customNodeTempDto = new CustomNodeTempDto();
        when(clientMongoOperator.findOne(any(Query.class), anyString(), any(), any())).thenReturn(customNodeTempDto);
        try (MockedStatic<ScriptUtil> scriptUtilMockedStatic = mockStatic(ScriptUtil.class);) {
            scriptUtilMockedStatic.when(() -> {
                ScriptUtil.getScriptEngine(eq(null),
                        anyList(),
                        any(ClientMongoOperator.class),
                        eq(null),
                        any(ObsScriptLogger.class));
            }).thenThrow(new ScriptException("getFailed"));
            try{

                hazelcastCustomProcessor.doInit(jetContext);
            }catch (TapCodeException e){
                assertEquals(ScriptProcessorExCode_30.CUSTOM_PROCESSOR_GET_SCRIPT_ENGINE_FAILED,e.getCode());
            }
        }

    }

}

package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.error.ScriptProcessorExCode_30;
import com.tapdata.tm.commons.customNode.CustomNodeTempDto;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import javax.script.Invocable;
import javax.script.ScriptException;

import java.util.HashMap;
import java.util.Map;

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

    @Test
    @SneakyThrows
    void testExecute() {
        TapdataEvent tapdataEvent = new TapdataEvent();
        TapInsertRecordEvent event = TapInsertRecordEvent.create().init();
        Map<String, Object> after = new HashMap<>();
        after.put("id", "1");
        after.put("name", "test");
        event.setAfter(after);
        event.setTableId("table1");
        tapdataEvent.setTapEvent(event);
        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        customProcessorNode.setCustomNodeId("customNodeId");
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();
        doCallRealMethod().when(hazelcastCustomProcessor).execute(tapdataEvent);
        doCallRealMethod().when(hazelcastCustomProcessor).buildContextMap(any(), any(), any(), any(), any());
        Invocable engine = ScriptUtil.getScriptEngine(
                "function process(record, form){\n" +
                        "\n" +
                        "\t// Enter your code here\n" +
                        "\trecord.__op = context.op;\n" +
                        "\treturn record;\n" +
                        "}",
                null,
                clientMongoOperator,
                null,
                null);
        ReflectionTestUtils.setField(hazelcastCustomProcessor, "engine", engine);
        ThreadLocal<Map<String, Object>> processContextThreadLocal = ThreadLocal.withInitial(HashMap::new);
        ReflectionTestUtils.setField(hazelcastCustomProcessor, "processContextThreadLocal", processContextThreadLocal);
        when(hazelcastCustomProcessor.getProcessorBaseContext()).thenReturn(dataProcessorContext);
        when(dataProcessorContext.getTaskDto()).thenReturn(mock(TaskDto.class));
        hazelcastCustomProcessor.execute(tapdataEvent);
        assertEquals("i", event.getAfter().get("__op"));
    }
}

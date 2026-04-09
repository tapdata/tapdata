package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.error.ScriptProcessorExCode_30;
import com.tapdata.tm.commons.customNode.CustomNodeTempDto;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import io.tapdata.observable.logging.ObsLogger;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
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
                ScriptUtil.getScriptEngine(anyString(),
                        any(),
                        anyList(),
                        any(ClientMongoOperator.class),
                        any(),
                        any(),
                        any(),
                        any(),
                        anyBoolean());
            }).thenThrow(new ScriptException("getFailed"));
            try{

                hazelcastCustomProcessor.doInit(jetContext);
            }catch (TapCodeException e){
                assertEquals(ScriptProcessorExCode_30.CUSTOM_PROCESSOR_GET_SCRIPT_ENGINE_FAILED,e.getCode());
            }
        }

    }

    @DisplayName("test doInit when node type is not CUSTOM_PROCESSOR should skip custom processor logic")
    @Test
    void testDoInitNotCustomProcessorNode() {
        MigrateJsProcessorNode jsNode = new MigrateJsProcessorNode();
        ReflectionTestUtils.setField(dataProcessorContext, "node", jsNode);
        doCallRealMethod().when(dataProcessorContext).getNode();

        // Should not throw any exception and should not call clientMongoOperator
        hazelcastCustomProcessor.doInit(jetContext);
        verify(clientMongoOperator, never()).findOne(any(Query.class), anyString(), any(), any());
    }

    @DisplayName("test doInit success path: engine, stateMap, scriptExecutorsManager are set correctly")
    @Test
    void testDoInitSuccess() {
        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        customProcessorNode.setCustomNodeId("customNodeId");
        customProcessorNode.setId("nodeId1");
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();

        // mock running field
        ReflectionTestUtils.setField(hazelcastCustomProcessor, "running", new AtomicBoolean(true));

        // mock processorBaseContext.getTaskDto() early, needed by ScriptCacheService constructor
        TaskDto mockTaskDto = mock(TaskDto.class);
        when(mockTaskDto.getId()).thenReturn(new org.bson.types.ObjectId());
        when(mockTaskDto.isNormalTask()).thenReturn(true);
        when(dataProcessorContext.getTaskDto()).thenReturn(mockTaskDto);

        CustomNodeTempDto customNodeTempDto = new CustomNodeTempDto();
        customNodeTempDto.setTemplate("function process(record){ return record; }");
        when(clientMongoOperator.findOne(any(Query.class), anyString(), any(), any())).thenReturn(customNodeTempDto);
        doReturn(new ArrayList<JavaScriptFunctions>()).when(clientMongoOperator).find(any(Query.class), anyString(), any(), any());

        // mock ScriptUtil to return a mock engine
        Invocable mockEngine = mock(Invocable.class, withSettings().extraInterfaces(ScriptEngine.class));
        try (MockedStatic<ScriptUtil> scriptUtilMockedStatic = mockStatic(ScriptUtil.class)) {
            scriptUtilMockedStatic.when(() -> {
                ScriptUtil.getScriptEngine(anyString(),
                        any(),
                        anyList(),
                        any(ClientMongoOperator.class),
                        any(),
                        any(),
                        any(),
                        any(),
                        anyBoolean());
            }).thenReturn(mockEngine);

            // mock jetContext.hazelcastInstance() for stateMap and scriptExecutorsManager
            HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
            when(jetContext.hazelcastInstance()).thenReturn(mockHazelcastInstance);
            JetService mockJetService = mock(JetService.class);
            when(mockHazelcastInstance.getJet()).thenReturn(mockJetService);

            // mock getScriptObsLogger
            ObsLogger mockScriptObsLogger = mock(ObsLogger.class);
            when(hazelcastCustomProcessor.getScriptObsLogger()).thenReturn(mockScriptObsLogger);

            // mock jetContext field for scriptExecutorsManager
            ReflectionTestUtils.setField(hazelcastCustomProcessor, "jetContext", jetContext);

            hazelcastCustomProcessor.doInit(jetContext);

            // verify engine was set
            Invocable engineField = (Invocable) ReflectionTestUtils.getField(hazelcastCustomProcessor, "engine");
            assertNotNull(engineField);
            assertSame(mockEngine, engineField);

            // verify stateMap was set via engine.put("state", ...)
            verify((ScriptEngine) mockEngine).put(eq("state"), any());

            // verify scriptExecutorsManager was set via engine.put("ScriptExecutorsManager", ...)
            verify((ScriptEngine) mockEngine).put(eq("ScriptExecutorsManager"), any());
        }
    }

    @DisplayName("test doInit with empty taskIds list should still init correctly")
    @Test
    void testDoInitWithEmptyJavaScriptFunctions() {
        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        customProcessorNode.setCustomNodeId("customNodeId");
        customProcessorNode.setId("nodeId2");
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();

        ReflectionTestUtils.setField(hazelcastCustomProcessor, "running", new AtomicBoolean(true));

        TaskDto mockTaskDto = mock(TaskDto.class);
        when(mockTaskDto.getId()).thenReturn(new org.bson.types.ObjectId());
        when(mockTaskDto.isNormalTask()).thenReturn(true);
        when(dataProcessorContext.getTaskDto()).thenReturn(mockTaskDto);

        CustomNodeTempDto customNodeTempDto = new CustomNodeTempDto();
        when(clientMongoOperator.findOne(any(Query.class), anyString(), any(), any())).thenReturn(customNodeTempDto);
        doReturn(null).when(clientMongoOperator).find(any(Query.class), anyString(), any(), any());

        Invocable mockEngine = mock(Invocable.class, withSettings().extraInterfaces(ScriptEngine.class));
        try (MockedStatic<ScriptUtil> scriptUtilMockedStatic = mockStatic(ScriptUtil.class)) {
            scriptUtilMockedStatic.when(() -> {
                ScriptUtil.getScriptEngine(anyString(),
                        any(),
                        any(),
                        any(ClientMongoOperator.class),
                        any(),
                        any(),
                        any(),
                        any(),
                        anyBoolean());
            }).thenReturn(mockEngine);

            HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
            when(jetContext.hazelcastInstance()).thenReturn(mockHazelcastInstance);
            JetService mockJetService = mock(JetService.class);
            when(mockHazelcastInstance.getJet()).thenReturn(mockJetService);
            ObsLogger mockScriptObsLogger = mock(ObsLogger.class);
            when(hazelcastCustomProcessor.getScriptObsLogger()).thenReturn(mockScriptObsLogger);
            ReflectionTestUtils.setField(hazelcastCustomProcessor, "jetContext", jetContext);

            hazelcastCustomProcessor.doInit(jetContext);

            Invocable engineField = (Invocable) ReflectionTestUtils.getField(hazelcastCustomProcessor, "engine");
            assertNotNull(engineField);
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

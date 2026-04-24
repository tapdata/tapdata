package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
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
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import io.tapdata.flow.engine.V2.script.ScriptExecutorsManager;
import io.tapdata.observable.logging.ObsLogger;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
        doCallRealMethod().when(hazelcastCustomProcessor).executeAndGetResult(tapdataEvent);
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
        Object result = hazelcastCustomProcessor.executeAndGetResult(tapdataEvent);
        assertNotNull(result);
        assertEquals("i", ((Map<String, Object>) result).get("__op"));
    }

    @Test
    @SneakyThrows
    void testTryProcessUseOpListForListResult() {
        HazelcastCustomProcessor processor = spy(new HazelcastCustomProcessor(dataProcessorContext));
        ReflectionTestUtils.setField(processor, "processorBaseContext", dataProcessorContext);
        ReflectionTestUtils.setField(processor, "clientMongoOperator", clientMongoOperator);
        ReflectionTestUtils.setField(processor, "processContextThreadLocal", ThreadLocal.withInitial(HashMap::new));
        ReflectionTestUtils.setField(processor, "globalTaskContent", new HashMap<String, Object>());

        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        customProcessorNode.setCustomNodeId("customNodeId");
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();
        when(dataProcessorContext.getTaskDto()).thenReturn(mock(TaskDto.class));

        TapdataEvent tapdataEvent = new TapdataEvent();
        TapInsertRecordEvent event = TapInsertRecordEvent.create().init();
        Map<String, Object> after = new HashMap<>();
        after.put("id", "source");
        event.setAfter(after);
        tapdataEvent.setTapEvent(event);

        Invocable engine = ScriptUtil.getScriptEngine(
                "function process(record, form){\n" +
                        "\tcontext.opList = ['i', 'd'];\n" +
                        "\treturn [\n" +
                        "\t\t{id: '1', name: 'inserted'},\n" +
                        "\t\t{id: '2', name: 'deleted'}\n" +
                        "\t];\n" +
                        "}",
                null,
                clientMongoOperator,
                null,
                null);
        ReflectionTestUtils.setField(processor, "engine", engine);

        List<TapdataEvent> outputEvents = new ArrayList<>();
        processor.tryProcess(tapdataEvent, (resultEvent, processResult) -> outputEvents.add(resultEvent));

        assertEquals(2, outputEvents.size());

        TapEvent firstEvent = outputEvents.get(0).getTapEvent();
        assertInstanceOf(TapInsertRecordEvent.class, firstEvent);
        assertEquals("inserted", ((TapInsertRecordEvent) firstEvent).getAfter().get("name"));

        TapEvent secondEvent = outputEvents.get(1).getTapEvent();
        assertInstanceOf(TapDeleteRecordEvent.class, secondEvent);
        assertEquals("deleted", ((TapDeleteRecordEvent) secondEvent).getBefore().get("name"));
    }

    @Test
    @SneakyThrows
    @DisplayName("test tryProcess with single map result should invoke consumer once")
    void testTryProcessSingleMapResult() {
        HazelcastCustomProcessor processor = spy(new HazelcastCustomProcessor(dataProcessorContext));
        ReflectionTestUtils.setField(processor, "processorBaseContext", dataProcessorContext);
        ReflectionTestUtils.setField(processor, "clientMongoOperator", clientMongoOperator);
        ReflectionTestUtils.setField(processor, "processContextThreadLocal", ThreadLocal.withInitial(HashMap::new));
        ReflectionTestUtils.setField(processor, "globalTaskContent", new HashMap<String, Object>());

        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        customProcessorNode.setCustomNodeId("customNodeId");
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();
        when(dataProcessorContext.getTaskDto()).thenReturn(mock(TaskDto.class));

        TapdataEvent tapdataEvent = new TapdataEvent();
        TapInsertRecordEvent event = TapInsertRecordEvent.create().init();
        Map<String, Object> after = new HashMap<>();
        after.put("id", "1");
        event.setAfter(after);
        event.setTableId("table1");
        tapdataEvent.setTapEvent(event);

        Invocable engine = ScriptUtil.getScriptEngine(
                "function process(record, form){\n" +
                        "\treturn {id: '1', name: 'processed'};\n" +
                        "}",
                null, clientMongoOperator, null, null);
        ReflectionTestUtils.setField(processor, "engine", engine);

        List<TapdataEvent> outputEvents = new ArrayList<>();
        processor.tryProcess(tapdataEvent, (resultEvent, processResult) -> outputEvents.add(resultEvent));

        assertEquals(1, outputEvents.size());
        TapEvent resultEvent = outputEvents.get(0).getTapEvent();
        assertInstanceOf(TapInsertRecordEvent.class, resultEvent);
        assertEquals("processed", ((TapInsertRecordEvent) resultEvent).getAfter().get("name"));
    }

    @Test
    @SneakyThrows
    @DisplayName("test tryProcess when script returns null should not invoke consumer")
    void testTryProcessNullResult() {
        HazelcastCustomProcessor processor = spy(new HazelcastCustomProcessor(dataProcessorContext));
        ReflectionTestUtils.setField(processor, "processorBaseContext", dataProcessorContext);
        ReflectionTestUtils.setField(processor, "clientMongoOperator", clientMongoOperator);
        ReflectionTestUtils.setField(processor, "processContextThreadLocal", ThreadLocal.withInitial(HashMap::new));
        ReflectionTestUtils.setField(processor, "globalTaskContent", new HashMap<String, Object>());

        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        customProcessorNode.setCustomNodeId("customNodeId");
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();
        when(dataProcessorContext.getTaskDto()).thenReturn(mock(TaskDto.class));

        TapdataEvent tapdataEvent = new TapdataEvent();
        TapInsertRecordEvent event = TapInsertRecordEvent.create().init();
        Map<String, Object> after = new HashMap<>();
        after.put("id", "1");
        event.setAfter(after);
        tapdataEvent.setTapEvent(event);

        Invocable engine = ScriptUtil.getScriptEngine(
                "function process(record, form){\n" +
                        "\treturn null;\n" +
                        "}",
                null, clientMongoOperator, null, null);
        ReflectionTestUtils.setField(processor, "engine", engine);

        List<TapdataEvent> outputEvents = new ArrayList<>();
        processor.tryProcess(tapdataEvent, (resultEvent, processResult) -> outputEvents.add(resultEvent));

        assertEquals(0, outputEvents.size());
    }

    @Test
    @SneakyThrows
    @DisplayName("test tryProcess with op change: insert -> update")
    void testTryProcessOpChangeInsertToUpdate() {
        HazelcastCustomProcessor processor = spy(new HazelcastCustomProcessor(dataProcessorContext));
        ReflectionTestUtils.setField(processor, "processorBaseContext", dataProcessorContext);
        ReflectionTestUtils.setField(processor, "clientMongoOperator", clientMongoOperator);
        ReflectionTestUtils.setField(processor, "processContextThreadLocal", ThreadLocal.withInitial(HashMap::new));
        ReflectionTestUtils.setField(processor, "globalTaskContent", new HashMap<String, Object>());

        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        customProcessorNode.setCustomNodeId("customNodeId");
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();
        when(dataProcessorContext.getTaskDto()).thenReturn(mock(TaskDto.class));

        TapdataEvent tapdataEvent = new TapdataEvent();
        TapInsertRecordEvent event = TapInsertRecordEvent.create().init();
        Map<String, Object> after = new HashMap<>();
        after.put("id", "1");
        event.setAfter(after);
        tapdataEvent.setTapEvent(event);

        Invocable engine = ScriptUtil.getScriptEngine(
                "function process(record, form){\n" +
                        "\tcontext.op = 'u';\n" +
                        "\treturn {id: '1', name: 'updated'};\n" +
                        "}",
                null, clientMongoOperator, null, null);
        ReflectionTestUtils.setField(processor, "engine", engine);

        List<TapdataEvent> outputEvents = new ArrayList<>();
        processor.tryProcess(tapdataEvent, (resultEvent, processResult) -> outputEvents.add(resultEvent));

        assertEquals(1, outputEvents.size());
        TapEvent resultEvent = outputEvents.get(0).getTapEvent();
        assertInstanceOf(TapUpdateRecordEvent.class, resultEvent);
        assertEquals("updated", ((TapUpdateRecordEvent) resultEvent).getAfter().get("name"));
    }

    @Test
    @SneakyThrows
    @DisplayName("test executeAndGetResult with empty after and before should return null")
    void testExecuteAndGetResultEmptyAfterAndBefore() {
        doCallRealMethod().when(hazelcastCustomProcessor).executeAndGetResult(any());
        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();

        TapdataEvent tapdataEvent = new TapdataEvent();
        TapInsertRecordEvent event = TapInsertRecordEvent.create().init();
        // after is null, before is null for insert event
        tapdataEvent.setTapEvent(event);

        Object result = hazelcastCustomProcessor.executeAndGetResult(tapdataEvent);
        assertNull(result);
    }

    @Test
    @SneakyThrows
    @DisplayName("test executeAndGetResult with delete event should use before map")
    void testExecuteAndGetResultDeleteEvent() {
        HazelcastCustomProcessor processor = spy(new HazelcastCustomProcessor(dataProcessorContext));
        ReflectionTestUtils.setField(processor, "processorBaseContext", dataProcessorContext);
        ReflectionTestUtils.setField(processor, "clientMongoOperator", clientMongoOperator);
        ReflectionTestUtils.setField(processor, "processContextThreadLocal", ThreadLocal.withInitial(HashMap::new));
        ReflectionTestUtils.setField(processor, "globalTaskContent", new HashMap<String, Object>());

        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        customProcessorNode.setCustomNodeId("customNodeId");
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();
        when(dataProcessorContext.getTaskDto()).thenReturn(mock(TaskDto.class));

        TapdataEvent tapdataEvent = new TapdataEvent();
        TapDeleteRecordEvent event = TapDeleteRecordEvent.create().init();
        Map<String, Object> before = new HashMap<>();
        before.put("id", "1");
        before.put("name", "toDelete");
        event.setBefore(before);
        tapdataEvent.setTapEvent(event);

        Invocable engine = ScriptUtil.getScriptEngine(
                "function process(record, form){\n" +
                        "\trecord.processed = true;\n" +
                        "\treturn record;\n" +
                        "}",
                null, clientMongoOperator, null, null);
        ReflectionTestUtils.setField(processor, "engine", engine);

        Object result = processor.executeAndGetResult(tapdataEvent);
        assertNotNull(result);
        assertInstanceOf(Map.class, result);
        assertEquals(true, ((Map<String, Object>) result).get("processed"));
    }

    @Nested
    @DisplayName("doClose method tests")
    class DoCloseTest {
        @Test
        @DisplayName("test doClose with GraalJSScriptEngine should close engine")
        void testDoCloseWithGraalJSScriptEngine() {
            GraalJSScriptEngine mockEngine = mock(GraalJSScriptEngine.class);
            ReflectionTestUtils.setField(hazelcastCustomProcessor, "engine", mockEngine);
            ThreadLocal<Map<String, Object>> threadLocal = ThreadLocal.withInitial(HashMap::new);
            ReflectionTestUtils.setField(hazelcastCustomProcessor, "processContextThreadLocal", threadLocal);
            ScriptExecutorsManager mockScriptMgr = mock(ScriptExecutorsManager.class);
            ReflectionTestUtils.setField(hazelcastCustomProcessor, "scriptExecutorsManager", mockScriptMgr);
            CustomProcessorNode mockNode = mock(CustomProcessorNode.class);
            when(mockNode.getTaskId()).thenReturn("task1");
            doReturn(mockNode).when(hazelcastCustomProcessor).getNode();
            ReflectionTestUtils.setField(hazelcastCustomProcessor, "obsLogger", mockObsLogger);
            doCallRealMethod().when(hazelcastCustomProcessor).doClose();

            assertDoesNotThrow(() -> hazelcastCustomProcessor.doClose());
            verify(mockScriptMgr).close();
        }

        @Test
        @DisplayName("test doClose with null engine and null scriptExecutorsManager should not throw")
        void testDoCloseWithNullFields() {
            ReflectionTestUtils.setField(hazelcastCustomProcessor, "engine", null);
            ReflectionTestUtils.setField(hazelcastCustomProcessor, "processContextThreadLocal", null);
            ReflectionTestUtils.setField(hazelcastCustomProcessor, "scriptExecutorsManager", null);
            CustomProcessorNode mockNode = mock(CustomProcessorNode.class);
            when(mockNode.getTaskId()).thenReturn("task1");
            doReturn(mockNode).when(hazelcastCustomProcessor).getNode();
            ReflectionTestUtils.setField(hazelcastCustomProcessor, "obsLogger", mockObsLogger);
            doCallRealMethod().when(hazelcastCustomProcessor).doClose();

            assertDoesNotThrow(() -> hazelcastCustomProcessor.doClose());
        }
    }

    @Test
    @DisplayName("test needCopyBatchEventWrapper should return true")
    void testNeedCopyBatchEventWrapper() {
        when(hazelcastCustomProcessor.needCopyBatchEventWrapper()).thenCallRealMethod();
        assertTrue(hazelcastCustomProcessor.needCopyBatchEventWrapper());
    }

    @Test
    @DisplayName("test handleTransformToTapValueResult should set result to null")
    void testHandleTransformToTapValueResult() {
        doCallRealMethod().when(hazelcastCustomProcessor).handleTransformToTapValueResult(any());
        TapdataEvent tapdataEvent = new TapdataEvent();
        tapdataEvent.setTransformToTapValueResult(com.tapdata.entity.TransformToTapValueResult.create());
        hazelcastCustomProcessor.handleTransformToTapValueResult(tapdataEvent);
        assertNull(tapdataEvent.getTransformToTapValueResult());
    }

    @Test
    @DisplayName("test tryProcess with opList size mismatch should still process and warn")
    @SneakyThrows
    void testTryProcessOpListSizeMismatch() {
        HazelcastCustomProcessor processor = spy(new HazelcastCustomProcessor(dataProcessorContext));
        ReflectionTestUtils.setField(processor, "processorBaseContext", dataProcessorContext);
        ReflectionTestUtils.setField(processor, "clientMongoOperator", clientMongoOperator);
        ReflectionTestUtils.setField(processor, "processContextThreadLocal", ThreadLocal.withInitial(HashMap::new));
        ReflectionTestUtils.setField(processor, "globalTaskContent", new HashMap<String, Object>());
        ReflectionTestUtils.setField(processor, "obsLogger", mockObsLogger);

        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        customProcessorNode.setCustomNodeId("customNodeId");
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();
        when(dataProcessorContext.getTaskDto()).thenReturn(mock(TaskDto.class));

        TapdataEvent tapdataEvent = new TapdataEvent();
        TapInsertRecordEvent event = TapInsertRecordEvent.create().init();
        Map<String, Object> after = new HashMap<>();
        after.put("id", "source");
        event.setAfter(after);
        tapdataEvent.setTapEvent(event);

        // opList has 3 elements but result list has 2
        Invocable engine = ScriptUtil.getScriptEngine(
                "function process(record, form){\n" +
                        "\tcontext.opList = ['i', 'd', 'u'];\n" +
                        "\treturn [\n" +
                        "\t\t{id: '1'},\n" +
                        "\t\t{id: '2'}\n" +
                        "\t];\n" +
                        "}",
                null, clientMongoOperator, null, null);
        ReflectionTestUtils.setField(processor, "engine", engine);

        List<TapdataEvent> outputEvents = new ArrayList<>();
        processor.tryProcess(tapdataEvent, (resultEvent, processResult) -> outputEvents.add(resultEvent));

        assertEquals(2, outputEvents.size());
        verify(mockObsLogger).warn("context.opList size must match the result list size");
    }

    @Test
    @DisplayName("test tryProcess with op change: insert -> delete should produce TapDeleteRecordEvent with before")
    @SneakyThrows
    void testTryProcessOpChangeInsertToDelete() {
        HazelcastCustomProcessor processor = spy(new HazelcastCustomProcessor(dataProcessorContext));
        ReflectionTestUtils.setField(processor, "processorBaseContext", dataProcessorContext);
        ReflectionTestUtils.setField(processor, "clientMongoOperator", clientMongoOperator);
        ReflectionTestUtils.setField(processor, "processContextThreadLocal", ThreadLocal.withInitial(HashMap::new));
        ReflectionTestUtils.setField(processor, "globalTaskContent", new HashMap<String, Object>());

        CustomProcessorNode customProcessorNode = new CustomProcessorNode();
        customProcessorNode.setCustomNodeId("customNodeId");
        ReflectionTestUtils.setField(dataProcessorContext, "node", customProcessorNode);
        doCallRealMethod().when(dataProcessorContext).getNode();
        when(dataProcessorContext.getTaskDto()).thenReturn(mock(TaskDto.class));

        TapdataEvent tapdataEvent = new TapdataEvent();
        TapInsertRecordEvent event = TapInsertRecordEvent.create().init();
        Map<String, Object> after = new HashMap<>();
        after.put("id", "1");
        event.setAfter(after);
        tapdataEvent.setTapEvent(event);

        Invocable engine = ScriptUtil.getScriptEngine(
                "function process(record, form){\n" +
                        "\tcontext.op = 'd';\n" +
                        "\treturn {id: '1', name: 'deleted'};\n" +
                        "}",
                null, clientMongoOperator, null, null);
        ReflectionTestUtils.setField(processor, "engine", engine);

        List<TapdataEvent> outputEvents = new ArrayList<>();
        processor.tryProcess(tapdataEvent, (resultEvent, processResult) -> outputEvents.add(resultEvent));

        assertEquals(1, outputEvents.size());
        TapEvent resultEvent = outputEvents.get(0).getTapEvent();
        assertInstanceOf(TapDeleteRecordEvent.class, resultEvent);
        // delete op should set recordMap to before
        assertEquals("deleted", ((TapDeleteRecordEvent) resultEvent).getBefore().get("name"));
    }

    @Test
    @DisplayName("test getStateMapName returns correct format")
    void testGetStateMapName() {
        String result = (String) ReflectionTestUtils.invokeMethod(HazelcastCustomProcessor.class, "getStateMapName", "node123");
        assertEquals("HazelcastCustomProcessor-node123", result);
    }
}

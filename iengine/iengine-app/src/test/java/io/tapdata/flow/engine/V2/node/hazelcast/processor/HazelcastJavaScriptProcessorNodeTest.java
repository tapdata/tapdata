package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TransformToTapValueResult;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import com.tapdata.processor.error.ScriptProcessorExCode_30;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.ProcessorNode;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.exception.TapCodeException;
import io.tapdata.observable.logging.LogLevel;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.TaskLogger;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import javax.script.Invocable;
import javax.script.ScriptException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2023-11-21 22:26
 **/
@DisplayName("HazelcastJavaScriptProcessorNode Class Test")
class HazelcastJavaScriptProcessorNodeTest extends BaseHazelcastNodeTest {
	private HazelcastJavaScriptProcessorNode hazelcastJavaScriptProcessorNode;
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(HazelcastJavaScriptProcessorNodeTest.class);

	@BeforeEach
	protected void beforeEach() {
		super.allSetup();
		hazelcastJavaScriptProcessorNode = new HazelcastJavaScriptProcessorNode(processorBaseContext) {
		};
	}

	@Nested
	@DisplayName("Do close test")
	class DoCloseTest {
		@BeforeEach
		void beforeEach() {
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode, "obsLogger", mockObsLogger);
		}

		@Test
		@DisplayName("ThreadLocal must call remove when do close")
		void testDoCloseThreadLocal() {
			ThreadLocal<Map<String, Object>> processContextThreadLocal = ThreadLocal.withInitial(HashMap::new);
			processContextThreadLocal.get().put("test", "test");
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode, "processContextThreadLocal", processContextThreadLocal);
			hazelcastJavaScriptProcessorNode.doClose();
			Object actualObj = ReflectionTestUtils.getField(hazelcastJavaScriptProcessorNode, "processContextThreadLocal");
			assertNotNull(actualObj);
			assertTrue(actualObj instanceof ThreadLocal);
			assertEquals(HashMap.class, ((ThreadLocal<?>) actualObj).get().getClass());
			Map<String, Object> map = ((ThreadLocal<Map<String, Object>>) actualObj).get();
			assertEquals(0, map.size());
		}
	}
	@Nested
	class tryProcessTest{
		private TapdataEvent tapdataEvent;
		private BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer;
		private Invocable engine;
		private TapUpdateRecordEvent tapEvent;
		@BeforeEach
		@SneakyThrows
		void beforeEach(){
			consumer = mock(BiConsumer.class);
			ThreadLocal<Map<String, Object>> processContextThreadLocal = ThreadLocal.withInitial(HashMap::new);
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode,"processContextThreadLocal",processContextThreadLocal);
			engine = spy(ScriptUtil.getScriptEngine(
					JSEngineEnum.GRAALVM_JS.getEngineName(),
					"",
					null,
					null,
					null,
					null,
					null,
					null,
					true));
			Map<String, Invocable> engineMap = new HashMap<>();
			engineMap.put(Thread.currentThread().getName(), engine);
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode, "engineMap", engineMap);
			tapdataEvent = mock(TapdataEvent.class);
			tapEvent = mock(TapUpdateRecordEvent.class);
			when(tapEvent.getTableId()).thenReturn("tableId");
			when(tapdataEvent.getTapEvent()).thenReturn(tapEvent);
		}
		@Test
		@SneakyThrows
		void testForStandardJSHandleBefore(){
			boolean standard = true;
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode,"standard",standard);
			Map<String, Object> after = mock(HashMap.class);
			Map<String, Object> before = mock(HashMap.class);
			when(tapEvent.getAfter()).thenReturn(after);
			when(tapEvent.getBefore()).thenReturn(before);
			when(tapEvent.getType()).thenReturn(302);
			hazelcastJavaScriptProcessorNode.tryProcess(tapdataEvent, consumer);
			verify(engine,new Times(2)).invokeFunction(anyString(),any());
		}
		@Test
		@SneakyThrows
		void testForStandardJSHandleWithoutBefore(){
			boolean standard = true;
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode,"standard",standard);
			Map<String, Object> after = mock(HashMap.class);
			when(tapEvent.getAfter()).thenReturn(after);
			when(tapEvent.getBefore()).thenReturn(null);
			hazelcastJavaScriptProcessorNode.tryProcess(tapdataEvent, consumer);
			verify(engine,new Times(1)).invokeFunction(anyString(),any());
		}
		@Test
		@SneakyThrows
		void testForDelEvent(){
			boolean standard = true;
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode,"standard",standard);
			Map<String, Object> before = mock(HashMap.class);
			TapDeleteRecordEvent tapDelEvent = mock(TapDeleteRecordEvent.class);
			when(tapDelEvent.getBefore()).thenReturn(before);
			when(((TapBaseEvent) tapDelEvent).getTableId()).thenReturn("tableId");
			when(tapdataEvent.getTapEvent()).thenReturn(tapDelEvent);
			hazelcastJavaScriptProcessorNode.tryProcess(tapdataEvent, consumer);
			verify(engine,new Times(1)).invokeFunction(anyString(),any());
		}
		@Test
		@SneakyThrows
		void testForListResult(){
			boolean standard = true;
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode,"standard",standard);
			Map<String, Object> after = mock(HashMap.class);
			Map<String, Object> before = mock(HashMap.class);
			tapEvent = new TapUpdateRecordEvent();
			tapEvent.setAfter(after);
			tapEvent.setBefore(before);
			tapEvent.setTableId("tableId");
			when(tapdataEvent.getTapEvent()).thenReturn(tapEvent);
			List<Map<String ,Object>> listResult = new ArrayList<>();
			Map<String, Object> mapRes1 = new HashMap<>();
			mapRes1.put("col1",1);
			mapRes1.put("col2",1);
			Map<String, Object> mapRes2 = new HashMap<>();
			mapRes2.put("col2",1);
			mapRes2.put("col3",1);
			listResult.add(mapRes1);
			listResult.add(mapRes2);
			List<Map<String ,Object>> beforeResult = new ArrayList<>();
			Map<String, Object> mapRes3 = new HashMap<>();
			mapRes3.put("col2",2);
			mapRes3.put("col3",2);
			beforeResult.add(mapRes1);
			beforeResult.add(mapRes3);
			doReturn(listResult).doReturn(beforeResult).when(engine).invokeFunction(anyString(),any());
			when(tapdataEvent.clone()).thenReturn(tapdataEvent);
			hazelcastJavaScriptProcessorNode.tryProcess(tapdataEvent, consumer);

			ArgumentCaptor<TapdataEvent> captor = ArgumentCaptor.forClass(TapdataEvent.class);
			ArgumentCaptor<HazelcastProcessorBaseNode.ProcessResult> intCaptor = ArgumentCaptor.forClass(HazelcastProcessorBaseNode.ProcessResult.class);
			verify(consumer, new Times(2)).accept(captor.capture(), intCaptor.capture());
			TapdataEvent event = captor.getValue();
			assertEquals(tapEvent,event.getTapEvent());
			assertEquals(beforeResult.get(1),((TapUpdateRecordEvent) event.getTapEvent()).getBefore());
			assertEquals(listResult.get(1),((TapUpdateRecordEvent) event.getTapEvent()).getAfter());
		}
		@DisplayName("test tryProcess for exception JAVA_SCRIPT_PROCESS_FAILED")
		@Test
		void test5() throws ScriptException, NoSuchMethodException {
			boolean standard = true;
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode,"standard",standard);
			Map<String, Object> after = mock(HashMap.class);
			Map<String, Object> before = mock(HashMap.class);
			tapEvent = new TapUpdateRecordEvent();
			tapEvent.setAfter(after);
			tapEvent.setBefore(before);
			tapEvent.setTableId("tableId");
			when(tapdataEvent.getTapEvent()).thenReturn(tapEvent);

			doThrow(new ScriptException("a is not defeind")).when(engine).invokeFunction(anyString(),any());
			when(tapdataEvent.clone()).thenReturn(tapdataEvent);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastJavaScriptProcessorNode.tryProcess(tapdataEvent, consumer);
			});
			assertEquals(ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESS_FAILED,tapCodeException.getCode());
		}
		@DisplayName("test try tryProcess for testRun exception")
		@Test
		void test6() throws ScriptException, NoSuchMethodException {
			boolean standard = true;
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode,"standard",standard);
			Map<String, Object> after = mock(HashMap.class);
			Map<String, Object> before = mock(HashMap.class);
			tapEvent = new TapUpdateRecordEvent();
			tapEvent.setAfter(after);
			tapEvent.setBefore(before);
			tapEvent.setTableId("tableId");
			when(tapdataEvent.getTapEvent()).thenReturn(tapEvent);
			TaskDto taskDto2 = new TaskDto();
			taskDto2.setSyncType(TaskDto.SYNC_TYPE_TEST_RUN);
			when(processorBaseContext.getTaskDto()).thenReturn(taskDto2);
			doThrow(new ScriptException("a is not defeind")).when(engine).invokeFunction(anyString(),any());
			when(tapdataEvent.clone()).thenReturn(tapdataEvent);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastJavaScriptProcessorNode.tryProcess(tapdataEvent, consumer);
			});
			assertEquals(ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESS_FAILED,tapCodeException.getCode());
		}
		@Test
		@SneakyThrows
		void testForFinalJsHandleBefore(){
			boolean standard = false;
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode,"standard",standard);
			Map<String, Object> after = mock(HashMap.class);
			Map<String, Object> before = mock(HashMap.class);
			when(tapEvent.getAfter()).thenReturn(after);
			when(tapEvent.getBefore()).thenReturn(before);
			hazelcastJavaScriptProcessorNode.tryProcess(tapdataEvent, consumer);
			verify(engine,new Times(1)).invokeFunction(anyString(),any());
		}
		@Test
		@SneakyThrows
		void testForFinalJSHandleWithoutBefore(){
			boolean standard = false;
			ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode,"standard",standard);
			Map<String, Object> after = mock(HashMap.class);
			when(tapEvent.getAfter()).thenReturn(after);
			when(tapEvent.getBefore()).thenReturn(null);
			hazelcastJavaScriptProcessorNode.tryProcess(tapdataEvent, consumer);
			verify(engine,new Times(1)).invokeFunction(anyString(),any());
		}
	}

	@Test
	void testNeedCopyBatchEventWrapper(){
		Assertions.assertTrue(hazelcastJavaScriptProcessorNode.needCopyBatchEventWrapper());
	}

	@Test
	void testHandleTransformToTapValueResult() {
		TapdataEvent tapdataEvent = new TapdataEvent();
		tapdataEvent.setTransformToTapValueResult(TransformToTapValueResult.create()
				.beforeTransformedToTapValueFieldNames(new HashSet<String>() {{
					add("created");
				}})
				.afterTransformedToTapValueFieldNames(new HashSet<String>() {{
					add("created");
				}}));
		hazelcastJavaScriptProcessorNode.handleTransformToTapValueResult(tapdataEvent);
		assertNull(tapdataEvent.getTransformToTapValueResult());
	}

	@Test
	void testGetScriptObsLogger() {
		ProcessorNode node = new JsProcessorNode();
		node.setId("nodeId");
		node.setName("nodeName");
		ProcessorBaseContext processorBaseContext = ProcessorBaseContext.newBuilder()
				.withNode(node)
				.withTaskDto(new TaskDto())
				.build();
		processorBaseContext.getTaskDto().setId(new ObjectId());
		List<String> logTags = new ArrayList<>();
		logTags.add("type=test_process");
		HazelcastProcessorBaseNode processorBaseNode = new HazelcastProcessorBaseNode(processorBaseContext) {
			@Override
			protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {

			}

			private int counter = -1;
			@Override
			protected List<String> getLogTags() {
				counter++;
				if (counter == 0)
					return Collections.emptyList();
				return logTags;
			}
		};

		try (MockedStatic<ObsLoggerFactory> mockObsLoggerFactory = mockStatic(ObsLoggerFactory.class)) {

			ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
			ObsLogger logger = mock(ObsLogger.class);

			mockObsLoggerFactory.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);

			Assertions.assertEquals(obsLoggerFactory, ObsLoggerFactory.getInstance());

			doReturn(logger).when(obsLoggerFactory).getObsLogger(any(TaskDto.class), anyString(), anyString(), anyList());

			ObsLogger log = processorBaseNode.getScriptObsLogger();
			Assertions.assertNotNull(log);

			ObsLogger log1 = processorBaseNode.getScriptObsLogger();
			Assertions.assertEquals(log, log1);

			ReflectionTestUtils.setField(processorBaseNode, "scriptObsLogger", null);
			ObsLogger log2 = processorBaseNode.getScriptObsLogger();
			Assertions.assertNotNull(log2);
		}
	}

	@Test
	void testGetScriptObsLoggerWithDefaultLogTag() {
		ProcessorNode node = new JsProcessorNode();
		node.setId("nodeId");
		node.setName("nodeName");
		ProcessorBaseContext processorBaseContext = ProcessorBaseContext.newBuilder()
				.withNode(node)
				.withTaskDto(new TaskDto())
				.build();
		processorBaseContext.getTaskDto().setId(new ObjectId());
		HazelcastProcessorBaseNode processorBaseNode = new HazelcastProcessorBaseNode(processorBaseContext) {
			@Override
			protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {

			}
		};

		try (MockedStatic<ObsLoggerFactory> mockObsLoggerFactory = mockStatic(ObsLoggerFactory.class)) {

			ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
			ObsLogger logger = mock(ObsLogger.class);

			mockObsLoggerFactory.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);

			Assertions.assertEquals(obsLoggerFactory, ObsLoggerFactory.getInstance());

			doReturn(logger).when(obsLoggerFactory).getObsLogger(any(TaskDto.class), anyString(), anyString(), anyList());

			ObsLogger log = processorBaseNode.getScriptObsLogger();
			Assertions.assertNotNull(log);

		}
	}
}

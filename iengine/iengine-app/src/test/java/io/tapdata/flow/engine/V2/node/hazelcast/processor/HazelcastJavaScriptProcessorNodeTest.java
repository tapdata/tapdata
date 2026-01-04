package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.cache.scripts.ScriptCacheService;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TransformToTapValueResult;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import com.tapdata.processor.error.ScriptProcessorExCode_30;
import com.tapdata.processor.standard.ScriptStandardizationUtil;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.ProcessorNode;
import com.tapdata.tm.commons.dag.process.StandardJsProcessorNode;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
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
import org.springframework.data.mongodb.core.query.Query;
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
		@Disabled
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
	void testGetOrInitEngine(){
		HazelcastJavaScriptProcessorNode hazelcastJavaScriptProcessorNode1 = mock(HazelcastJavaScriptProcessorNode.class);
		Map<String, Invocable> engineMap=new HashMap<>();
		ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode1,"engineMap",engineMap);
		Node jsProcessorNode = mock(JsProcessorNode.class);
		when(jsProcessorNode.getId()).thenReturn("6510f74ca270a1cf5533d1ba");
		when(hazelcastJavaScriptProcessorNode1.getNode()).thenReturn(jsProcessorNode);
		JsProcessorNode jsProcessorNode1= (JsProcessorNode) jsProcessorNode;
		when(jsProcessorNode1.getScript()).thenReturn("function process(record){\n" +
				"\n" +
				"\t// Enter you code at here\n" +
				"\treturn record;\n" +
				"}");
		TaskDto taskDto1 = new TaskDto();
		taskDto1.setId(new ObjectId("6510f74ca270a1cf5533d1b9"));
		DataProcessorContext processorBaseContext1 = mock(DataProcessorContext.class);
		when(processorBaseContext1.getTaskDto()).thenReturn(taskDto1);
		when(processorBaseContext1.getNode()).thenReturn(jsProcessorNode);
		ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode1,"processorBaseContext",processorBaseContext1);
		HttpClientMongoOperator clientMongoOperator = mock(HttpClientMongoOperator.class);
		ReflectionTestUtils.setField(hazelcastJavaScriptProcessorNode1,"clientMongoOperator",clientMongoOperator);
		List<JavaScriptFunctions> javaScriptFunctions=new ArrayList<>();
		when(clientMongoOperator.find(any(Query.class),anyString(),any(Class.class))).thenReturn(javaScriptFunctions);
		doCallRealMethod().when(hazelcastJavaScriptProcessorNode1).getOrInitEngine();
		try(MockedStatic<ScriptUtil> scriptUtilMockedStatic = mockStatic(ScriptUtil.class);){
			scriptUtilMockedStatic.when(()->{
				ScriptUtil.getScriptEngine(anyString(),anyString(),anyList(),any(HttpClientMongoOperator.class),eq(null),eq(null),any(ScriptCacheService.class),any(ObsScriptLogger.class),eq(false));
			}).thenThrow(new ScriptException("get failed"));
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastJavaScriptProcessorNode1.getOrInitEngine();
			});
			assertEquals(ScriptProcessorExCode_30.JAVA_SCRIPT_PROCESSOR_GET_SCRIPT_FAILED,tapCodeException.getCode());
		}


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

	@Nested
	@DisplayName("getTapEvent method test")
	class GetTapEventTest {

		@Test
		@DisplayName("test getTapEvent when op is same as original event - should return original event")
		void testGetTapEventWhenOpIsSame() throws Exception {
			// Arrange
			TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create();
			insertEvent.setTableId("test_table");
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("name", "test");
			insertEvent.setAfter(after);

			String op = "i"; // INSERT operation
			Map<String, Object> before = null;

			// Act
			TapEvent result = ReflectionTestUtils.invokeMethod(
					hazelcastJavaScriptProcessorNode,
					"getTapEvent",
					insertEvent,
					op,
					before
			);

			// Assert
			assertNotNull(result);
			assertSame(insertEvent, result, "Should return the same event when op matches");
		}

		@Test
		@DisplayName("test getTapEvent when converting INSERT to UPDATE")
		void testGetTapEventConvertInsertToUpdate() throws Exception {
			// Arrange
			TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create();
			insertEvent.setTableId("test_table");
			insertEvent.setReferenceTime(System.currentTimeMillis());
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("name", "test");
			insertEvent.setAfter(after);

			String op = "u"; // UPDATE operation
			Map<String, Object> before = new HashMap<>();
			before.put("id", 1);
			before.put("name", "old_name");

			// Act
			TapEvent result = ReflectionTestUtils.invokeMethod(
					hazelcastJavaScriptProcessorNode,
					"getTapEvent",
					insertEvent,
					op,
					before
			);

			// Assert
			assertNotNull(result);
			assertTrue(result instanceof TapUpdateRecordEvent, "Should convert to TapUpdateRecordEvent");
			TapUpdateRecordEvent updateEvent = (TapUpdateRecordEvent) result;
			assertEquals("test_table", updateEvent.getTableId());
			assertEquals(before, updateEvent.getBefore(), "Before map should be set from parameter");
			assertNotNull(updateEvent.getReferenceTime(), "Should clone properties from original event");
		}

		@Test
		@DisplayName("test getTapEvent when converting UPDATE to INSERT")
		void testGetTapEventConvertUpdateToInsert() throws Exception {
			// Arrange
			TapUpdateRecordEvent updateEvent = TapUpdateRecordEvent.create();
			updateEvent.setTableId("test_table");
			updateEvent.setReferenceTime(System.currentTimeMillis());
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("name", "new_name");
			updateEvent.setAfter(after);
			Map<String, Object> before = new HashMap<>();
			before.put("id", 1);
			before.put("name", "old_name");
			updateEvent.setBefore(before);

			String op = "i"; // INSERT operation
			Map<String, Object> contextBefore = null;

			// Act
			TapEvent result = ReflectionTestUtils.invokeMethod(
					hazelcastJavaScriptProcessorNode,
					"getTapEvent",
					updateEvent,
					op,
					contextBefore
			);

			// Assert
			assertNotNull(result);
			assertTrue(result instanceof TapInsertRecordEvent, "Should convert to TapInsertRecordEvent");
			TapInsertRecordEvent insertEvent = (TapInsertRecordEvent) result;
			assertEquals("test_table", insertEvent.getTableId());
			assertNotNull(insertEvent.getReferenceTime(), "Should clone properties from original event");
		}

		@Test
		@DisplayName("test getTapEvent when converting UPDATE to DELETE")
		void testGetTapEventConvertUpdateToDelete() throws Exception {
			// Arrange
			TapUpdateRecordEvent updateEvent = TapUpdateRecordEvent.create();
			updateEvent.setTableId("test_table");
			updateEvent.setReferenceTime(System.currentTimeMillis());
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("name", "new_name");
			updateEvent.setAfter(after);
			Map<String, Object> before = new HashMap<>();
			before.put("id", 1);
			before.put("name", "old_name");
			updateEvent.setBefore(before);

			String op = "d"; // DELETE operation
			Map<String, Object> contextBefore = null;

			// Act
			TapEvent result = ReflectionTestUtils.invokeMethod(
					hazelcastJavaScriptProcessorNode,
					"getTapEvent",
					updateEvent,
					op,
					contextBefore
			);

			// Assert
			assertNotNull(result);
			assertTrue(result instanceof TapDeleteRecordEvent, "Should convert to TapDeleteRecordEvent");
			TapDeleteRecordEvent deleteEvent = (TapDeleteRecordEvent) result;
			assertEquals("test_table", deleteEvent.getTableId());
			assertNotNull(deleteEvent.getReferenceTime(), "Should clone properties from original event");
		}

		@Test
		@DisplayName("test getTapEvent when converting DELETE to INSERT")
		void testGetTapEventConvertDeleteToInsert() throws Exception {
			// Arrange
			TapDeleteRecordEvent deleteEvent = TapDeleteRecordEvent.create();
			deleteEvent.setTableId("test_table");
			deleteEvent.setReferenceTime(System.currentTimeMillis());
			Map<String, Object> before = new HashMap<>();
			before.put("id", 1);
			before.put("name", "deleted_name");
			deleteEvent.setBefore(before);

			String op = "i"; // INSERT operation
			Map<String, Object> contextBefore = null;

			// Act
			TapEvent result = ReflectionTestUtils.invokeMethod(
					hazelcastJavaScriptProcessorNode,
					"getTapEvent",
					deleteEvent,
					op,
					contextBefore
			);

			// Assert
			assertNotNull(result);
			assertTrue(result instanceof TapInsertRecordEvent, "Should convert to TapInsertRecordEvent");
			TapInsertRecordEvent insertEvent = (TapInsertRecordEvent) result;
			assertEquals("test_table", insertEvent.getTableId());
			assertNotNull(insertEvent.getReferenceTime(), "Should clone properties from original event");
		}

		@Test
		@DisplayName("test getTapEvent when converting non-INSERT to UPDATE with before data")
		void testGetTapEventConvertToUpdateWithBeforeFromContext() throws Exception {
			// Arrange
			TapDeleteRecordEvent deleteEvent = TapDeleteRecordEvent.create();
			deleteEvent.setTableId("test_table");
			deleteEvent.setReferenceTime(System.currentTimeMillis());
			Map<String, Object> before = new HashMap<>();
			before.put("id", 1);
			before.put("name", "deleted_name");
			deleteEvent.setBefore(before);

			String op = "u"; // UPDATE operation
			Map<String, Object> contextBefore = new HashMap<>();
			contextBefore.put("id", 1);
			contextBefore.put("name", "context_before_name");

			// Act
			TapEvent result = ReflectionTestUtils.invokeMethod(
					hazelcastJavaScriptProcessorNode,
					"getTapEvent",
					deleteEvent,
					op,
					contextBefore
			);

			// Assert
			assertNotNull(result);
			assertTrue(result instanceof TapUpdateRecordEvent, "Should convert to TapUpdateRecordEvent");
			TapUpdateRecordEvent updateEvent = (TapUpdateRecordEvent) result;
			assertEquals("test_table", updateEvent.getTableId());
			assertEquals(contextBefore, updateEvent.getAfter(), "After should be set from context before when original is not INSERT");
			assertNotNull(updateEvent.getReferenceTime(), "Should clone properties from original event");
		}

		@Test
		@DisplayName("test getTapEvent with unsupported operation type - should throw exception")
		void testGetTapEventWithUnsupportedOperation() {
			// Arrange
			TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create();
			insertEvent.setTableId("test_table");
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			insertEvent.setAfter(after);

			String op = "invalid_op"; // Invalid operation
			Map<String, Object> before = null;

			// Act & Assert
			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
				ReflectionTestUtils.invokeMethod(
						hazelcastJavaScriptProcessorNode,
						"getTapEvent",
						insertEvent,
						op,
						before
				);
			});

			assertTrue(exception.getMessage().contains("Unsupported operation type"),
					"Should throw exception with unsupported operation message");
		}

		@Test
		@DisplayName("test getTapEvent preserves event properties after conversion")
		void testGetTapEventPreservesEventProperties() throws Exception {
			// Arrange
			TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create();
			insertEvent.setTableId("test_table");
			long referenceTime = System.currentTimeMillis();
			insertEvent.setReferenceTime(referenceTime);
			Map<String, Object> info = new HashMap<>();
			info.put("source", "mysql");
			insertEvent.setInfo(info);
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			insertEvent.setAfter(after);

			String op = "d"; // DELETE operation
			Map<String, Object> before = null;

			// Act
			TapEvent result = ReflectionTestUtils.invokeMethod(
					hazelcastJavaScriptProcessorNode,
					"getTapEvent",
					insertEvent,
					op,
					before
			);

			// Assert
			assertNotNull(result);
			assertTrue(result instanceof TapDeleteRecordEvent, "Should convert to TapDeleteRecordEvent");
			TapDeleteRecordEvent deleteEvent = (TapDeleteRecordEvent) result;
			assertEquals("test_table", deleteEvent.getTableId(), "Should preserve table ID");
			assertEquals(referenceTime, deleteEvent.getReferenceTime(), "Should preserve reference time");
			assertEquals(info, deleteEvent.getInfo(), "Should preserve info map");
		}
	}
}

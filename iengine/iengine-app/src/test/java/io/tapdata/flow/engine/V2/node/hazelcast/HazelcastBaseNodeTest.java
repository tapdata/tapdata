package io.tapdata.flow.engine.V2.node.hazelcast;

import base.BaseTest;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.tapdata.constant.BeanUtil;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.MockTaskUtil;
import io.tapdata.aspect.DataNodeInitAspect;
import io.tapdata.aspect.ProcessorNodeInitAspect;
import io.tapdata.common.SettingService;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.error.TapProcessorUnknownException;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.monitor.Monitor;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.monitor.impl.JetJobStatusMonitor;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.aggregation.HazelcastMultiAggregatorProcessor;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.internal.verification.Times;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2023-11-11 15:41
 **/
@RunWith(MockitoJUnitRunner.class)
class HazelcastBaseNodeTest extends BaseTest {
	private ProcessorBaseContext processorBaseContext;
	private TableNode tableNode;
	private TaskDto taskDto;
	private HazelcastBaseNode hazelcastBaseNode;
	private HazelcastBaseNode mockHazelcastBaseNode;

	@BeforeEach
	void beforeEach() {
		// Mock task and node data
		taskDto = MockTaskUtil.setUpTaskDtoByJsonFile();
		tableNode = (TableNode) taskDto.getDag().getNodes().get(0);

		// Mock some common object
		processorBaseContext = mock(ProcessorBaseContext.class);
		when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
		when(processorBaseContext.getNode()).thenReturn((Node) tableNode);
		mockHazelcastBaseNode = mock(HazelcastBaseNode.class);

		ReflectionTestUtils.setField(mockHazelcastBaseNode, "processorBaseContext", processorBaseContext);
		hazelcastBaseNode = new HazelcastBaseNode(processorBaseContext) {
		};
	}

	@Nested
	class HazelcastBaseNodeInitTest {

		Processor.Context context;
		ExternalStorageDto externalStorageDto;
		AtomicBoolean doInit = new AtomicBoolean(false);
		AtomicBoolean doInitWithDisableNode = new AtomicBoolean(false);

		@BeforeEach
		void beforeEach() {
			context = mock(Processor.Context.class);
			externalStorageDto = mock(ExternalStorageDto.class);
			when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
			when(processorBaseContext.getNode()).thenReturn((Node) tableNode);
			when(processorBaseContext.getEdges()).thenReturn(taskDto.getDag().getEdges());
			when(processorBaseContext.getNodes()).thenReturn(taskDto.getDag().getNodes());

			when(mockHazelcastBaseNode.initClientMongoOperator()).thenReturn(mockClientMongoOperator);
			when(mockHazelcastBaseNode.initSettingService()).thenReturn(mockSettingService);
			when(mockHazelcastBaseNode.initObsLogger()).thenReturn(mockObsLogger);
			when(mockHazelcastBaseNode.initExternalStorage()).thenReturn(externalStorageDto);

			ReflectionTestUtils.setField(mockHazelcastBaseNode, "running", new AtomicBoolean(false));
			when(processorBaseContext.getConfigurationCenter()).thenReturn(mockConfigurationCenter);
			doNothing().when(mockHazelcastBaseNode).executeAspectOnInit();
			doNothing().when(mockHazelcastBaseNode).startMonitorIfNeed(context);
			when(mockHazelcastBaseNode.initFilterCodec()).thenCallRealMethod();
			when(mockHazelcastBaseNode.getNode()).thenReturn((Node) tableNode);
			when(mockHazelcastBaseNode.initMonitor()).thenCallRealMethod();
			doAnswer(invocationOnMock -> doInit.compareAndSet(false, true)).when(mockHazelcastBaseNode).doInit(context);
			doAnswer(invocationOnMock -> doInitWithDisableNode.compareAndSet(false, true)).when(mockHazelcastBaseNode).doInitWithDisableNode(context);
			doAnswer(invocationOnMock -> {
				throw (Throwable) invocationOnMock.getArgument(0);
			}).when(mockHazelcastBaseNode).errorHandle(any(Throwable.class));
		}

		@Test
		void testHazelcastBaseNodeDefaultVariables() {
			assertNotNull(hazelcastBaseNode.getProcessorBaseContext());
			ProcessorBaseContext actualProcessorBaseContext = hazelcastBaseNode.getProcessorBaseContext();
			assertEquals(processorBaseContext, actualProcessorBaseContext);
			assertNotNull(hazelcastBaseNode.getNode());
		}

		@Test
		void testDoInit() {
			doAnswer(invocationOnMock -> doInit.compareAndSet(false, true)).when(mockHazelcastBaseNode).doInit(context);
			mockHazelcastBaseNode.doInit(context);
			assertTrue(doInit.get());
		}

		@Test
		void testDoInitWithDisableNode() {
			doAnswer(invocationOnMock -> doInitWithDisableNode.compareAndSet(false, true)).when(mockHazelcastBaseNode).doInitWithDisableNode(context);
			mockHazelcastBaseNode.doInitWithDisableNode(context);
			assertTrue(doInitWithDisableNode.get());
		}

		@Test
		@SneakyThrows
		void testInit() {
			doCallRealMethod().when(mockHazelcastBaseNode).init(context);
			mockHazelcastBaseNode.init(context);

			assertEquals(context, ReflectionTestUtils.getField(mockHazelcastBaseNode, "jetContext"));
			Object runningObj = ReflectionTestUtils.getField(mockHazelcastBaseNode, "running");
			assertNotNull(runningObj);
			assertEquals(AtomicBoolean.class, runningObj.getClass());
			assertTrue(((AtomicBoolean) runningObj).get());
			assertEquals(mockObsLogger, mockHazelcastBaseNode.obsLogger);
			assertEquals(mockClientMongoOperator, mockHazelcastBaseNode.clientMongoOperator);
			assertEquals(mockSettingService, mockHazelcastBaseNode.settingService);
			assertEquals(externalStorageDto, mockHazelcastBaseNode.externalStorageDto);
			assertNotNull(mockHazelcastBaseNode.codecsFilterManager);
			assertNotNull(mockHazelcastBaseNode.monitorManager);
			assertTrue(doInit.get());
			assertFalse(doInitWithDisableNode.get());
		}

		@Test
		@SneakyThrows
		void testInitWithOutConfigureCenter() {
			when(processorBaseContext.getConfigurationCenter()).thenReturn(null);
			doCallRealMethod().when(mockHazelcastBaseNode).init(context);
			assertThrows(TapCodeException.class, () -> mockHazelcastBaseNode.init(context));
		}

		@Test
		@SneakyThrows
		void testInitWhenNodeGraphIsNull() {
			tableNode.setGraph(null);
			TaskDto taskDto1 = MockTaskUtil.setUpTaskDtoByJsonFile();
			taskDto1.setDag(null);
			when(processorBaseContext.getTaskDto()).thenReturn(taskDto1);
			doCallRealMethod().when(mockHazelcastBaseNode).init(context);
			mockHazelcastBaseNode.init(context);
			assertNotNull(processorBaseContext.getTaskDto().getDag());
		}

		@Test
		@SneakyThrows
		void testInitDisableNode() {
			Map<String, Object> attrs = new HashMap<>();
			attrs.put("disabled", true);
			tableNode.setAttrs(attrs);
			doCallRealMethod().when(mockHazelcastBaseNode).init(context);
			mockHazelcastBaseNode.init(context);
			assertFalse(doInit.get());
			assertTrue(doInitWithDisableNode.get());
		}

		@Test
		@SneakyThrows
		void testInitHaveError() {
			TapCodeException mockTapEx = new TapCodeException(TaskProcessorExCode_11.UNKNOWN_ERROR);
			when(mockHazelcastBaseNode.initObsLogger()).thenThrow(mockTapEx);
			when(mockHazelcastBaseNode.errorHandle(any(RuntimeException.class))).thenCallRealMethod();
			ReflectionTestUtils.setField(mockHazelcastBaseNode, "error", mockTapEx);
			// Execute init function
			doCallRealMethod().when(mockHazelcastBaseNode).init(any(Processor.Context.class));
			mockHazelcastBaseNode.init(context);
			verify(mockHazelcastBaseNode, new Times(1)).errorHandle(mockTapEx);
		}
	}

	@Nested
	class ExecuteAspectTest {

		private AtomicBoolean consumed;
		private AtomicBoolean observed;
		private AlwaysIntercept alwaysIntercept;
		private NeverIntercept neverIntercept;
		private AspectObserver<MockDataFunctionAspect> aspectObserver;
		private AspectManager aspectManager;

		@BeforeEach
		void beforeEach() {
			consumed = new AtomicBoolean(false);
			observed = new AtomicBoolean(false);
			aspectObserver = aspect -> observed.compareAndSet(false, true);
			alwaysIntercept = new AlwaysIntercept();
			neverIntercept = new NeverIntercept();
			aspectManager = InstanceFactory.instance(AspectManager.class);
		}

		@Test
		void testExecuteDataFuncAspectNotIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			aspectManager.registerAspectInterceptor(MockDataFunctionAspect.class, 1, neverIntercept);
			AspectInterceptResult actual = hazelcastBaseNode.executeDataFuncAspect(
					MockDataFunctionAspect.class,
					MockDataFunctionAspect::new,
					mockAspect -> consumed.compareAndSet(false, true)
			);
			assertNotNull(actual);
			assertFalse(actual.isIntercepted());
			assertTrue(consumed.get());
			assertTrue(observed.get());
		}

		@Test
		void testExecuteDataFuncAspectIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			aspectManager.registerAspectInterceptor(MockDataFunctionAspect.class, 1, alwaysIntercept);
			AspectInterceptResult actual = hazelcastBaseNode.executeDataFuncAspect(
					MockDataFunctionAspect.class,
					MockDataFunctionAspect::new,
					mockAspect -> consumed.compareAndSet(false, true)
			);
			assertNotNull(actual);
			assertTrue(actual.isIntercepted());
			assertFalse(consumed.get());
			assertFalse(observed.get());
		}

		@Test
		void testExecuteDataFuncAspectWithoutIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			AspectInterceptResult actual = hazelcastBaseNode.executeDataFuncAspect(
					MockDataFunctionAspect.class,
					MockDataFunctionAspect::new,
					mockAspect -> consumed.compareAndSet(false, true)
			);
			assertNull(actual);
			assertTrue(consumed.get());
			assertTrue(observed.get());
		}

		@Test
		void testExecuteDataFuncAspectWithoutObserve() {
			AspectInterceptResult actual = hazelcastBaseNode.executeDataFuncAspect(
					MockDataFunctionAspect.class,
					MockDataFunctionAspect::new,
					mockAspect -> consumed.compareAndSet(false, true)
			);
			assertNull(actual);
			assertTrue(consumed.get());
			assertFalse(observed.get());
		}

		@Test
		void testExecuteAspectOneParamNotIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			aspectManager.registerAspectInterceptor(MockDataFunctionAspect.class, 1, neverIntercept);
			MockDataFunctionAspect mockDataFunctionAspect = new MockDataFunctionAspect();
			AspectInterceptResult actual = hazelcastBaseNode.executeAspect(mockDataFunctionAspect);
			assertNotNull(actual);
			assertFalse(actual.isIntercepted());
			assertTrue(observed.get());
		}

		@Test
		void testExecuteAspectOneParamIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			aspectManager.registerAspectInterceptor(MockDataFunctionAspect.class, 1, alwaysIntercept);
			MockDataFunctionAspect mockDataFunctionAspect = new MockDataFunctionAspect();
			AspectInterceptResult actual = hazelcastBaseNode.executeAspect(mockDataFunctionAspect);
			assertNotNull(actual);
			assertTrue(actual.isIntercepted());
			assertFalse(observed.get());
		}

		@Test
		void testExecuteAspectTwoParamNotIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			aspectManager.registerAspectInterceptor(MockDataFunctionAspect.class, 1, neverIntercept);
			AspectInterceptResult actual = hazelcastBaseNode.executeAspect(MockDataFunctionAspect.class, MockDataFunctionAspect::new);
			assertNull(actual);
			assertTrue(observed.get());
		}

		@Test
		void testExecuteAspectTwoParamIntercept() {
			aspectManager.registerAspectObserver(MockDataFunctionAspect.class, 1, aspectObserver);
			aspectManager.registerAspectInterceptor(MockDataFunctionAspect.class, 1, alwaysIntercept);
			AspectInterceptResult actual = hazelcastBaseNode.executeAspect(MockDataFunctionAspect.class, MockDataFunctionAspect::new);
			assertNotNull(actual);
			assertTrue(actual.isIntercepted());
			assertFalse(observed.get());
		}

		@AfterEach
		void afterEach() {
			aspectManager.unregisterAspectObserver(MockDataFunctionAspect.class, aspectObserver);
			aspectManager.unregisterAspectInterceptor(MockDataFunctionAspect.class, alwaysIntercept);
			aspectManager.unregisterAspectInterceptor(MockDataFunctionAspect.class, neverIntercept);
		}
	}

	@Nested
	class WrapTapCodeExceptionMethodTest {
		@Test
		void testWrapTapCodeExceptionInputTapCodeException() {
			TapCodeException tapCodeException = new TapCodeException("test");
			TapCodeException actual = HazelcastBaseNode.wrapTapCodeException(tapCodeException);
			assertNotNull(actual);
			assertEquals(tapCodeException, actual);
		}

		@Test
		void testWrapTapCodeExceptionInputRuntimeException() {
			RuntimeException runtimeException = new RuntimeException();
			TapCodeException actual = HazelcastBaseNode.wrapTapCodeException(runtimeException);
			assertNotNull(actual);
			assertEquals(TapProcessorUnknownException.class, actual.getClass());
			assertNotNull(actual.getCause());
			assertEquals(runtimeException, actual.getCause());
		}

		@Test
		void testWrapTapCodeExceptionInputNull() {
			Throwable actual = assertThrows(IllegalArgumentException.class, () -> HazelcastBaseNode.wrapTapCodeException(null));
			assertNotNull(actual.getMessage());
			assertEquals("Input exception cannot be null", actual.getMessage());
		}
	}

	@Nested
	class SetThreadNameTest {

		@BeforeEach
		void beforeEach() {
			when(processorBaseContext.getNode()).thenReturn((Node) tableNode);
			when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
		}

		@Test
		void testSetThreadName() {
			hazelcastBaseNode.setThreadName();
			String actual = Thread.currentThread().getName();
			assertEquals("HazelcastBaseNode-dummy2dummy(6555b257407e2d16ae88c5ad)-dummy_test(2cbc1a4d-906d-4b32-9cf4-6596ed4bd0e4)", actual);
		}
	}

	@Nested
	class InitFilterCodecTest {
		@Test
		void testInitFilterCodec() {
			TapCodecsFilterManager actual = hazelcastBaseNode.initFilterCodec();
			assertNotNull(actual);
			assertEquals(TapCodecsFilterManager.class, actual.getClass());
		}
	}

	@Nested
	class TransformFromTapValueTest {
		final Long TIME = 1700015312781L;
		final String TABLE_NAME = "test_table";
		final ObjectId objectId = new ObjectId("65546c165006f2da7ba64c73");
		final Map<String, Object> BEFORE_MOCK_DATA = new HashMap<String, Object>() {{
			put("_id", new TapStringValue(objectId.toHexString()).originValue(objectId).tapType(new TapString()).originType("ObjectID"));
			put("id", new TapNumberValue(1D));
			put("name", new TapStringValue("test"));
			put("insert_dte", new TapDateTimeValue(new DateTime(TIME, 3)));
		}};
		final Map<String, Object> AFTER_MOCK_DATA = new HashMap<String, Object>() {{
			put("_id", new TapStringValue(objectId.toHexString()).originValue(objectId).tapType(new TapString()).originType("ObjectID"));
			put("id", new TapNumberValue(1D));
			put("name", new TapStringValue("test1"));
			put("insert_dte", new TapDateTimeValue(new DateTime(TIME, 3)));
		}};
		TapInsertRecordEvent tapInsertRecordEvent;
		TapUpdateRecordEvent tapUpdateRecordEvent;

		@BeforeEach
		void beforeEach() {
			ReflectionTestUtils.setField(hazelcastBaseNode, "codecsFilterManager", ReflectionTestUtils.invokeMethod(hazelcastBaseNode, "initFilterCodec"));
			tapInsertRecordEvent = mock(TapInsertRecordEvent.class);
			when(tapInsertRecordEvent.getAfter()).thenReturn(AFTER_MOCK_DATA);
			when(tapInsertRecordEvent.getTableId()).thenReturn(TABLE_NAME);
			when(tapInsertRecordEvent.getReferenceTime()).thenReturn(TIME);

			tapUpdateRecordEvent = mock(TapUpdateRecordEvent.class);
			when(tapUpdateRecordEvent.getBefore()).thenReturn(BEFORE_MOCK_DATA);
			when(tapUpdateRecordEvent.getAfter()).thenReturn(AFTER_MOCK_DATA);
			when(tapUpdateRecordEvent.getTableId()).thenReturn(TABLE_NAME);
			when(tapUpdateRecordEvent.getReferenceTime()).thenReturn(TIME);
		}

		@Test
		void testTransformFromTapValueNullTapdataEvent() {
			HazelcastBaseNode.TapValueTransform actual = hazelcastBaseNode.transformFromTapValue(null);
			assertNull(actual);
		}

		@Test
		void testTransformFromTapValueNullTapEvent() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			HazelcastBaseNode.TapValueTransform actual = hazelcastBaseNode.transformFromTapValue(tapdataEvent);
			assertNull(actual);
		}

		@Test
		void testTransformFromTapValueTapInsertRecordEvent() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			HazelcastBaseNode.TapValueTransform actual = hazelcastBaseNode.transformFromTapValue(tapdataEvent);
			assertNotNull(actual);
			assertNotNull(actual.getAfter());
			assertFalse(actual.getAfter().isEmpty());
			assertEquals(TapStringValue.class, actual.getAfter().get("_id").getClass());
			TapStringValue oid = (TapStringValue) actual.getAfter().get("_id");
			assertEquals(objectId, oid.getOriginValue());
			assertEquals("ObjectID", oid.getOriginType());
			assertNull(actual.getBefore());
			assertEquals(tapInsertRecordEvent, tapdataEvent.getTapEvent());
			assertEquals(Double.class, ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("id").getClass());
			assertEquals(String.class, ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("name").getClass());
			assertEquals(Instant.class, ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("insert_dte").getClass());
		}

		@Test
		void testTransformFromTapValueTapUpdateRecordEvent() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			HazelcastBaseNode.TapValueTransform actual = hazelcastBaseNode.transformFromTapValue(tapdataEvent);
			assertNotNull(actual);
			assertNotNull(actual.getBefore());
			assertFalse(actual.getBefore().isEmpty());
			assertEquals(TapStringValue.class, actual.getBefore().get("_id").getClass());
			TapStringValue beforeOid = (TapStringValue) actual.getBefore().get("_id");
			assertEquals(objectId, beforeOid.getOriginValue());
			assertEquals("ObjectID", beforeOid.getOriginType());
			assertNotNull(actual.getAfter());
			assertFalse(actual.getAfter().isEmpty());
			assertEquals(TapStringValue.class, actual.getAfter().get("_id").getClass());
			TapStringValue afterOid = (TapStringValue) actual.getAfter().get("_id");
			assertEquals(objectId, afterOid.getOriginValue());
			assertEquals("ObjectID", afterOid.getOriginType());
			assertEquals(tapUpdateRecordEvent, tapdataEvent.getTapEvent());
			assertEquals(Double.class, ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("id").getClass());
			assertEquals(String.class, ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("name").getClass());
			assertEquals(Instant.class, ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("insert_dte").getClass());
			assertEquals(Double.class, ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("id").getClass());
			assertEquals(String.class, ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("name").getClass());
			assertEquals(Instant.class, ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("insert_dte").getClass());
		}

		@Test
		void testTransformFromTapValueTapUpdateRecordEventWithoutAfter() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			when(tapUpdateRecordEvent.getAfter()).thenReturn(null);
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			HazelcastBaseNode.TapValueTransform actual = hazelcastBaseNode.transformFromTapValue(tapdataEvent);
			assertNotNull(actual);
			assertNull(actual.getAfter());
		}
	}

	@Nested
	class TransformToTapValueTest {
		final Long TIME = 1700015312781L;
		final String TABLE_NAME = "test_table";
		final ObjectId objectId = new ObjectId("65546c165006f2da7ba64c73");
		final Map<String, Object> BEFORE_MOCK_DATA = new HashMap<String, Object>() {{
			put("_id", objectId.toHexString());
			put("id", 1D);
			put("name", "test");
			put("insert_dte", Instant.ofEpochMilli(TIME));
		}};
		final Map<String, Object> AFTER_MOCK_DATA = new HashMap<String, Object>() {{
			put("_id", objectId.toHexString());
			put("id", 1D);
			put("name", "test");
			put("insert_dte", Instant.ofEpochMilli(TIME));
		}};
		TapInsertRecordEvent tapInsertRecordEvent;
		TapUpdateRecordEvent tapUpdateRecordEvent;
		TapDeleteRecordEvent tapDeleteRecordEvent;
		TapTableMap<String, TapTable> tapTableMap;
		TapTable tapTable;

		@BeforeEach
		void beforeEach() {
			ReflectionTestUtils.setField(hazelcastBaseNode, "codecsFilterManager", ReflectionTestUtils.invokeMethod(hazelcastBaseNode, "initFilterCodec"));
			tapInsertRecordEvent = mock(TapInsertRecordEvent.class);
			when(tapInsertRecordEvent.getAfter()).thenReturn(AFTER_MOCK_DATA);
			when(tapInsertRecordEvent.getTableId()).thenReturn(TABLE_NAME);
			when(tapInsertRecordEvent.getReferenceTime()).thenReturn(TIME);

			tapUpdateRecordEvent = mock(TapUpdateRecordEvent.class);
			when(tapUpdateRecordEvent.getAfter()).thenReturn(AFTER_MOCK_DATA);
			when(tapUpdateRecordEvent.getBefore()).thenReturn(BEFORE_MOCK_DATA);
			when(tapUpdateRecordEvent.getTableId()).thenReturn(TABLE_NAME);
			when(tapUpdateRecordEvent.getReferenceTime()).thenReturn(TIME);

			tapDeleteRecordEvent = mock(TapDeleteRecordEvent.class);
			when(tapDeleteRecordEvent.getBefore()).thenReturn(BEFORE_MOCK_DATA);
			when(tapDeleteRecordEvent.getTableId()).thenReturn(TABLE_NAME);
			when(tapDeleteRecordEvent.getReferenceTime()).thenReturn(TIME);

			tapTable = new TapTable(TABLE_NAME);
			tapTable.setNameFieldMap(new LinkedHashMap<String, TapField>() {{
				put("_id", new TapField("_id", "ObjectID").tapType(new TapString()));
				put("id", new TapField("id", "Double").tapType(new TapNumber()));
				put("name", new TapField("name", "String").tapType(new TapString()));
				put("insert_dte", new TapField("insert_dte", "Date").tapType(new TapDateTime()));
			}});

			tapTableMap = mock(TapTableMap.class);
			when(tapTableMap.get(TABLE_NAME)).thenReturn(tapTable);
		}

		@Test
		void testTransformToTapValueNullTapTableMap() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			assertThrows(IllegalArgumentException.class, () -> hazelcastBaseNode.transformToTapValue(tapdataEvent, null, TABLE_NAME));
		}

		@Test
		void testTransformToTapValueNullTapTable() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			when(tapTableMap.get(TABLE_NAME)).thenReturn(null);
			assertThrows(IllegalArgumentException.class, () -> hazelcastBaseNode.transformToTapValue(tapdataEvent, tapTableMap, TABLE_NAME));
		}

		@Test
		void testTransformToTapValueNullNameFieldMap() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			tapTable.setNameFieldMap(null);
			assertThrows(IllegalArgumentException.class, () -> hazelcastBaseNode.transformToTapValue(tapdataEvent, tapTableMap, TABLE_NAME));
		}

		/**
		 * When not a TapRecordEvent then do nothing on TapdataEvent
		 */
		@Test
		void testTransformToTapValueNotTapRecordEvent() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			TapCreateTableEvent tapCreateTableEvent = new TapCreateTableEvent();
			tapdataEvent.setTapEvent(tapCreateTableEvent);
			hazelcastBaseNode.transformToTapValue(tapdataEvent, tapTableMap, TABLE_NAME);
			assertEquals(tapCreateTableEvent, tapdataEvent.getTapEvent());
		}

		@Test
		void testTransformToTapValueThreeParamWhenInsert() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			hazelcastBaseNode.transformToTapValue(tapdataEvent, tapTableMap, TABLE_NAME);
			assertEquals(tapInsertRecordEvent, tapdataEvent.getTapEvent());
			assertEquals(4, ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter().size());
			Object actualOid = ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("_id");
			assertEquals(String.class, actualOid.getClass());
			assertEquals(objectId.toHexString(), actualOid);
			assertEquals(1D, ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("id"));
			assertEquals("test", ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("name"));
			Object actualInsertDteObj = ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("insert_dte");
			assertEquals(TapDateTimeValue.class, actualInsertDteObj.getClass());
			TapDateTimeValue actualInsertDte = (TapDateTimeValue) actualInsertDteObj;
			assertEquals(new DateTime(TIME), actualInsertDte.getValue());
			assertEquals(Instant.ofEpochMilli(TIME), actualInsertDte.getOriginValue());
		}

		@Test
		void testTransformToTapValueThreeParamWhenUpdate() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			hazelcastBaseNode.transformToTapValue(tapdataEvent, tapTableMap, TABLE_NAME);
			assertEquals(tapUpdateRecordEvent, tapdataEvent.getTapEvent());
			assertEquals(4, ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getBefore().size());
			Object actualOid = ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("_id");
			assertEquals(String.class, actualOid.getClass());
			assertEquals(objectId.toHexString(), actualOid);
			assertEquals(1D, ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("id"));
			assertEquals("test", ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("name"));
			Object actualInsertDteObj = ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("insert_dte");
			assertEquals(TapDateTimeValue.class, actualInsertDteObj.getClass());
			TapDateTimeValue actualInsertDte = (TapDateTimeValue) actualInsertDteObj;
			assertEquals(new DateTime(TIME), actualInsertDte.getValue());
			assertEquals(Instant.ofEpochMilli(TIME), actualInsertDte.getOriginValue());
		}

		@Test
		void testTransformToTapValueThreeParamWhenDelete() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapDeleteRecordEvent);
			hazelcastBaseNode.transformToTapValue(tapdataEvent, tapTableMap, TABLE_NAME);
			assertEquals(tapDeleteRecordEvent, tapdataEvent.getTapEvent());
			assertEquals(4, ((TapDeleteRecordEvent) tapdataEvent.getTapEvent()).getBefore().size());
			Object actualOid = ((TapDeleteRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("_id");
			assertEquals(String.class, actualOid.getClass());
			assertEquals(objectId.toHexString(), actualOid);
			assertEquals(1D, ((TapDeleteRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("id"));
			assertEquals("test", ((TapDeleteRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("name"));
			Object actualInsertDteObj = ((TapDeleteRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("insert_dte");
			assertEquals(TapDateTimeValue.class, actualInsertDteObj.getClass());
			TapDateTimeValue actualInsertDte = (TapDateTimeValue) actualInsertDteObj;
			assertEquals(new DateTime(TIME), actualInsertDte.getValue());
			assertEquals(Instant.ofEpochMilli(TIME), actualInsertDte.getOriginValue());
		}

		@Test
		void testTransformToTapValueFourParamWhenInsert() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			HazelcastBaseNode.TapValueTransform tapValueTransform = new HazelcastBaseNode.TapValueTransform();
			tapValueTransform.after(new HashMap<String, TapValue<?, ?>>() {{
				put("_id", new TapStringValue(objectId.toHexString()).originValue(objectId).tapType(new TapString()).originType("ObjectID"));
			}});
			hazelcastBaseNode.transformToTapValue(tapdataEvent, tapTableMap, TABLE_NAME, tapValueTransform);
			assertEquals(tapInsertRecordEvent, tapdataEvent.getTapEvent());
			assertEquals(4, ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter().size());
			Object actualOid = ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("_id");
			assertEquals(TapStringValue.class, actualOid.getClass());
			assertEquals(objectId.toHexString(), ((TapStringValue) actualOid).getValue());
			assertEquals(objectId, ((TapStringValue) actualOid).getOriginValue());
			assertEquals("ObjectID", ((TapStringValue) actualOid).getOriginType());
			assertEquals(TapString.class, ((TapStringValue) actualOid).getTapType().getClass());
			assertEquals(1D, ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("id"));
			assertEquals("test", ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("name"));
			Object actualInsertDteObj = ((TapInsertRecordEvent) tapdataEvent.getTapEvent()).getAfter().get("insert_dte");
			assertEquals(TapDateTimeValue.class, actualInsertDteObj.getClass());
			TapDateTimeValue actualInsertDte = (TapDateTimeValue) actualInsertDteObj;
			assertEquals(new DateTime(TIME), actualInsertDte.getValue());
			assertEquals(Instant.ofEpochMilli(TIME), actualInsertDte.getOriginValue());
		}

		@Test
		void testTransformToTapValueFourParamWhenUpdate() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapUpdateRecordEvent);
			HazelcastBaseNode.TapValueTransform tapValueTransform = new HazelcastBaseNode.TapValueTransform();
			tapValueTransform.before(new HashMap<String, TapValue<?, ?>>() {{
				put("_id", new TapStringValue(objectId.toHexString()).originValue(objectId).tapType(new TapString()).originType("ObjectID"));
			}});
			hazelcastBaseNode.transformToTapValue(tapdataEvent, tapTableMap, TABLE_NAME, tapValueTransform);
			assertEquals(tapUpdateRecordEvent, tapdataEvent.getTapEvent());
			assertEquals(4, ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getBefore().size());
			Object actualOid = ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("_id");
			assertEquals(TapStringValue.class, actualOid.getClass());
			assertEquals(objectId.toHexString(), ((TapStringValue) actualOid).getValue());
			assertEquals(objectId, ((TapStringValue) actualOid).getOriginValue());
			assertEquals("ObjectID", ((TapStringValue) actualOid).getOriginType());
			assertEquals(TapString.class, ((TapStringValue) actualOid).getTapType().getClass());
			assertEquals(1D, ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("id"));
			assertEquals("test", ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("name"));
			Object actualInsertDteObj = ((TapUpdateRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("insert_dte");
			assertEquals(TapDateTimeValue.class, actualInsertDteObj.getClass());
			TapDateTimeValue actualInsertDte = (TapDateTimeValue) actualInsertDteObj;
			assertEquals(new DateTime(TIME), actualInsertDte.getValue());
			assertEquals(Instant.ofEpochMilli(TIME), actualInsertDte.getOriginValue());
		}

		@Test
		void testTransformToTapValueFourParamWhenDelete() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapDeleteRecordEvent);
			HazelcastBaseNode.TapValueTransform tapValueTransform = new HazelcastBaseNode.TapValueTransform();
			tapValueTransform.before(new HashMap<String, TapValue<?, ?>>() {{
				put("_id", new TapStringValue(objectId.toHexString()).originValue(objectId).tapType(new TapString()).originType("ObjectID"));
			}});
			hazelcastBaseNode.transformToTapValue(tapdataEvent, tapTableMap, TABLE_NAME, tapValueTransform);
			assertEquals(tapDeleteRecordEvent, tapdataEvent.getTapEvent());
			assertEquals(4, ((TapDeleteRecordEvent) tapdataEvent.getTapEvent()).getBefore().size());
			Object actualOid = ((TapDeleteRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("_id");
			assertEquals(TapStringValue.class, actualOid.getClass());
			assertEquals(objectId.toHexString(), ((TapStringValue) actualOid).getValue());
			assertEquals(objectId, ((TapStringValue) actualOid).getOriginValue());
			assertEquals("ObjectID", ((TapStringValue) actualOid).getOriginType());
			assertEquals(TapString.class, ((TapStringValue) actualOid).getTapType().getClass());
			assertEquals(1D, ((TapDeleteRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("id"));
			assertEquals("test", ((TapDeleteRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("name"));
			Object actualInsertDteObj = ((TapDeleteRecordEvent) tapdataEvent.getTapEvent()).getBefore().get("insert_dte");
			assertEquals(TapDateTimeValue.class, actualInsertDteObj.getClass());
			TapDateTimeValue actualInsertDte = (TapDateTimeValue) actualInsertDteObj;
			assertEquals(new DateTime(TIME), actualInsertDte.getValue());
			assertEquals(Instant.ofEpochMilli(TIME), actualInsertDte.getOriginValue());
		}
	}

	@Nested
	class TapEvent2MessageAndMessage2TapEventTest {
		final Long TIME = 1700015312781L;
		final String TABLE_NAME = "test_table";
		final ObjectId objectId = new ObjectId("65546c165006f2da7ba64c73");
		final Map<String, Object> MOCK_DATA = new HashMap<String, Object>() {{
			put("_id", objectId.toHexString());
			put("id", 1D);
			put("name", "test");
			put("insert_dte", Instant.ofEpochMilli(TIME));
		}};
		final Map<String, Object> INFO_MAP = new HashMap<String, Object>() {{
			put("test", "test");
		}};
		TapInsertRecordEvent tapInsertRecordEvent;

		@BeforeEach
		void beforeEach() {
			tapInsertRecordEvent = mock(TapInsertRecordEvent.class);
			when(tapInsertRecordEvent.getAfter()).thenReturn(MOCK_DATA);
			when(tapInsertRecordEvent.getTableId()).thenReturn(TABLE_NAME);
			when(tapInsertRecordEvent.getReferenceTime()).thenReturn(TIME);
			when(tapInsertRecordEvent.getInfo()).thenReturn(INFO_MAP);
			when(tapInsertRecordEvent.getTime()).thenReturn(TIME);
		}

		@Test
		void testTapEvent2MessageNullTapRecordEvent() {
			MessageEntity actual = hazelcastBaseNode.tapEvent2Message(null);
			assertNull(actual);
		}

		@Test
		void testTapEvent2Message() {
			MessageEntity actual = hazelcastBaseNode.tapEvent2Message(tapInsertRecordEvent);
			assertNotNull(actual);
			assertEquals(MessageEntity.class, actual.getClass());
			assertEquals(MOCK_DATA, actual.getAfter());
			assertEquals(TABLE_NAME, actual.getTableName());
			assertEquals(TIME, actual.getTimestamp());
			assertEquals("i", actual.getOp());
			assertEquals(INFO_MAP, actual.getInfo());
			assertEquals(TIME, actual.getTime());
		}

		@Test
		void testMessage2TapEventNullMessageEntity() {
			TapRecordEvent actual = hazelcastBaseNode.message2TapEvent(null);
			assertNull(actual);
		}

		@Test
		void testMessage2TapEventWhenInsert() {
			MessageEntity messageEntity = new MessageEntity("i", MOCK_DATA, TABLE_NAME);
			messageEntity.setInfo(INFO_MAP);
			messageEntity.setTimestamp(TIME);
			messageEntity.setTime(TIME);
			TapRecordEvent actual = hazelcastBaseNode.message2TapEvent(messageEntity);
			assertNotNull(actual);
			assertEquals(TapInsertRecordEvent.class, actual.getClass());
			TapInsertRecordEvent tapRecordEvent = (TapInsertRecordEvent) actual;
			assertEquals(MOCK_DATA, tapRecordEvent.getAfter());
			assertEquals(TABLE_NAME, tapRecordEvent.getTableId());
			assertEquals(TIME, tapRecordEvent.getReferenceTime());
			assertEquals(TIME, tapRecordEvent.getTime());
			assertEquals(INFO_MAP, tapRecordEvent.getInfo());
		}

		@Test
		void testMessage2TapEventWhenUpdate() {
			MessageEntity messageEntity = new MessageEntity("u", MOCK_DATA, TABLE_NAME);
			messageEntity.setInfo(INFO_MAP);
			messageEntity.setTimestamp(TIME);
			messageEntity.setBefore(MOCK_DATA);
			messageEntity.setTime(TIME);
			TapRecordEvent actual = hazelcastBaseNode.message2TapEvent(messageEntity);
			assertNotNull(actual);
			assertEquals(TapUpdateRecordEvent.class, actual.getClass());
			TapUpdateRecordEvent tapRecordEvent = (TapUpdateRecordEvent) actual;
			assertEquals(MOCK_DATA, tapRecordEvent.getAfter());
			assertEquals(MOCK_DATA, tapRecordEvent.getBefore());
			assertEquals(TABLE_NAME, tapRecordEvent.getTableId());
			assertEquals(TIME, tapRecordEvent.getReferenceTime());
			assertEquals(TIME, tapRecordEvent.getTime());
			assertEquals(INFO_MAP, tapRecordEvent.getInfo());
		}

		@Test
		void testMessage2TapEventWhenDelete() {
			MessageEntity messageEntity = new MessageEntity("d", null, TABLE_NAME);
			messageEntity.setInfo(INFO_MAP);
			messageEntity.setTimestamp(TIME);
			messageEntity.setBefore(MOCK_DATA);
			messageEntity.setTime(TIME);
			TapRecordEvent actual = hazelcastBaseNode.message2TapEvent(messageEntity);
			assertNotNull(actual);
			assertEquals(TapDeleteRecordEvent.class, actual.getClass());
			TapDeleteRecordEvent tapRecordEvent = (TapDeleteRecordEvent) actual;
			assertEquals(MOCK_DATA, tapRecordEvent.getBefore());
			assertEquals(TABLE_NAME, tapRecordEvent.getTableId());
			assertEquals(TIME, tapRecordEvent.getReferenceTime());
			assertEquals(TIME, tapRecordEvent.getTime());
			assertEquals(INFO_MAP, tapRecordEvent.getInfo());
		}

		@Test
		void testMessage2TapEventWhenInvalidOp() {
			MessageEntity messageEntity = new MessageEntity("invalid_op", MOCK_DATA, TABLE_NAME);
			assertThrows(IllegalArgumentException.class, () -> hazelcastBaseNode.message2TapEvent(messageEntity));
		}

		@Test
		void testMessage2TapEventWhenNotDmlOp() {
			MessageEntity messageEntity = new MessageEntity(OperationType.DDL.getOp(), MOCK_DATA, TABLE_NAME);
			TapRecordEvent tapRecordEvent = hazelcastBaseNode.message2TapEvent(messageEntity);
			assertNull(tapRecordEvent);
		}
	}

	@Nested
	class GetTableNameTest {
		final String TABLE_NAME1 = "test_table1";
		final String TABLE_NAME2 = "test_table2";
		TapInsertRecordEvent tapInsertRecordEvent;
		HeartbeatEvent heartbeatEvent;
		MessageEntity messageEntity;
		TapdataEvent tapdataEvent;

		@BeforeEach
		void beforeEach() {
			messageEntity = mock(MessageEntity.class);
			when(messageEntity.getTableName()).thenReturn(TABLE_NAME1);
			tapInsertRecordEvent = mock(TapInsertRecordEvent.class);
			when(tapInsertRecordEvent.getTableId()).thenReturn(TABLE_NAME2);
			heartbeatEvent = mock(HeartbeatEvent.class);
			tapdataEvent = mock(TapdataEvent.class);
		}

		@Test
		void testGetTableNameOnlyHaveMessageEntity() {
			when(tapdataEvent.getMessageEntity()).thenReturn(messageEntity);
			when(tapdataEvent.getTapEvent()).thenReturn(null);
			String tableName = hazelcastBaseNode.getTableName(tapdataEvent);
			verify(tapdataEvent, new Times(1)).getMessageEntity();
			assertNotNull(tableName);
			assertEquals(TABLE_NAME1, tableName);
		}

		@Test
		void testGetTableNameOnlyHaveTapEvent() {
			when(tapdataEvent.getMessageEntity()).thenReturn(null);
			when(tapdataEvent.getTapEvent()).thenReturn(tapInsertRecordEvent);
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			String tableName = hazelcastBaseNode.getTableName(tapdataEvent);
			verify(tapdataEvent, new Times(1)).getTapEvent();
			assertNotNull(tableName);
			assertEquals(TABLE_NAME2, tableName);
		}

		@Test
		void testGetTableNameBothHaveMessageEntityAndTapEvent() {
			when(tapdataEvent.getMessageEntity()).thenReturn(messageEntity);
			when(tapdataEvent.getTapEvent()).thenReturn(tapInsertRecordEvent);
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			String tableName = hazelcastBaseNode.getTableName(tapdataEvent);
			verify(tapdataEvent, new Times(1)).getTapEvent();
			assertNotNull(tableName);
			assertEquals(TABLE_NAME1, tableName);
		}

		@Test
		void testGetTableNameWithNullTapdataEvent() {
			String tableName = hazelcastBaseNode.getTableName(null);
			assertNotNull(tableName);
			assertEquals("", tableName);
		}

		@Test
		void testGetTableNameNotTapBaseEvent() {
			when(tapdataEvent.getMessageEntity()).thenReturn(null);
			when(tapdataEvent.getTapEvent()).thenReturn(heartbeatEvent);
			String tableName = hazelcastBaseNode.getTableName(tapdataEvent);
			assertNotNull(tableName);
			assertEquals("", tableName);
		}
	}

	@Nested
	class OfferTest {
		TapdataEvent tapdataEvent;
		Outbox outbox;

		@BeforeEach
		void beforeEach() {
			CommonUtils.setProperty("test_prop", "false");
			tapdataEvent = new TapdataEvent();
			outbox = mock(Outbox.class);
			when(outbox.bucketCount()).thenReturn(1);
			when(mockHazelcastBaseNode.getOutboxAndCheckNullable()).thenReturn(outbox);
			when(mockHazelcastBaseNode.offer(any())).thenCallRealMethod();
		}

		@Test
		void testOfferWhenTryEmitReturnTrue() {
			when(mockHazelcastBaseNode.tryEmit(tapdataEvent, 1)).thenReturn(true);
			boolean actual = mockHazelcastBaseNode.offer(tapdataEvent);
			assertTrue(actual);
		}

		@Test
		void testOfferWhenTryEmitReturnFalse() {
			when(mockHazelcastBaseNode.tryEmit(tapdataEvent, 1)).thenReturn(false);
			boolean actual = mockHazelcastBaseNode.offer(tapdataEvent);
			assertFalse(actual);
		}

		@Test
		void testOfferWhenTapdataEventIsNull() {
			boolean actual = mockHazelcastBaseNode.offer(null);
			assertTrue(actual);
		}
	}

	@Nested
	class InitMonitorTest {
		@Test
		void testInitMonitor() {
			MonitorManager monitorManager = hazelcastBaseNode.initMonitor();
			assertNotNull(monitorManager);
		}
	}

	@Nested
	class StartMonitorIfNeedTest {
		MonitorManager monitorManager;
		Processor.Context context;
		HazelcastInstance hazelcastInstance;
		JetService jetService;
		Job job;
		private Object[] startMonitorArgs;
		Monitor jetJobStatusMonitor;

		@BeforeEach
		@SneakyThrows
		void beforeEach() {
			job = mock(Job.class);
			jetService = mock(JetService.class);
			when(jetService.getJob(anyLong())).thenReturn(job);
			hazelcastInstance = mock(HazelcastInstance.class);
			when(hazelcastInstance.getJet()).thenReturn(jetService);
			context = mock(Processor.Context.class);
			when(context.hazelcastInstance()).thenReturn(hazelcastInstance);
			when(context.jobId()).thenReturn(1L);
			monitorManager = mock(MonitorManager.class);
			startMonitorArgs = new Object[]{job, tableNode.getId()};
			doAnswer(invocationOnMock -> null).when(monitorManager)
					.startMonitor(MonitorManager.MonitorType.JET_JOB_STATUS_MONITOR, startMonitorArgs);
			jetJobStatusMonitor = mock(JetJobStatusMonitor.class);
			when(monitorManager.getMonitorByType(MonitorManager.MonitorType.JET_JOB_STATUS_MONITOR)).thenReturn(jetJobStatusMonitor);
			ReflectionTestUtils.setField(hazelcastBaseNode, "monitorManager", monitorManager);
		}

		@Test
		@SneakyThrows
		void testStartMonitorIfNeedWhenNotTestTask() {
			hazelcastBaseNode.startMonitorIfNeed(context);
			verify(monitorManager, new Times(1)).startMonitor(MonitorManager.MonitorType.JET_JOB_STATUS_MONITOR, startMonitorArgs);
			assertEquals(jetJobStatusMonitor, ReflectionTestUtils.getField(hazelcastBaseNode, "jetJobStatusMonitor"));
		}

		@Test
		@SneakyThrows
		void testStartMonitorIfNeedWhenTestTask() {
			taskDto.setSyncType(TaskDto.SYNC_TYPE_TEST_RUN);
			hazelcastBaseNode.startMonitorIfNeed(context);
			verify(monitorManager, new Times(0)).startMonitor(MonitorManager.MonitorType.JET_JOB_STATUS_MONITOR, startMonitorArgs);
			assertNull(ReflectionTestUtils.getField(hazelcastBaseNode, "jetJobStatusMonitor"));
		}

		@Test
		@SneakyThrows
		void testStartMonitorIfNeedWhenDeduceSchemaTask() {
			taskDto.setSyncType(TaskDto.SYNC_TYPE_DEDUCE_SCHEMA);
			hazelcastBaseNode.startMonitorIfNeed(context);
			verify(monitorManager, new Times(0)).startMonitor(MonitorManager.MonitorType.JET_JOB_STATUS_MONITOR, startMonitorArgs);
			assertNull(ReflectionTestUtils.getField(hazelcastBaseNode, "jetJobStatusMonitor"));
		}

		@Test
		@SneakyThrows
		void testStartMonitorIfNeedError() {
			NullPointerException nullPointerException = new NullPointerException();
			doThrow(nullPointerException).when(monitorManager)
					.startMonitor(MonitorManager.MonitorType.JET_JOB_STATUS_MONITOR, startMonitorArgs);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> hazelcastBaseNode.startMonitorIfNeed(context));
			assertEquals(TaskProcessorExCode_11.START_JET_JOB_STATUS_MONITOR_FAILED, tapCodeException.getCode());
			assertEquals(nullPointerException, tapCodeException.getCause());
		}
	}

	@Nested
	class ExecuteAspectOnInitTest {
		AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
		private AtomicBoolean observe;

		@BeforeEach
		void beforeEach() {
			observe = new AtomicBoolean(false);
		}

		@Test
		void testExecuteAspectOnInit() {
			aspectManager.registerAspectObserver(DataNodeInitAspect.class, 1, aspect -> observe.compareAndSet(false, true));
			DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
			when(dataProcessorContext.getNode()).thenReturn((Node) tableNode);
			ReflectionTestUtils.setField(hazelcastBaseNode, "processorBaseContext", dataProcessorContext);
			assertDoesNotThrow(hazelcastBaseNode::executeAspectOnInit);
			assertTrue(observe.get());
		}

		@Test
		void testExecuteAspectOnInitProcessorNode() {
			aspectManager.registerAspectObserver(ProcessorNodeInitAspect.class, 1, aspect -> observe.compareAndSet(false, true));
			MockProcessorNode mockProcessorNode = new MockProcessorNode(processorBaseContext);
			assertDoesNotThrow(mockProcessorNode::executeAspectOnInit);
			assertTrue(observe.get());
		}

		@Test
		void testExecuteAspectOnInitMultiAggregatorProcessorNode() {
			aspectManager.registerAspectObserver(ProcessorNodeInitAspect.class, 1, aspect -> observe.compareAndSet(false, true));
			HazelcastMultiAggregatorProcessor hazelcastMultiAggregatorProcessor = mock(HazelcastMultiAggregatorProcessor.class);
			doCallRealMethod().when(hazelcastMultiAggregatorProcessor).executeAspectOnInit();
			ReflectionTestUtils.setField(hazelcastMultiAggregatorProcessor, "processorBaseContext", processorBaseContext);
			assertDoesNotThrow(hazelcastMultiAggregatorProcessor::executeAspectOnInit);
			assertTrue(observe.get());
		}
	}

	@Nested
	class InitExternalStorageTest {
		@Test
		void testInitExternalStorage() {
			ReflectionTestUtils.setField(hazelcastBaseNode, "clientMongoOperator", mockClientMongoOperator);
			ExternalStorageDto mockExternalStorage = new ExternalStorageDto();
			mockExternalStorage.setId(new ObjectId());
			TaskConfig taskConfig = TaskConfig.create();
			Map<String, ExternalStorageDto> externalStorageDtoMap = new HashMap<>();
			externalStorageDtoMap.put(mockExternalStorage.getId().toHexString(), mockExternalStorage);
			taskConfig.externalStorageDtoMap(externalStorageDtoMap);
			when(processorBaseContext.getTaskConfig()).thenReturn(taskConfig);
			List<Node> nodes = mock(ArrayList.class);
			when(processorBaseContext.getNodes()).thenReturn(nodes);
			try (MockedStatic<ExternalStorageUtil> externalStorageUtilMockedStatic = mockStatic(ExternalStorageUtil.class)) {
				externalStorageUtilMockedStatic.when(() -> ExternalStorageUtil.getExternalStorage(
						externalStorageDtoMap, tableNode, mockClientMongoOperator, nodes, null
				)).thenReturn(mockExternalStorage);
				ExternalStorageDto externalStorageDto = hazelcastBaseNode.initExternalStorage();
				assertEquals(mockExternalStorage, externalStorageDto);
			}
		}
	}

	@Nested
	class InitSettingServiceTest {
		@Test
		void testInitSettingService() {
			ReflectionTestUtils.setField(hazelcastBaseNode, "clientMongoOperator", mockClientMongoOperator);
			SettingService actual = hazelcastBaseNode.initSettingService();
			assertNotNull(actual);
			assertEquals(mockClientMongoOperator, ReflectionTestUtils.getField(actual, "clientMongoOperator"));
		}

		@Test
		void testInitSettingServiceClientMongoOperatorIsNull() {
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> hazelcastBaseNode.initSettingService());
			assertEquals("11018", tapCodeException.getCode());
		}
	}

	@Nested
	class InitClientMongoOperatorTest {
		@Test
		void testInitClientMongoOperator() {
			try (MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
				beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(mockClientMongoOperator);
				ClientMongoOperator actual = hazelcastBaseNode.initClientMongoOperator();
				assertEquals(mockClientMongoOperator, actual);
			}
		}
	}

	@Nested
	class InitObsLoggerTest {
		@Test
		void testInitObsLogger() {
			ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
			when(obsLoggerFactory.getObsLogger(processorBaseContext.getTaskDto(),
					processorBaseContext.getNode().getId(),
					processorBaseContext.getNode().getName())).thenReturn(mockObsLogger);
			try (MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class)) {
				obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
				ObsLogger actual = hazelcastBaseNode.initObsLogger();
				assertEquals(mockObsLogger, actual);
			}
		}
	}

	@Nested
	class TryEmitTest {
		TapdataEvent tapdataEvent;
		Outbox mockOutBox;

		@BeforeEach
		void beforeEach() {
			tapdataEvent = new TapdataEvent();
			mockOutBox = mock(Outbox.class);
			ReflectionTestUtils.setField(hazelcastBaseNode, "outbox", mockOutBox);
			ReflectionTestUtils.setField(hazelcastBaseNode, "bucketIndex", 0);
		}

		@Test
		void testTryEmit() {
			when(mockOutBox.offer(any(TapdataEvent.class))).thenReturn(true);
			boolean actual = hazelcastBaseNode.tryEmit(tapdataEvent, 1);
			assertTrue(actual);
		}

		@Test
		void testTryEmitTwoBucketSuccess() {
			when(mockOutBox.offer(anyInt(), any(TapdataEvent.class))).thenReturn(true);
			boolean actual = hazelcastBaseNode.tryEmit(tapdataEvent, 2);
			assertTrue(actual);
		}

		@Test
		void testTryEmitTwoBucketOneFail() {
			TapdataEvent spyTapdataEvent = spy(tapdataEvent);
			when(spyTapdataEvent.clone()).thenReturn(tapdataEvent);
			when(mockOutBox.offer(0, tapdataEvent)).thenReturn(true);
			when(mockOutBox.offer(1, tapdataEvent)).thenReturn(false);
			boolean actual = hazelcastBaseNode.tryEmit(spyTapdataEvent, 2);
			assertFalse(actual);
		}

		@Test
		void testTryEmitEventIsNull() {
			boolean actual = hazelcastBaseNode.tryEmit(null, 1);
			assertTrue(actual);
			verify(mockOutBox, new Times(0)).offer(tapdataEvent);
		}
	}

	@Nested
	class GetOutboxAndCheckNullableTest {
		@Test
		void testGetOutboxAndCheckNullable() {
			Outbox mockOutbox = mock(Outbox.class);
			ReflectionTestUtils.setField(hazelcastBaseNode, "outbox", mockOutbox);
			Outbox actual = hazelcastBaseNode.getOutboxAndCheckNullable();
			assertNotNull(actual);
			assertEquals(mockOutbox, actual);
		}

		@Test
		void testGetOutboxAndCheckNullableWhenOutboxIsNull() {
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> hazelcastBaseNode.getOutboxAndCheckNullable());
			assertEquals(TaskProcessorExCode_11.OUTBOX_IS_NULL_WHEN_OFFER, tapCodeException.getCode());
			assertNotNull(tapCodeException.getMessage());
		}
	}

	@Nested
	class DoCloseTest {

		TapTableMap tapTableMap;
		MonitorManager monitorManager;
		ObsLogger mockObsLogger;

		@BeforeEach
		void beforeEach() {
			mockObsLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(hazelcastBaseNode, "obsLogger", mockObsLogger);
			tapTableMap = mock(TapTableMap.class);
			when(processorBaseContext.getTapTableMap()).thenReturn(tapTableMap);
			monitorManager = mock(MonitorManager.class);
			ReflectionTestUtils.setField(hazelcastBaseNode, "monitorManager", monitorManager);
		}

		@Test
		@SneakyThrows
		void testDoClose() {
			hazelcastBaseNode.doClose();
			verify(tapTableMap, new Times(1)).reset();
			verify(monitorManager, new Times(1)).close();
			verify(mockObsLogger, new Times(2)).info(anyString());
		}

		@Test
		void testDoCloseTapTableMapIsNull() {
			when(processorBaseContext.getTapTableMap()).thenReturn(null);
			assertDoesNotThrow(() -> hazelcastBaseNode.doClose());
			verify(mockObsLogger, new Times(2)).info(anyString());
		}

		@Test
		void testDoCloseResetTapTableMapError() {
			doThrow(new RuntimeException("test")).when(tapTableMap).reset();
			assertDoesNotThrow(() -> hazelcastBaseNode.doClose());
			verify(mockObsLogger, new Times(1)).warn(anyString());
		}

		@Test
		void testDoCloseResetMonitorManagerIsNull() {
			ReflectionTestUtils.setField(hazelcastBaseNode, "monitorManager", null);
			assertDoesNotThrow(() -> hazelcastBaseNode.doClose());
			verify(mockObsLogger, new Times(2)).info(anyString());
		}

		@Test
		@SneakyThrows
		void testDoCloseResetCloseMonitorManagerError() {
			doThrow(new RuntimeException("test")).when(monitorManager).close();
			assertDoesNotThrow(() -> hazelcastBaseNode.doClose());
			verify(mockObsLogger, new Times(1)).warn(anyString());
		}
	}
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.ex.TestException;
import base.hazelcast.BaseHazelcastNodeTest;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.Processor;
import com.tapdata.entity.Connections;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.cdcdelay.CdcDelay;
import com.tapdata.tm.commons.cdcdelay.ICdcDelay;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.DDLConfiguration;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.TableCountFuncAspect;
import io.tapdata.common.concurrent.SimpleConcurrentProcessorImpl;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.NodeException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.ddl.DDLFilter;
import io.tapdata.flow.engine.V2.ddl.DDLSchemaHandler;
import io.tapdata.flow.engine.V2.sharecdc.ShareCDCOffset;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.async.AsyncUtils;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.TapTableMap;
import io.tapdata.supervisor.TaskResourceSupervisorManager;
import lombok.SneakyThrows;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/11/24 12:18 Create
 */
@DisplayName("HazelcastSourcePdkBaseNode Class Test")
class HazelcastSourcePdkBaseNodeTest extends BaseHazelcastNodeTest {
	private HazelcastSourcePdkBaseNode instance;
	private MockHazelcastSourcePdkBaseNode mockInstance;
	SyncProgress syncProgress;

	@BeforeEach
	void beforeEach() {
		super.allSetup();
		mockInstance = mock(MockHazelcastSourcePdkBaseNode.class);
		ReflectionTestUtils.setField(mockInstance, "processorBaseContext", processorBaseContext);
		ReflectionTestUtils.setField(mockInstance, "dataProcessorContext", dataProcessorContext);
		when(mockInstance.getDataProcessorContext()).thenReturn(dataProcessorContext);
		ReflectionTestUtils.setField(mockInstance, "obsLogger", mockObsLogger);
		instance = new HazelcastSourcePdkBaseNode(dataProcessorContext) {
			@Override
			void startSourceRunner() {

			}
		};
		syncProgress = mock(SyncProgress.class);
		ReflectionTestUtils.setField(mockInstance, "syncProgress", syncProgress);
	}

	@Nested
	@DisplayName("doInit methods test")
	class DoInitTest {
		@BeforeEach
		void beforeEach() {
			TaskResourceSupervisorManager taskResourceSupervisorManager = new TaskResourceSupervisorManager();
			ReflectionTestUtils.setField(mockInstance, "taskResourceSupervisorManager", taskResourceSupervisorManager);
			doCallRealMethod().when(mockInstance).doInit(jetContext);
		}

		@Test
		void testCdcDelaySwitch() {
			Connections mockConnections = mock(Connections.class);
			when(dataProcessorContext.getConnections()).thenReturn(mockConnections);
			when(mockConnections.getHeartbeatEnable()).thenReturn(false);
			when(mockInstance.getNode()).thenReturn((Node) tableNode);

			// mock heartbeat table
			TapTableMap<String, TapTable> tapTableMap = mock(TapTableMap.class);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			TapTable mockTapTable = new TapTable(ConnHeartbeatUtils.TABLE_NAME);
			mockTapTable.add(new TapField("id", "string"));
			when(tapTableMap.get(ConnHeartbeatUtils.TABLE_NAME)).thenReturn(mockTapTable);


			try (MockedStatic<AsyncUtils> asyncUtilsMockedStatic = mockStatic(AsyncUtils.class)) {
				asyncUtilsMockedStatic.when(() -> AsyncUtils.createThreadPoolExecutor(anyString(), anyInt(), any(), anyString())
				).thenReturn(mock(ThreadPoolExecutorEx.class));
				assertDoesNotThrow(() -> mockInstance.doInit(jetContext));

				when(mockConnections.getHeartbeatEnable()).thenReturn(true);

				assertDoesNotThrow(() -> mockInstance.doInit(jetContext));
			}

		}

	}

	@Nested
	@DisplayName("doBatchCountFunction methods test")
	class DoBatchCountFunctionMethodTest {

		long expectedValue = 99;
		TapTable testTable = new TapTable("testTable");
		BatchCountFunction mockBatchCountFunction = mock(BatchCountFunction.class);

		@BeforeEach
		void beforeEach() {
		}

		@Test
		@SneakyThrows
		@DisplayName("Exception test")
		void testException() {
			taskDto.setId(new ObjectId());
			when(mockBatchCountFunction.count(null, testTable)).thenThrow(new TestException());

			TaskConfig taskConfig = TaskConfig.create();
			taskConfig.taskRetryConfig(TaskRetryConfig.create());
			taskConfig.getTaskRetryConfig().retryIntervalSecond(1000L);
			when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);

			HazelcastSourcePdkBaseNode spyInstance = Mockito.spy(instance);
			doReturn(new ConnectorNode()).when(spyInstance).getConnectorNode();

			try {
				spyInstance.doBatchCountFunction(mockBatchCountFunction, testTable);
			} catch (Exception e) {
				assertTrue(null != e.getCause() && null != e.getCause().getCause() && (e.getCause().getCause().getCause() instanceof TestException), e.getMessage());
			}
		}

		@Test
		@SneakyThrows
		@DisplayName("No acceptor register test")
		void testNoAcceptorRegisterTest() {
			when(mockBatchCountFunction.count(null, testTable)).thenReturn(expectedValue);

			TaskConfig taskConfig = TaskConfig.create();
			taskConfig.taskRetryConfig(TaskRetryConfig.create());
			taskConfig.getTaskRetryConfig().retryIntervalSecond(1000L);
			when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);

			HazelcastSourcePdkBaseNode spyInstance = Mockito.spy(instance);
			doReturn(new ConnectorNode()).when(spyInstance).getConnectorNode();

			Long counts = spyInstance.doBatchCountFunction(mockBatchCountFunction, testTable);
			assertEquals(expectedValue, counts);
		}

		@Test
		@SneakyThrows
		@DisplayName("Expected result test")
		void testUExpectedResult() {
			AtomicBoolean observed = new AtomicBoolean(false);
			AspectObserver<TableCountFuncAspect> aspectObserver = aspect -> observed.compareAndSet(false, true);
			AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
			aspectManager.registerAspectObserver(TableCountFuncAspect.class, 1, aspectObserver);
			when(mockBatchCountFunction.count(null, testTable)).thenReturn(expectedValue);

			TaskConfig taskConfig = TaskConfig.create();
			taskConfig.taskRetryConfig(TaskRetryConfig.create());
			taskConfig.getTaskRetryConfig().retryIntervalSecond(1000L);
			when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);

			HazelcastSourcePdkBaseNode spyInstance = Mockito.spy(instance);
			doReturn(new ConnectorNode()).when(spyInstance).getConnectorNode();

			Long counts = spyInstance.doBatchCountFunction(mockBatchCountFunction, testTable);
			assertEquals(expectedValue, counts);
		}

	}

	@Nested
	@DisplayName("doAsyncTableCount methods test")
	class DoAsyncTableCountMethodTest {
		AtomicReference<BatchCountFunction> mockBatchCountFunction = new AtomicReference<>();

		@BeforeEach
		void beforeEach() {
			mockBatchCountFunction.set(mock(BatchCountFunction.class));
		}

		@Test
		@DisplayName("un-support batchCountFunction test")
		void testUnSupportBatchCountFunction() {
			String testTableName = "testTable";

			when(mockInstance.doAsyncTableCount(null, testTableName)).thenCallRealMethod();

			assertDoesNotThrow(() -> {
				try (AutoCloseable autoCloseable = mockInstance.doAsyncTableCount(null, testTableName)) {
				}
			});
		}

		@Test
		@DisplayName("test exit if not running")
		void testExitIfNotRunning() {
			String testTableName = "testTable";

			when(mockInstance.doAsyncTableCount(mockBatchCountFunction.get(), testTableName)).thenCallRealMethod();
			when(mockInstance.isRunning()).thenReturn(false);

			assertDoesNotThrow(() -> {
				try (AutoCloseable autoCloseable = mockInstance.doAsyncTableCount(mockBatchCountFunction.get(), testTableName)) {
				}
			});
		}

		@Test
		@DisplayName("throw exception test")
		void testThrowException() {
			String testTableName = "testTable";

			when(mockInstance.doAsyncTableCount(mockBatchCountFunction.get(), testTableName)).thenCallRealMethod();
			when(mockInstance.isRunning()).thenReturn(true);
			when(mockInstance.getDataProcessorContext()).thenThrow(new TestException());

			// test thread exception if running
			assertDoesNotThrow(() -> {
				try (AutoCloseable autoCloseable = mockInstance.doAsyncTableCount(mockBatchCountFunction.get(), testTableName)) {
				}
			});

			// test thread exception if not running
			when(mockInstance.isRunning()).thenReturn(false);
			assertDoesNotThrow(() -> {
				// the isRunning method Return true on the first call and false on the second call
				// Ensure that the exception 'isRunning' returns false, please!
				when(mockInstance.isRunning()).thenReturn(true, false);
				try (AutoCloseable autoCloseable = mockInstance.doAsyncTableCount(mockBatchCountFunction.get(), testTableName)) {
				}
			});
		}

		@Test
		@DisplayName("set snapshotRowSizeMap test")
		void testSetSnapshotRowSizeMap() {

			long tableSize = 1000L; // Expected result

			// mock definition
			TapTable testTable = new TapTable("testTable");
			TapTable testSnapshotRowSizeMapNotNullTable = new TapTable("testTable2");

			TaskDto mockTask = mock(TaskDto.class);
			when(mockTask.getId()).thenReturn(new ObjectId());
			when(dataProcessorContext.getTaskDto()).thenReturn(mockTask);

			Node mockNode = mock(Node.class);
			when(dataProcessorContext.getNode()).thenReturn(mockNode);

			TapTableMap mockTapTableMap = mock(TapTableMap.class);
			when(mockTapTableMap.get(testTable.getName())).thenReturn(testTable);
			when(mockTapTableMap.get(testSnapshotRowSizeMapNotNullTable.getName())).thenReturn(testSnapshotRowSizeMapNotNullTable);
			when(dataProcessorContext.getTapTableMap()).thenReturn(mockTapTableMap);

			when(mockInstance.doAsyncTableCount(mockBatchCountFunction.get(), testTable.getName())).thenCallRealMethod();
			when(mockInstance.doBatchCountFunction(mockBatchCountFunction.get(), testTable)).thenReturn(tableSize);
			when(mockInstance.isRunning()).thenReturn(true);

			// test SnapshotRowSizeMap is null
			assertDoesNotThrow(() -> {
				try (AutoCloseable autoCloseable = mockInstance.doAsyncTableCount(mockBatchCountFunction.get(), testTable.getName())) {
				}
			});
			Long countResult = mockInstance.snapshotRowSizeMap.get(testTable.getName());
			assertEquals(tableSize, (long) countResult);

			// test SnapshotRowSizeMap not null
			assertDoesNotThrow(() -> {
				try (AutoCloseable autoCloseable = mockInstance.doAsyncTableCount(mockBatchCountFunction.get(), testTable.getName())) {
				}
			});
			countResult = mockInstance.snapshotRowSizeMap.get(testTable.getName());
			assertEquals(tableSize, (long) countResult);
		}
	}

	@Nested
	@DisplayName("handleSchemaChange methods test")
	class HandleSchemaChangeTest {
		@Test
		@DisplayName("cover setTableAttr test")
		void testHandleSchemaChangeCoverSetTableAttr() {
			TapEvent tapEvent = mock(TapDDLEvent.class);
			when(((TapDDLEvent) tapEvent).getTableId()).thenReturn("111");
			doCallRealMethod().when(mockInstance).handleSchemaChange(tapEvent);
			when(processorBaseContext.getTapTableMap()).thenReturn(mock(TapTableMap.class));
			DDLSchemaHandler handler = mock(DDLSchemaHandler.class);
			when(mockInstance.ddlSchemaHandler()).thenReturn(handler);
			doNothing().when(handler).updateSchemaByDDLEvent(any(), any());
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(mockInstance.getConnectorNode()).thenReturn(connectorNode);
			TapConnectorContext context = mock(TapConnectorContext.class);
			when(connectorNode.getConnectorContext()).thenReturn(context);
			TapNodeSpecification specification = mock(TapNodeSpecification.class);
			when(context.getSpecification()).thenReturn(specification);
			when(specification.getDataTypesMap()).thenReturn(mock(DefaultExpressionMatchingMap.class));
			when(processorBaseContext.getTapTableMap().get(((TapDDLEvent) tapEvent).getTableId())).thenReturn(mock(TapTable.class));
			DAG dag = mock(DAG.class);
			TaskDto dto = mock(TaskDto.class);
			when(processorBaseContext.getTaskDto()).thenReturn(dto);
			when(dto.getDag()).thenReturn(dag);
			TransformerWsMessageDto transformerWsMessageDto = mock(TransformerWsMessageDto.class);
			ReflectionTestUtils.setField(mockInstance, "transformerWsMessageDto", transformerWsMessageDto);
			when(transformerWsMessageDto.getMetadataInstancesDtoList()).thenReturn(mock(ArrayList.class));
			DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
			when(mockInstance.initDagDataService(transformerWsMessageDto)).thenReturn(dagDataService);
			TapTableMap tableMap = mock(TapTableMap.class);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tableMap);
			when(tableMap.getQualifiedName(anyString())).thenReturn("qualifiedName");
			when(dag.transformSchema(any(), any(), any())).thenReturn(mock(HashMap.class));
			MetadataInstancesDto metadata = mock(MetadataInstancesDto.class);
			when(dagDataService.getMetadata(anyString())).thenReturn(metadata);
			ObjectId objectId = mock(ObjectId.class);
			when(metadata.getId()).thenReturn(objectId);
			HashMap attr = mock(HashMap.class);
			when(metadata.getTableAttr()).thenReturn(attr);
			when(objectId.toHexString()).thenReturn("objectId");
			mockInstance.handleSchemaChange(tapEvent);
			assertEquals(attr, metadata.getTableAttr());
		}
	}

	@Nested
	class DdlSchemaHandlerTest {
		@Test
		void testDdlSchemaHandler() {
			try (MockedStatic<InstanceFactory> factory = Mockito
					.mockStatic(InstanceFactory.class)) {
				DDLSchemaHandler handler = mock(DDLSchemaHandler.class);
				factory.when(() -> InstanceFactory.bean(DDLSchemaHandler.class)).thenReturn(handler);
				doCallRealMethod().when(mockInstance).ddlSchemaHandler();
				assertEquals(handler, mockInstance.ddlSchemaHandler());
			}
		}
	}

	@Nested
	@DisplayName("Method initBatchAndStreamOffset test")
	class InitBatchAndStreamOffsetTest {

		private SyncProgress syncProgress;
		private HazelcastSourcePdkDataNode hazelcastSourcePdkDataNode;

		@BeforeEach
		void beforeEach() {
			hazelcastSourcePdkDataNode = spy(new HazelcastSourcePdkDataNode(dataProcessorContext));
			doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).initBatchAndStreamOffsetFirstTime(any(TaskDto.class));
			doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).readBatchAndStreamOffset(any(TaskDto.class));
		}

		@Test
		@DisplayName("test syncProgress is null")
		void testSyncProgressIsNull() {
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode, "syncProgress", syncProgress);
			hazelcastSourcePdkDataNode.initBatchAndStreamOffset(dataProcessorContext.getTaskDto());
			verify(hazelcastSourcePdkDataNode, times(1)).initBatchAndStreamOffsetFirstTime(any(TaskDto.class));
			verify(hazelcastSourcePdkDataNode, never()).readBatchAndStreamOffset(any(TaskDto.class));
		}

		@Test
		@DisplayName("test syncProgress is not null")
		void testSyncProgressIsNotNull() {
			this.syncProgress = new SyncProgress();
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode, "syncProgress", syncProgress);
			hazelcastSourcePdkDataNode.initBatchAndStreamOffset(dataProcessorContext.getTaskDto());
			verify(hazelcastSourcePdkDataNode, never()).initBatchAndStreamOffsetFirstTime(any(TaskDto.class));
			verify(hazelcastSourcePdkDataNode, times(1)).readBatchAndStreamOffset(any(TaskDto.class));
		}
	}

	@Test
	@DisplayName("Method readBatchAndStreamOffset test")
	void testReadBatchAndStreamOffset() {
		HazelcastSourcePdkDataNode hazelcastSourcePdkDataNode = spy(new HazelcastSourcePdkDataNode(dataProcessorContext));
		doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).readBatchOffset();
		doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).readStreamOffset(any(TaskDto.class));
		hazelcastSourcePdkDataNode.readBatchAndStreamOffset(dataProcessorContext.getTaskDto());
		verify(hazelcastSourcePdkDataNode, times(1)).readBatchOffset();
		verify(hazelcastSourcePdkDataNode, times(1)).readStreamOffset(any(TaskDto.class));
	}

	@Nested
	@DisplayName("Method readStreamOffset test")
	class readStreamOffsetTest {

		private HazelcastSourcePdkDataNode hazelcastSourcePdkDataNode;
		private SyncProgress syncProgress = new SyncProgress();

		@BeforeEach
		void setUp() {
			hazelcastSourcePdkDataNode = spy(new HazelcastSourcePdkDataNode(dataProcessorContext));
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode, "syncProgress", syncProgress);
			syncProgress.setSourceTime(System.currentTimeMillis());
		}

		@Test
		@DisplayName("test normal and log collector type")
		void testNormalAndLogCollectorType() {
			syncProgress.setType(SyncProgress.Type.NORMAL);
			syncProgress.setStreamOffset("test");
			syncProgress.setEventTime(null);
			doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).readNormalAndLogCollectorTaskStreamOffset(any(String.class));
			hazelcastSourcePdkDataNode.readStreamOffset(dataProcessorContext.getTaskDto());
			verify(hazelcastSourcePdkDataNode, times(1)).readNormalAndLogCollectorTaskStreamOffset(any(String.class));
			assertEquals(syncProgress.getSourceTime(), syncProgress.getEventTime());
		}

		@Test
		@DisplayName("test share cdc type")
		void testShareCDCType() {
			syncProgress.setType(SyncProgress.Type.SHARE_CDC);
			syncProgress.setStreamOffset("test");
			doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).readShareCDCStreamOffset(any(TaskDto.class), any(String.class));
			hazelcastSourcePdkDataNode.readStreamOffset(dataProcessorContext.getTaskDto());
			verify(hazelcastSourcePdkDataNode, times(1)).readShareCDCStreamOffset(any(TaskDto.class), any(String.class));
		}

		@Test
		@DisplayName("test polling cdc type")
		void testPollingCDCType() {
			syncProgress.setType(SyncProgress.Type.POLLING_CDC);
			syncProgress.setStreamOffset("test");
			doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).readPollingCDCStreamOffset(any(String.class));
			hazelcastSourcePdkDataNode.readStreamOffset(dataProcessorContext.getTaskDto());
			verify(hazelcastSourcePdkDataNode, times(1)).readPollingCDCStreamOffset(any(String.class));
		}

		@Test
		@DisplayName("test nonsupport type")
		void testNonsupportType() {
			syncProgress.setType(SyncProgress.Type.UNIT_TEST);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> hazelcastSourcePdkDataNode.readStreamOffset(dataProcessorContext.getTaskDto()));
			assertNotNull(tapCodeException);
			assertEquals(TaskProcessorExCode_11.READ_STREAM_OFFSET_UNKNOWN_TASK_TYPE, tapCodeException.getCode());
		}
	}

	@Nested
	@DisplayName("Method readShareCDCStreamOffsetSwitchNormalTask test")
	class readShareCDCStreamOffsetSwitchNormalTaskTest {

		private SyncProgress syncProgress;

		@BeforeEach
		void setUp() {
			instance = spy(instance);
			syncProgress = new SyncProgress();
			ReflectionTestUtils.setField(instance, "syncProgress", syncProgress);
		}

		@Test
		@DisplayName("test read normal task stream offset")
		void testReadNormalTaskStreamOffset() {
			Map<String, Object> streamOffsetMap = new HashMap<>();
			streamOffsetMap.put("test", 1);
			syncProgress.setStreamOffset(PdkUtil.encodeOffset(streamOffsetMap));
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			doReturn(connectorNode).when(instance).getConnectorNode();
			instance.readShareCDCStreamOffsetSwitchNormalTask(syncProgress.getStreamOffset());
			assertNotNull(syncProgress.getStreamOffsetObj());
			assertInstanceOf(Map.class, syncProgress.getStreamOffsetObj());
			assertEquals(1, ((Map) syncProgress.getStreamOffsetObj()).size());
			assertEquals(1, ((Map) syncProgress.getStreamOffsetObj()).get("test"));
		}

		@Test
		@DisplayName("test read share cdc task switch to normal task stream offset")
		void testReadShareCDCTaskSwitchToNormalTaskStreamOffset() {
			Map<String, Object> streamOffsetMap = new HashMap<>();
			streamOffsetMap.put("test", 1);
			ShareCDCOffset shareCDCOffset = new ShareCDCOffset(null, streamOffsetMap);
			syncProgress.setStreamOffset(PdkUtil.encodeOffset(shareCDCOffset));
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			doReturn(connectorNode).when(instance).getConnectorNode();
			instance.readShareCDCStreamOffsetSwitchNormalTask(syncProgress.getStreamOffset());
			assertNotNull(syncProgress.getStreamOffsetObj());
			assertInstanceOf(Map.class, syncProgress.getStreamOffsetObj());
			assertEquals(1, ((Map) syncProgress.getStreamOffsetObj()).size());
			assertEquals(1, ((Map) syncProgress.getStreamOffsetObj()).get("test"));
		}

		@Test
		@DisplayName("test stream offset is null and sourceTime, eventTime is null")
		void testStreamOffsetIsNullAndSourceTimeEventTimeIsNull() {
			syncProgress.setSourceTime(null);
			syncProgress.setEventTime(null);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> instance.readShareCDCStreamOffsetSwitchNormalTask(null));
			assertNotNull(tapCodeException);
			assertEquals(TaskProcessorExCode_11.SHARE_CDC_SWITCH_TO_NORMAL_TASK_FAILED, tapCodeException.getCode());
		}

		@Test
		@SneakyThrows
		@DisplayName("test stream offset is null and source time is not null")
		void testStreamOffsetIsNullAndSourceTimeIsNotNull() {
			syncProgress.setSourceTime(1L);
			syncProgress.setEventTime(null);
			Object fakeStreamOffset = new Object();
			TapConnectorContext tapConnectorContext = mock(TapConnectorContext.class);
			TimestampToStreamOffsetFunction timestampToStreamOffsetFunction = mock(TimestampToStreamOffsetFunction.class);
			when(timestampToStreamOffsetFunction.timestampToStreamOffset(eq(tapConnectorContext), any(Long.class))).thenReturn(fakeStreamOffset);
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			when(connectorFunctions.getTimestampToStreamOffsetFunction()).thenReturn(timestampToStreamOffsetFunction);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			when(connectorNode.getConnectorContext()).thenReturn(tapConnectorContext);
			doReturn(connectorNode).when(instance).getConnectorNode();
			doCallRealMethod().when(connectorNode).applyClassLoaderContext(any(Runnable.class));

			instance.readShareCDCStreamOffsetSwitchNormalTask(null);

			verify(instance, times(1)).initStreamOffsetFromTime(syncProgress.getSourceTime());
			verify(instance, never()).initStreamOffsetFromTime(syncProgress.getEventTime());
			assertEquals(fakeStreamOffset, syncProgress.getStreamOffsetObj());
		}

		@Test
		@SneakyThrows
		@DisplayName("test stream offset is null and event time is not null")
		void testStreamOffsetIsNullAndEventTimeIsNotNull() {
			syncProgress.setEventTime(1L);
			syncProgress.setSourceTime(null);
			Object fakeStreamOffset = new Object();
			TapConnectorContext tapConnectorContext = mock(TapConnectorContext.class);
			TimestampToStreamOffsetFunction timestampToStreamOffsetFunction = mock(TimestampToStreamOffsetFunction.class);
			when(timestampToStreamOffsetFunction.timestampToStreamOffset(eq(tapConnectorContext), any(Long.class))).thenReturn(fakeStreamOffset);
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			when(connectorFunctions.getTimestampToStreamOffsetFunction()).thenReturn(timestampToStreamOffsetFunction);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			when(connectorNode.getConnectorContext()).thenReturn(tapConnectorContext);
			doReturn(connectorNode).when(instance).getConnectorNode();
			doCallRealMethod().when(connectorNode).applyClassLoaderContext(any(Runnable.class));

			instance.readShareCDCStreamOffsetSwitchNormalTask(null);

			verify(instance, times(1)).initStreamOffsetFromTime(syncProgress.getEventTime());
			verify(instance, never()).initStreamOffsetFromTime(syncProgress.getSourceTime());
			assertEquals(fakeStreamOffset, syncProgress.getStreamOffsetObj());
		}

		@Test
		@SneakyThrows
		@DisplayName("test stream offset is null and source time and event time all not null")
		void testStreamOffsetIsNullAndSourceTimeEventTimeAllNotNull() {
			syncProgress.setSourceTime(1L);
			syncProgress.setEventTime(2L);
			Object fakeStreamOffset = new Object();
			TapConnectorContext tapConnectorContext = mock(TapConnectorContext.class);
			TimestampToStreamOffsetFunction timestampToStreamOffsetFunction = mock(TimestampToStreamOffsetFunction.class);
			when(timestampToStreamOffsetFunction.timestampToStreamOffset(eq(tapConnectorContext), any(Long.class))).thenReturn(fakeStreamOffset);
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			when(connectorFunctions.getTimestampToStreamOffsetFunction()).thenReturn(timestampToStreamOffsetFunction);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			when(connectorNode.getConnectorContext()).thenReturn(tapConnectorContext);
			doReturn(connectorNode).when(instance).getConnectorNode();
			doCallRealMethod().when(connectorNode).applyClassLoaderContext(any(Runnable.class));

			instance.readShareCDCStreamOffsetSwitchNormalTask(null);

			verify(instance, times(1)).initStreamOffsetFromTime(syncProgress.getEventTime());
			verify(instance, never()).initStreamOffsetFromTime(syncProgress.getSourceTime());
			assertEquals(fakeStreamOffset, syncProgress.getStreamOffsetObj());
		}
	}

	@Nested
	@DisplayName("Method readShareCDCStreamOffsetContinueShareCDC test")
	class readShareCDCStreamOffsetContinueShareCDCTest {

		private SyncProgress syncProgress;

		@BeforeEach
		void setUp() {
			instance = spy(instance);
			syncProgress = new SyncProgress();
			ReflectionTestUtils.setField(instance, "syncProgress", syncProgress);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			doReturn(connectorNode).when(instance).getConnectorNode();
		}

		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			Map<String, Long> fakeSequenceMap = new HashMap<>();
			fakeSequenceMap.put("test", 1L);
			ShareCDCOffset shareCDCOffset = new ShareCDCOffset(fakeSequenceMap, null);
			instance.readShareCDCStreamOffsetContinueShareCDC(PdkUtil.encodeOffset(shareCDCOffset));

			assertNotNull(syncProgress.getStreamOffsetObj());
			assertEquals(1, ((Map) syncProgress.getStreamOffsetObj()).size());
			assertEquals(1L, ((Map) syncProgress.getStreamOffsetObj()).get("test"));
		}

		@Test
		@DisplayName("test offset is not ShareCDCOffset")
		void testOffsetIsNotShareCDCOffset() {
			Map<String, Object> fakeStreamOffset = new HashMap<>();
			fakeStreamOffset.put("test", 1);
			instance.readShareCDCStreamOffsetContinueShareCDC(PdkUtil.encodeOffset(fakeStreamOffset));

			assertNotNull(syncProgress.getStreamOffsetObj());
			assertEquals(1, ((Map) syncProgress.getStreamOffsetObj()).size());
			assertEquals(1, ((Map) syncProgress.getStreamOffsetObj()).get("test"));
		}

		@Test
		@DisplayName("test stream offset string is null")
		void testStreamOffsetIsNull() {
			doAnswer(invocationOnMock -> null).when(instance).initStreamOffsetFromTime(null);
			instance.readShareCDCStreamOffsetContinueShareCDC(null);

			verify(instance, times(1)).initStreamOffsetFromTime(null);
		}
	}

	@Nested
	@DisplayName("Method readNormalAndLogCollectorTaskStreamOffset test")
	class readNormalAndLogCollectorTaskStreamOffsetTest {
		private SyncProgress syncProgress;

		@BeforeEach
		void setUp() {
			instance = spy(instance);
			syncProgress = new SyncProgress();
			ReflectionTestUtils.setField(instance, "syncProgress", syncProgress);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			doReturn(connectorNode).when(instance).getConnectorNode();
		}

		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			Map<String, Object> fakeOffset = new HashMap<>();
			fakeOffset.put("test", 1);
			instance.readNormalAndLogCollectorTaskStreamOffset(PdkUtil.encodeOffset(fakeOffset));

			assertNotNull(syncProgress.getStreamOffsetObj());
			assertEquals(1, ((Map) syncProgress.getStreamOffsetObj()).size());
			assertEquals(1, ((Map) syncProgress.getStreamOffsetObj()).get("test"));
		}

		@Test
		@DisplayName("test stream offset is null and syncType is initial+cdc")
		void testStreamOffsetIsNull1() {
			SyncTypeEnum syncTypeEnum = SyncTypeEnum.INITIAL_SYNC_CDC;
			ReflectionTestUtils.setField(instance, "syncType", syncTypeEnum);
			doAnswer(invocationOnMock -> null).when(instance).initStreamOffsetFromTime(null);
			instance.readNormalAndLogCollectorTaskStreamOffset(null);

			verify(instance, times(1)).initStreamOffsetFromTime(null);
		}

		@Test
		@DisplayName("test stream offset is null and syncType is cdc")
		void testStreamOffsetIsNull2() {
			SyncTypeEnum syncTypeEnum = SyncTypeEnum.CDC;
			ReflectionTestUtils.setField(instance, "syncType", syncTypeEnum);
			doAnswer(invocationOnMock -> null).when(instance).initStreamOffsetFromTime(null);
			instance.readNormalAndLogCollectorTaskStreamOffset(null);

			verify(instance, times(1)).initStreamOffsetFromTime(null);
		}

		@Test
		@DisplayName("test stream offset is null and syncType is initial")
		void testStreamOffsetIsNull3() {
			SyncTypeEnum syncTypeEnum = SyncTypeEnum.INITIAL_SYNC;
			ReflectionTestUtils.setField(instance, "syncType", syncTypeEnum);
			doAnswer(invocationOnMock -> null).when(instance).initStreamOffsetFromTime(null);
			instance.readNormalAndLogCollectorTaskStreamOffset(null);

			verify(instance, never()).initStreamOffsetFromTime(null);
		}
	}

	@Nested
	@DisplayName("Method readBatchOffset test")
	class readBatchOffsetTest {

		private SyncProgress syncProgress;

		@BeforeEach
		void setUp() {
			instance = spy(instance);
			syncProgress = new SyncProgress();
			ReflectionTestUtils.setField(instance, "syncProgress", syncProgress);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			doReturn(connectorNode).when(instance).getConnectorNode();
		}

		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			Map<String, Object> fakeBatchOffset = new HashMap<>();
			fakeBatchOffset.put("test", 1);
			syncProgress.setBatchOffset(PdkUtil.encodeOffset(fakeBatchOffset));
			instance.readBatchOffset();
			assertNotNull(syncProgress.getBatchOffsetObj());
			assertInstanceOf(Map.class, syncProgress.getBatchOffsetObj());
			assertEquals(1, ((Map) syncProgress.getBatchOffsetObj()).get("test"));
		}

		@Test
		@DisplayName("test sync progress is null")
		void testSyncProgressIsNull() {
			ReflectionTestUtils.setField(instance, "syncProgress", null);
			assertDoesNotThrow(() -> instance.readBatchOffset());
		}

		@Test
		@DisplayName("test batch offset is null")
		void testBatchOffsetIsNull() {
			syncProgress.setBatchOffset(null);
			assertDoesNotThrow(() -> instance.readBatchOffset());

			assertNotNull(syncProgress.getBatchOffsetObj());
			assertInstanceOf(HashMap.class, syncProgress.getBatchOffsetObj());
			assertTrue(((Map) syncProgress.getBatchOffsetObj()).isEmpty());
		}
	}

	@Nested
	@DisplayName("Method initBatchAndStreamOffsetFirstTime test")
	class initBatchAndStreamOffsetFirstTimeTest {
		@BeforeEach
		void setUp() {
			instance = spy(instance);
			Map<String, Object> fakeStreamOffset = new HashMap<>();
			fakeStreamOffset.put("test", 1);
			doAnswer(invocationOnMock -> {
				Object syncProgress = ReflectionTestUtils.getField(instance, "syncProgress");
				assertInstanceOf(SyncProgress.class, syncProgress);
				((SyncProgress) syncProgress).setStreamOffsetObj(fakeStreamOffset);
				return null;
			}).when(instance).initStreamOffsetInitialAndCDC(null);
			doCallRealMethod().when(instance).initStreamOffsetInitial();
			doAnswer(invocationOnMock -> {
				Object syncProgress = ReflectionTestUtils.getField(instance, "syncProgress");
				assertInstanceOf(SyncProgress.class, syncProgress);
				((SyncProgress) syncProgress).setStreamOffsetObj(fakeStreamOffset);
				return 1L;
			}).when(instance).initStreamOffsetCDC(any(TaskDto.class), eq(null));
			doAnswer(invocationOnMock -> null).when(instance).enqueue(any(TapdataEvent.class));
		}

		@Test
		@DisplayName("test initial+cdc sync type")
		void testInitialCdcSyncType() {
			ReflectionTestUtils.setField(instance, "syncType", SyncTypeEnum.INITIAL_SYNC_CDC);
			instance.initBatchAndStreamOffsetFirstTime(dataProcessorContext.getTaskDto());

			SyncProgress actualSyncProgress = instance.syncProgress;
			assertNotNull(actualSyncProgress);
			verify(instance, times(1)).initStreamOffsetInitialAndCDC(null);
			verify(instance, times(1)).enqueue(any(TapdataEvent.class));
			verify(instance, never()).initStreamOffsetInitial();
			verify(instance, never()).initStreamOffsetCDC(any(TaskDto.class), anyLong());
			assertNotNull(actualSyncProgress.getBatchOffsetObj());
			assertInstanceOf(HashMap.class, actualSyncProgress.getBatchOffsetObj());
			assertTrue(((Map) actualSyncProgress.getBatchOffsetObj()).isEmpty());
			assertNotNull(actualSyncProgress.getStreamOffsetObj());
			assertInstanceOf(HashMap.class, actualSyncProgress.getStreamOffsetObj());
			assertEquals(1, ((Map) actualSyncProgress.getStreamOffsetObj()).get("test"));
		}

		@Test
		@DisplayName("test initial sync type")
		void testInitialSyncType() {
			ReflectionTestUtils.setField(instance, "syncType", SyncTypeEnum.INITIAL_SYNC);
			instance.initBatchAndStreamOffsetFirstTime(dataProcessorContext.getTaskDto());

			SyncProgress actualSyncProgress = instance.syncProgress;
			assertNotNull(actualSyncProgress);
			verify(instance, never()).initStreamOffsetInitialAndCDC(null);
			verify(instance, never()).enqueue(any(TapdataEvent.class));
			verify(instance, times(1)).initStreamOffsetInitial();
			verify(instance, never()).initStreamOffsetCDC(any(TaskDto.class), anyLong());
			assertNotNull(actualSyncProgress.getBatchOffsetObj());
			assertInstanceOf(HashMap.class, actualSyncProgress.getBatchOffsetObj());
			assertTrue(((Map) actualSyncProgress.getBatchOffsetObj()).isEmpty());
			assertNull(actualSyncProgress.getStreamOffsetObj());
			assertEquals(SyncStage.INITIAL_SYNC.name(), actualSyncProgress.getSyncStage());
		}

		@Test
		@DisplayName("test cdc sync type")
		void testCdcSyncType() {
			ReflectionTestUtils.setField(instance, "syncType", SyncTypeEnum.CDC);
			instance.initBatchAndStreamOffsetFirstTime(dataProcessorContext.getTaskDto());

			SyncProgress actualSyncProgress = instance.syncProgress;
			assertNotNull(actualSyncProgress);
			verify(instance, never()).initStreamOffsetInitialAndCDC(null);
			verify(instance, times(1)).enqueue(any(TapdataEvent.class));
			verify(instance, never()).initStreamOffsetInitial();
			verify(instance, times(1)).initStreamOffsetCDC(any(TaskDto.class), eq(null));
		}
	}

	@Nested
	@DisplayName("Method initStreamOffsetCDC test")
	class initStreamOffsetCDCTest {
		@BeforeEach
		void setUp() {
			instance = spy(instance);
			doAnswer(invocationOnMock -> null).when(instance).initStreamOffsetFromTime(null);
			doAnswer(invocationOnMock -> null).when(instance).initStreamOffsetFromTime(anyLong());
		}

		@Test
		@DisplayName("test normal task, current sync point")
		void testCurrentSyncPoint() {
			TaskDto.SyncPoint syncPoint = new TaskDto.SyncPoint();
			syncPoint.setNodeId(instance.getNode().getId());
			syncPoint.setPointType("current");
			List<TaskDto.SyncPoint> syncPoints = new ArrayList<>();
			syncPoints.add(syncPoint);
			dataProcessorContext.getTaskDto().setSyncPoints(syncPoints);
			Long actualReturn = instance.initStreamOffsetCDC(dataProcessorContext.getTaskDto(), null);

			verify(instance, times(1)).initStreamOffsetFromTime(null);
			assertNull(actualReturn);
		}

		@Test
		@DisplayName("test local timezone sync point")
		void testLocalTZSyncPoint() {
			TaskDto.SyncPoint syncPoint = new TaskDto.SyncPoint();
			syncPoint.setNodeId(instance.getNode().getId());
			syncPoint.setPointType("localTZ");
			long syncDateTime = System.currentTimeMillis();
			syncPoint.setDateTime(syncDateTime);
			List<TaskDto.SyncPoint> syncPoints = new ArrayList<>();
			syncPoints.add(syncPoint);
			dataProcessorContext.getTaskDto().setSyncPoints(syncPoints);
			Long actualReturn = instance.initStreamOffsetCDC(dataProcessorContext.getTaskDto(), null);
			assertEquals(syncDateTime, actualReturn);
		}

		@Test
		@SneakyThrows
		@DisplayName("test connection timezone sync point")
		void testConnTZSyncPoint() {
			TaskDto.SyncPoint syncPoint = new TaskDto.SyncPoint();
			syncPoint.setNodeId(instance.getNode().getId());
			syncPoint.setPointType("connTZ");
			long syncDateTime = System.currentTimeMillis();
			syncPoint.setDateTime(syncDateTime);
			List<TaskDto.SyncPoint> syncPoints = new ArrayList<>();
			syncPoints.add(syncPoint);
			dataProcessorContext.getTaskDto().setSyncPoints(syncPoints);
			Long actualReturn = instance.initStreamOffsetCDC(dataProcessorContext.getTaskDto(), null);
			assertEquals(syncDateTime, actualReturn);
		}

		@Test
		@DisplayName("test polling cdc task")
		void testPollingCDCTask() {
			doReturn(true).when(instance).isPollingCDC(any(Node.class));
			SyncProgress syncProgress = new SyncProgress();
			ReflectionTestUtils.setField(instance, "syncProgress", syncProgress);
			Long actualReturn = instance.initStreamOffsetCDC(dataProcessorContext.getTaskDto(), null);

			assertNull(actualReturn);
			assertInstanceOf(SyncProgress.class, syncProgress);
			assertInstanceOf(HashMap.class, syncProgress.getStreamOffsetObj());
			assertTrue(((Map) syncProgress.getStreamOffsetObj()).isEmpty());
		}

		@Test
		@DisplayName("test sync point type is empty")
		void testSyncPointTypeIsEmpty() {
			TaskDto.SyncPoint syncPoint = new TaskDto.SyncPoint();
			syncPoint.setNodeId(instance.getNode().getId());
			syncPoint.setPointType(null);
			List<TaskDto.SyncPoint> syncPoints = new ArrayList<>();
			syncPoints.add(syncPoint);
			dataProcessorContext.getTaskDto().setSyncPoints(syncPoints);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> instance.initStreamOffsetCDC(dataProcessorContext.getTaskDto(), null));
			assertEquals(TaskProcessorExCode_11.INIT_STREAM_OFFSET_SYNC_POINT_TYPE_IS_EMPTY, tapCodeException.getCode());
		}

		@Test
		@DisplayName("test sync point is null")
		void testSyncPointIsNull() {
			List<TaskDto.SyncPoint> syncPoints = new ArrayList<>();
			dataProcessorContext.getTaskDto().setSyncPoints(syncPoints);
			Long actualReturn = instance.initStreamOffsetCDC(dataProcessorContext.getTaskDto(), null);

			verify(instance, times(1)).initStreamOffsetFromTime(null);
			assertNull(actualReturn);
		}

		@Test
		@DisplayName("test unknown sync point type")
		void testUnknownSyncPointType() {
			TaskDto.SyncPoint syncPoint = new TaskDto.SyncPoint();
			syncPoint.setNodeId(instance.getNode().getId());
			syncPoint.setPointType("xxx");
			List<TaskDto.SyncPoint> syncPoints = new ArrayList<>();
			syncPoints.add(syncPoint);
			dataProcessorContext.getTaskDto().setSyncPoints(syncPoints);

			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> instance.initStreamOffsetCDC(dataProcessorContext.getTaskDto(), null));
			assertEquals(TaskProcessorExCode_11.INIT_STREAM_OFFSET_UNKNOWN_POINT_TYPE, tapCodeException.getCode());
		}
	}

	@Nested
	@DisplayName("Method initStreamOffsetInitialAndCDC test")
	class initStreamOffsetInitialAndCDCTest {
		@BeforeEach
		void setUp() {
			instance = spy(instance);
		}

		@Test
		@DisplayName("test offset start time is null")
		void testOffsetStartTimeIsNull() {
			doAnswer(invocationOnMock -> null).when(instance).initStreamOffsetFromTime(null);
			instance.initStreamOffsetInitialAndCDC(null);
			verify(instance, times(1)).initStreamOffsetFromTime(null);
			verify(instance, never()).initStreamOffsetFromTime(anyLong());
		}

		@Test
		@DisplayName("test offset start time is not null")
		void testOffsetStartTimeIsNotNull() {
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertEquals(1L, argument1);
				return argument1;
			}).when(instance).initStreamOffsetFromTime(anyLong());
			instance.initStreamOffsetInitialAndCDC(1L);
			verify(instance, times(1)).initStreamOffsetFromTime(1L);
			verify(instance, never()).initStreamOffsetFromTime(null);
		}

		@Test
		@DisplayName("test polling cdc")
		void testPollingCdc() {
			SyncProgress syncProgress = new SyncProgress();
			ReflectionTestUtils.setField(instance, "syncProgress", syncProgress);
			doReturn(true).when(instance).isPollingCDC(any(Node.class));

			instance.initStreamOffsetInitialAndCDC(null);
			assertInstanceOf(HashMap.class, syncProgress.getStreamOffsetObj());
			assertTrue(((Map) syncProgress.getStreamOffsetObj()).isEmpty());
		}
	}

	@Nested
	@DisplayName("Method readPollingCDCStreamOffset test")
	class readPollingCDCStreamOffsetTest {

		private SyncProgress syncProgress;

		@BeforeEach
		void setUp() {
			instance = spy(instance);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			doReturn(connectorNode).when(instance).getConnectorNode();
			syncProgress = new SyncProgress();
			ReflectionTestUtils.setField(instance, "syncProgress", syncProgress);
		}

		@Test
		@DisplayName("test stream offset is not null")
		void testStreamOffsetIsNotNull() {
			Map<String, Object> fakeStreamOffset = new HashMap<>();
			fakeStreamOffset.put("test", 1);
			instance.readPollingCDCStreamOffset(PdkUtil.encodeOffset(fakeStreamOffset));

			assertInstanceOf(HashMap.class, syncProgress.getStreamOffsetObj());
			assertEquals(1, ((Map) syncProgress.getStreamOffsetObj()).get("test"));
		}

		@Test
		@DisplayName("test stream offset is null")
		void testStreamOffsetIsNull() {
			instance.readPollingCDCStreamOffset(null);

			assertInstanceOf(HashMap.class, syncProgress.getStreamOffsetObj());
			assertTrue(((Map) syncProgress.getStreamOffsetObj()).isEmpty());
		}
	}

	@Nested
	@DisplayName("Method readShareCDCStreamOffset test")
	class readShareCDCStreamOffsetTest {
		@BeforeEach
		void setUp() {
			instance = spy(instance);
		}

		@Test
		@DisplayName("test task enabled share cdc")
		void testTaskEnableShareCdc() {
			Connections sourceConn = mock(Connections.class);
			when(sourceConn.isShareCdcEnable()).thenReturn(true);
			when(dataProcessorContext.getSourceConn()).thenReturn(sourceConn);
			dataProcessorContext.getTaskDto().setShareCdcEnable(true);
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertEquals("test", argument1);
				return null;
			}).when(instance).readShareCDCStreamOffsetContinueShareCDC(any(String.class));
			doAnswer(invocationOnMock -> null).when(instance).readShareCDCStreamOffsetSwitchNormalTask(any());
			instance.readShareCDCStreamOffset(dataProcessorContext.getTaskDto(), "test");

			verify(instance, times(1)).readShareCDCStreamOffsetContinueShareCDC(anyString());
			verify(instance, never()).readShareCDCStreamOffsetSwitchNormalTask(any());
		}

		@Test
		@DisplayName("test task disabled share cdc")
		void testTaskDisableShareCdc() {
			Connections sourceConn = mock(Connections.class);
			when(sourceConn.isShareCdcEnable()).thenReturn(false);
			when(dataProcessorContext.getSourceConn()).thenReturn(sourceConn);
			dataProcessorContext.getTaskDto().setShareCdcEnable(false);
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertEquals("test", argument1);
				return null;
			}).when(instance).readShareCDCStreamOffsetSwitchNormalTask(any(String.class));
			doAnswer(invocationOnMock -> null).when(instance).readShareCDCStreamOffsetContinueShareCDC(any());
			instance.readShareCDCStreamOffset(dataProcessorContext.getTaskDto(), "test");

			verify(instance, never()).readShareCDCStreamOffsetContinueShareCDC(any());
			verify(instance, times(1)).readShareCDCStreamOffsetSwitchNormalTask(anyString());
		}
	}

	@Nested
	@DisplayName("Method initDDLFilter test")
	class InitDDLFilterTest {
		private HazelcastSourcePdkDataNode hazelcastSourcePdkDataNode;

		@BeforeEach
		void beforeEach() {
			hazelcastSourcePdkDataNode = spy(new HazelcastSourcePdkDataNode(dataProcessorContext));
		}

		@Test
		void test() {
			DatabaseNode node = new DatabaseNode();
			node.setDisabledEvents(new ArrayList<>());
			node.setIgnoredDDLRules("test");
			node.setDdlConfiguration(DDLConfiguration.ERROR);
			when(dataProcessorContext.getNode()).thenReturn((Node) node);
			hazelcastSourcePdkDataNode.initDDLFilter();
			DDLFilter ddlFilter = (DDLFilter) ReflectionTestUtils.getField(hazelcastSourcePdkDataNode, "ddlFilter");
			ReflectionTestUtils.setField(ddlFilter, "obsLogger", mockObsLogger);
			TapDDLEvent tapDDLEvent = new TapDDLUnknownEvent();
			tapDDLEvent.setOriginDDL("test");
			Assertions.assertFalse(ddlFilter.test(tapDDLEvent));
		}

	}

	@Nested
	@DisplayName("Method restartPdkConnector test")
	class restartPdkConnectorTest {
		@BeforeEach
		void setUp() {
			instance = spy(instance);
		}

		@Test
		@DisplayName("test main process")
		@SneakyThrows
		void testMainProcess() {
			try (
					MockedStatic<PDKInvocationMonitor> pdkInvocationMonitorMockedStatic = mockStatic(PDKInvocationMonitor.class);
					MockedStatic<PDKIntegration> pdkIntegrationMockedStatic = mockStatic(PDKIntegration.class)
			) {
				ThreadPoolExecutorEx sourceRunner = mock(ThreadPoolExecutorEx.class);
				ReflectionTestUtils.setField(instance, "readBatchSize", 100);
				ReflectionTestUtils.setField(instance, "sourceRunner", sourceRunner);
				String associateId = "test_associateId";
				ReflectionTestUtils.setField(instance, "associateId", associateId);
				ConnectorNode connectorNode = mock(ConnectorNode.class);
				when(connectorNode.getAssociateId()).thenReturn(associateId);
				pdkInvocationMonitorMockedStatic.when(() -> PDKInvocationMonitor.invoke(eq(connectorNode), eq(PDKMethod.STOP), any(), anyString()))
						.thenAnswer(invocationOnMock -> null);
				pdkIntegrationMockedStatic.when(() -> PDKIntegration.releaseAssociateId(associateId)).thenAnswer(invocationOnMock -> null);
				doReturn(connectorNode).when(instance).getConnectorNode();
				doAnswer(invocationOnMock -> null).when(instance).createPdkConnectorNode(eq(dataProcessorContext), any(HazelcastInstance.class));
				doAnswer(invocationOnMock -> null).when(instance).connectorNodeInit(dataProcessorContext);
				doAnswer(invocationOnMock -> null).when(instance).initAndStartSourceRunner();
				Processor.Context jetContext = mock(Processor.Context.class);
				when(jetContext.hazelcastInstance()).thenReturn(mock(HazelcastInstance.class));
				ReflectionTestUtils.setField(instance, "jetContext", jetContext);
				ConnectorNodeService.getInstance().putConnectorNode(connectorNode);
				StreamReadFuncAspect streamReadFuncAspect = mock(StreamReadFuncAspect.class);
				ReflectionTestUtils.setField(instance, "streamReadFuncAspect", streamReadFuncAspect);

				assertDoesNotThrow(() -> instance.restartPdkConnector());

				verify(sourceRunner, times(1)).shutdownNow();
				pdkInvocationMonitorMockedStatic.verify(
						() -> PDKInvocationMonitor.invoke(eq(connectorNode), eq(PDKMethod.STOP), any(), anyString()),
						times(1)
				);
				pdkIntegrationMockedStatic.verify(() -> PDKIntegration.releaseAssociateId(associateId), times(1));
				assertNull(ConnectorNodeService.getInstance().getConnectorNode(associateId));
				verify(instance, times(1)).createPdkConnectorNode(eq(dataProcessorContext), any(HazelcastInstance.class));
				verify(instance, times(1)).connectorNodeInit(dataProcessorContext);
				assertNotNull(instance.sourceRunner);
				assertNotEquals(sourceRunner, instance.sourceRunner);
				verify(instance, times(1)).initAndStartSourceRunner();
				verify(streamReadFuncAspect, times(1)).noMoreWaitRawData();
			}
		}

		@Test
		@DisplayName("test getConnectorNode return null")
		void testWhenConnectorNodeIsNull() {
			doReturn(null).when(instance).getConnectorNode();
			doReturn(null).when(instance).errorHandle(any(Throwable.class), anyString());

			assertDoesNotThrow(() -> instance.restartPdkConnector());

			verify(instance, times(1)).errorHandle(any(Throwable.class), anyString());
		}
	}


	@Test
	void testHandleCustomCommandResultForList() {
		HazelcastSourcePdkDataNode hazelcastSourcePdkDataNode = Mockito.mock(HazelcastSourcePdkDataNode.class);
		List<Map<String, Object>> list = new ArrayList<>();
		Map<String, Object> map = new HashMap<>();
		map.put("id", 1);
		list.add(map);
		String tableName = "test";
		BiConsumer<List<TapEvent>, Object> consumer = new BiConsumer<List<TapEvent>, Object>() {
			@Override
			public void accept(List<TapEvent> tapEvents, Object o) {
				TapEvent tapEvent = tapEvents.get(0);
				Assert.assertEquals(tableName, ((TapInsertRecordEvent) tapEvent).getTableId());
			}
		};
		ReflectionTestUtils.invokeMethod(hazelcastSourcePdkDataNode, "handleCustomCommandResult",
				list, tableName, consumer);
	}


	@Test
	void testHandleCustomCommandResultForLong() {
		HazelcastSourcePdkDataNode hazelcastSourcePdkDataNode = Mockito.mock(HazelcastSourcePdkDataNode.class);
		String tableName = "test";
		long excepted = 8L;
		BiConsumer<List<TapEvent>, Object> consumer = new BiConsumer<List<TapEvent>, Object>() {
			@Override
			public void accept(List<TapEvent> tapEvents, Object o) {
				Assert.assertTrue(!tapEvents.isEmpty());
			}
		};
		ReflectionTestUtils.setField(hazelcastSourcePdkDataNode, "obsLogger", Mockito.mock(ObsLogger.class));
		ReflectionTestUtils.invokeMethod(hazelcastSourcePdkDataNode, "handleCustomCommandResult",
				excepted, tableName, consumer);
	}

	@Nested
	@DisplayName("Method wrapSingleTapdataEvent test")
	class wrapSingleTapdataEventTest {
		@BeforeEach
		void setUp() {
			instance = spy(instance);
			ReflectionTestUtils.setField(instance, "obsLogger", mockObsLogger);
		}

		@Test
		@DisplayName("test source mode=LOG_COLLECTOR, TapEvent is a TapDDLUnknownEvent, expect return null")
		void test1() {
			HazelcastSourcePdkBaseNode.SourceMode sourceMode = HazelcastSourcePdkBaseNode.SourceMode.LOG_COLLECTOR;
			ReflectionTestUtils.setField(instance, "sourceMode", sourceMode);
			TapDDLUnknownEvent tapDDLUnknownEvent = new TapDDLUnknownEvent();
			tapDDLUnknownEvent.setReferenceTime(System.currentTimeMillis());
			tapDDLUnknownEvent.setTime(System.currentTimeMillis());
			tapDDLUnknownEvent.setOriginDDL("alter table xxx add new_field number(8,0)");
			TapdataEvent tapdataEvent = instance.wrapSingleTapdataEvent(tapDDLUnknownEvent, SyncStage.CDC, null, true);
			assertNull(tapdataEvent);
			verify(mockObsLogger).warn(any());
		}
	}

	@Nested
	class WrapTapdataEventTest {
		private HazelcastSourcePdkDataNode hazelcastSourcePdkDataNode;
		private ICdcDelay cdcDelay;

		@BeforeEach
		void setUp() {
			hazelcastSourcePdkDataNode = spy(new HazelcastSourcePdkDataNode(dataProcessorContext));
			cdcDelay = mock(CdcDelay.class);
		}

		@DisplayName("test wrapTapdataEvent tapEventTime not null")
		@Test
		void test1() {
			List<TapEvent> tapEvents = new ArrayList<>();
			TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
			tapUpdateRecordEvent.setTableId("testTableId");
			tapUpdateRecordEvent.setTime(System.currentTimeMillis());
			tapEvents.add(tapUpdateRecordEvent);
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode, "cdcDelayCalculation", cdcDelay);
			hazelcastSourcePdkDataNode.wrapTapdataEvent(tapEvents, null, null);
		}

		@DisplayName("test wrapTapdataEvent tapEventTime is null")
		@Test
		void test2() {
			List<TapEvent> tapEvents = new ArrayList<>();
			TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
			tapUpdateRecordEvent.setTableId("testTableId");
			tapEvents.add(tapUpdateRecordEvent);
			assertThrows(NodeException.class, () -> hazelcastSourcePdkDataNode.wrapTapdataEvent(tapEvents, null, null));
		}
	}

	@Nested
	@DisplayName("Method initToTapValueConcurrent test")
	class initToTapValueConcurrentTest {
		@Test
		@DisplayName("test default config, expect not enable concurrent transform to tapValue")
		void test1() {
			instance.initToTapValueConcurrent();

			Object toTapValueConcurrent = ReflectionTestUtils.getField(instance, "toTapValueConcurrent");
			assertInstanceOf(Boolean.class, toTapValueConcurrent);
			assertFalse((Boolean) toTapValueConcurrent);
			Object toTapValueConcurrentProcessor = ReflectionTestUtils.getField(instance, "toTapValueConcurrentProcessor");
			assertNull(toTapValueConcurrentProcessor);
		}

		@Test
		@DisplayName("test enable concurrent transform to tapValue")
		void test2() {
			System.setProperty(HazelcastSourcePdkBaseNode.SOURCE_TO_TAP_VALUE_CONCURRENT_PROP_KEY, "true");
			ReflectionTestUtils.setField(instance, "readBatchSize", 100);
//			ThreadPoolExecutorEx toTapValueRunner = mock(ThreadPoolExecutorEx.class);
//			ReflectionTestUtils.setField(instance, "toTapValueRunner", toTapValueRunner);

			instance.initToTapValueConcurrent();

			Object toTapValueConcurrent = ReflectionTestUtils.getField(instance, "toTapValueConcurrent");
			assertInstanceOf(Boolean.class, toTapValueConcurrent);
			assertTrue((Boolean) toTapValueConcurrent);
			Object toTapValueConcurrentProcessor = ReflectionTestUtils.getField(instance, "toTapValueConcurrentProcessor");
			assertInstanceOf(SimpleConcurrentProcessorImpl.class, toTapValueConcurrentProcessor);
			Object toTapValueRunner = ReflectionTestUtils.getField(instance, "toTapValueRunner");
			assertInstanceOf(ThreadPoolExecutorEx.class, toTapValueRunner);
		}
	}

	@Nested
	@DisplayName("Method concurrentToTapValueConsumer test")
	class concurrentToTapValueConsumerTest {
		@BeforeEach
		void setUp() {
			instance = spy(instance);
		}

		@Test
		@DisplayName("test main process")
		@SneakyThrows
		void test1() {
			CountDownLatch countDownLatch = new CountDownLatch(1000);
			int threadNum = 5;
			LinkedBlockingQueue<TapdataEvent> eventQueue = new LinkedBlockingQueue<>(Long.valueOf(countDownLatch.getCount()).intValue());
			IntStream.range(0, Long.valueOf(countDownLatch.getCount()).intValue()).forEach(i -> eventQueue.offer(new TapdataEvent()));
			ReflectionTestUtils.setField(instance, "eventQueue", eventQueue);
			ReflectionTestUtils.setField(instance, "readBatchSize", Long.valueOf(countDownLatch.getCount()).intValue());
			ReflectionTestUtils.setField(instance, "toTapValueBatchSize", 10);
			SimpleConcurrentProcessorImpl<Object, Object> simpleConcurrentProcessor = TapExecutors.createSimple(threadNum, 10, "test");
			ReflectionTestUtils.setField(instance, "toTapValueConcurrentProcessor", simpleConcurrentProcessor);
			doAnswer(invocationOnMock -> {
				Object argument = invocationOnMock.getArgument(0);
				assertInstanceOf(List.class, argument);
				List<TapdataEvent> tapdataEvents = (List<TapdataEvent>) argument;
				for (TapdataEvent tapdataEvent : tapdataEvents) {
					tapdataEvent.addInfo("test", 1);
				}
				return null;
			}).when(instance).batchTransformToTapValue(any());
			AtomicBoolean running = new AtomicBoolean(true);
			ReflectionTestUtils.setField(instance, "running", running);

			new Thread(instance::concurrentToTapValueConsumer).start();
			List<Object> results = new ArrayList();
			new Thread(() -> {
				while (countDownLatch.getCount() > 0) {
					Object o = assertDoesNotThrow(() -> simpleConcurrentProcessor.get());
					assertInstanceOf(List.class, o);
					for (Object result : (List<?>) o) {
						results.add(result);
						countDownLatch.countDown();
					}
				}
			}).start();
			countDownLatch.await();
			running.set(false);

			for (Object result : results) {
				assertInstanceOf(TapdataEvent.class, result);
				TapdataEvent tapdataEvent = (TapdataEvent) result;
				assertEquals(1, tapdataEvent.getInfo("test"));
			}
			simpleConcurrentProcessor.close();
		}
	}

	@Nested
	@DisplayName("Method doClose test")
	class doCloseTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			doCallRealMethod().when(mockInstance).doClose();
			ReflectionTestUtils.setField(mockInstance, "obsLogger", mockObsLogger);
			when(mockInstance.getNode()).thenReturn((Node) tableNode);
			final Object waitObj = new Object();
			ReflectionTestUtils.setField(mockInstance, "waitObj", waitObj);
			Thread thread = new Thread(() -> {
				synchronized (waitObj) {
					try {
						waitObj.wait();
					} catch (InterruptedException ignored) {
					}
				}
			});
			thread.start();
			ScheduledExecutorService tableMonitorResultHandler = mock(ScheduledExecutorService.class);
			ReflectionTestUtils.setField(mockInstance, "tableMonitorResultHandler", tableMonitorResultHandler);
			ThreadPoolExecutorEx sourceRunner = mock(ThreadPoolExecutorEx.class);
			ReflectionTestUtils.setField(mockInstance, "sourceRunner", sourceRunner);
			ThreadPoolExecutorEx toTapValueRunner = mock(ThreadPoolExecutorEx.class);
			ReflectionTestUtils.setField(mockInstance, "toTapValueRunner", toTapValueRunner);
			SimpleConcurrentProcessorImpl<List<TapdataEvent>, List<TapdataEvent>> toTapValueConcurrentProcessor = mock(SimpleConcurrentProcessorImpl.class);
			ReflectionTestUtils.setField(mockInstance, "toTapValueConcurrentProcessor", toTapValueConcurrentProcessor);

			mockInstance.doClose();

			verify(tableMonitorResultHandler).shutdownNow();
			verify(sourceRunner).shutdownNow();
			verify(toTapValueRunner).shutdownNow();
			verify(toTapValueConcurrentProcessor).close();
			assertFalse(thread.isAlive());
		}
	}
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.ex.TestException;
import base.hazelcast.BaseHazelcastNodeTest;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Processor;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TransformToTapValueResult;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.cdcdelay.CdcDelay;
import com.tapdata.tm.commons.cdcdelay.ICdcDelay;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.DDLConfiguration;
import com.tapdata.tm.commons.dag.Element;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.process.UnionProcessorNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import com.tapdata.tm.commons.util.NoPrimaryKeyVirtualField;
import io.tapdata.aspect.BatchSizeAspect;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.TableCountFuncAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.TaskAspectManager;
import io.tapdata.common.concurrent.SimpleConcurrentProcessorImpl;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.schema.partition.TapSubPartitionTableInfo;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.error.TaskProcessorExCode_11;
import io.github.openlg.graphlib.Graph;
import io.tapdata.exception.NodeException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.ddl.DDLFilter;
import io.tapdata.flow.engine.V2.ddl.DDLSchemaHandler;
import io.tapdata.flow.engine.V2.filter.TargetTableDataEventFilter;
import io.tapdata.flow.engine.V2.monitor.Monitor;
import io.tapdata.flow.engine.V2.monitor.MonitorManager;
import io.tapdata.flow.engine.V2.monitor.impl.JetJobStatusMonitor;
import io.tapdata.flow.engine.V2.monitor.impl.PartitionTableMonitor;
import io.tapdata.flow.engine.V2.monitor.impl.TableMonitor;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.DynamicLinkedBlockingQueue;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.sharecdc.ShareCDCOffset;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapPartitionResult;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.GetStreamOffsetFunction;
import io.tapdata.pdk.apis.functions.connector.source.QueryPartitionTablesByParentName;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.async.AsyncUtils;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import io.tapdata.supervisor.TaskResourceSupervisorManager;
import lombok.SneakyThrows;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import io.tapdata.entity.CountResult;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/11/24 12:18 Create
 */
@DisplayName("HazelcastSourcePdkBaseNode Class Test")
class HazelcastSourcePdkBaseNodeTest extends BaseHazelcastNodeTest {

	class HazelcastSourcePdkBaseNodeImp extends HazelcastSourcePdkBaseNode {
		public HazelcastSourcePdkBaseNodeImp(DataProcessorContext dataProcessorContext) {
			super(dataProcessorContext);
		}

		@Override
		void startSourceRunner() {

		}
		@Override
		public boolean isRunning() {
			return super.isRunning();
		}

		@Override
		public boolean offer(TapdataEvent tapEvent) {
			return super.offer(tapEvent);
		}
	}
	private HazelcastSourcePdkBaseNodeImp instance;
	private MockHazelcastSourcePdkBaseNode mockInstance;
	SyncProgress syncProgress;
	ObsLogger log;

	@BeforeEach
	void beforeEach() {
		super.allSetup();
		log = mock(ObsLogger.class);
		mockInstance = mock(MockHazelcastSourcePdkBaseNode.class);
		ReflectionTestUtils.setField(mockInstance, "processorBaseContext", processorBaseContext);
		ReflectionTestUtils.setField(mockInstance, "dataProcessorContext", dataProcessorContext);
		when(mockInstance.getDataProcessorContext()).thenReturn(dataProcessorContext);
		instance = new HazelcastSourcePdkBaseNodeImp(dataProcessorContext);
		syncProgress = mock(SyncProgress.class);
		ReflectionTestUtils.setField(mockInstance, "syncProgress", syncProgress);
		ReflectionTestUtils.setField(mockInstance, "obsLogger", log);
		doNothing().when(log).warn(anyString(), any(Object[].class));
		doNothing().when(log).info(anyString(), any(Object[].class));
		doNothing().when(log).debug(anyString(), any(Object[].class));
		doNothing().when(log).error(anyString(), any(Object[].class));
	}

	@Nested
	@DisplayName("doInit methods test")
	class DoInitTest {
		@BeforeEach
		void beforeEach() {
			TaskResourceSupervisorManager taskResourceSupervisorManager = new TaskResourceSupervisorManager();
			ReflectionTestUtils.setField(mockInstance, "taskResourceSupervisorManager", taskResourceSupervisorManager);
			ReflectionTestUtils.setField(mockInstance, "noPrimaryKeyVirtualField", new NoPrimaryKeyVirtualField());
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
		@Nested
		class TestInitSyncProgress{
			@SneakyThrows
			@Test
			void test1(){
				TaskDto taskDto = new TaskDto();
				taskDto.setSyncType(TaskDto.SYNC_TYPE_DEDUCE_SCHEMA);
				taskDto.setType(TaskDto.SYNC_TYPE_DEDUCE_SCHEMA);
				DataProcessorContext dataProcessorContext = DataProcessorContext.newBuilder().withTaskDto(taskDto).build();
				ObsLogger obsLogger = mock(ObsLogger.class);
				HazelcastSourcePdkBaseNode spyInstance = Mockito.spy(instance);
				ReflectionTestUtils.setField(spyInstance,"dataProcessorContext",dataProcessorContext);
				ReflectionTestUtils.setField(spyInstance,"obsLogger",obsLogger);
				spyInstance.initSyncProgress();
				verify(obsLogger,times(1)).trace(anyString(),anyString());
			}
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
		doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).readBatchOffset(syncProgress);
		doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).readStreamOffset(any(TaskDto.class));
		hazelcastSourcePdkDataNode.readBatchAndStreamOffset(dataProcessorContext.getTaskDto());
		verify(hazelcastSourcePdkDataNode, times(1)).readBatchOffset(any());
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
			TapTableMap tapTableMap = mock(TapTableMap.class);
			Set<String> tableIds = new HashSet<>();
			tableIds.add("test");
			when(tapTableMap.keySet()).thenReturn(tableIds);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
		}

		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			Map<String, Object> fakeBatchOffset = new HashMap<>();
			fakeBatchOffset.put("test", 1);
			syncProgress.setBatchOffset(PdkUtil.encodeOffset(fakeBatchOffset));
			instance.readBatchOffset(syncProgress);
			assertNotNull(syncProgress.getBatchOffsetObj());
			assertInstanceOf(Map.class, syncProgress.getBatchOffsetObj());
			assertEquals(1, ((Map) syncProgress.getBatchOffsetObj()).get("test"));
		}

		@Test
		@DisplayName("test sync progress is null")
		void testSyncProgressIsNull() {
			ReflectionTestUtils.setField(instance, "syncProgress", null);
			assertDoesNotThrow(() -> instance.readBatchOffset(syncProgress));
		}

		@Test
		@Disabled
		@DisplayName("test batch offset is null")
		void testBatchOffsetIsNull() {
			syncProgress.setBatchOffset(null);
			assertDoesNotThrow(() -> instance.readBatchOffset(syncProgress));

			assertNotNull(syncProgress.getBatchOffsetObj());
			assertInstanceOf(ConcurrentHashMap.class, syncProgress.getBatchOffsetObj());
			assertTrue(((Map) syncProgress.getBatchOffsetObj()).isEmpty());
		}

		@Test
		@Disabled
		@DisplayName("test when connector node is null")
		void test1() {
			doReturn(null).when(instance).getConnectorNode();
			Map<String, Object> fakeBatchOffset = new HashMap<>();
			fakeBatchOffset.put("test", 1);
			syncProgress.setBatchOffset(PdkUtil.encodeOffset(fakeBatchOffset));
			instance.readBatchOffset(syncProgress);
			assertInstanceOf(ConcurrentHashMap.class, syncProgress.getBatchOffsetObj());
			assertTrue(((Map<?, ?>) syncProgress.getBatchOffsetObj()).isEmpty());
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
		@Disabled
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
			assertInstanceOf(ConcurrentHashMap.class, actualSyncProgress.getBatchOffsetObj());
			assertTrue(((Map) actualSyncProgress.getBatchOffsetObj()).isEmpty());
			assertNotNull(actualSyncProgress.getStreamOffsetObj());
			assertInstanceOf(HashMap.class, actualSyncProgress.getStreamOffsetObj());
			assertEquals(1, ((Map) actualSyncProgress.getStreamOffsetObj()).get("test"));
		}

		@Test
		@Disabled
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
			assertInstanceOf(ConcurrentHashMap.class, actualSyncProgress.getBatchOffsetObj());
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
			doAnswer(invocationOnMock -> null).when(instance).initStreamOffsetFromString(anyString());
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

		@Test
		@DisplayName("test SyncPoint is String Offset")
		void testStringOffset(){
			TaskDto.SyncPoint syncPoint = new TaskDto.SyncPoint();
			syncPoint.setNodeId(instance.getNode().getId());
			syncPoint.setIsStreamOffset(true);
			syncPoint.setStreamOffsetString("{\"fno\":0,\"timestamp\":1733241600094}");
			List<TaskDto.SyncPoint> syncPoints = new ArrayList<>();
			syncPoints.add(syncPoint);
			dataProcessorContext.getTaskDto().setSyncPoints(syncPoints);
			Long actualReturn = instance.initStreamOffsetCDC(dataProcessorContext.getTaskDto(), null);
			assertNull(actualReturn);
		}
	}
	@Nested
	@DisplayName("Method initStreamOffsetFromStringTest test")
	class initStreamOffsetFromStringTest{
		SyncProgress syncProgress;
		@BeforeEach
		void setUp() {
			instance = spy(instance);
			syncProgress = new SyncProgress();
			ReflectionTestUtils.setField(instance, "syncProgress", syncProgress);

		}
		@Test
		void test_main(){
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			Object streamOffset = new Object();
			doReturn(connectorNode).when(instance).getConnectorNode();
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			GetStreamOffsetFunction getStreamOffsetFunction = mock(GetStreamOffsetFunction.class);
			when(connectorFunctions.getGetStreamOffsetFunction()).thenReturn(getStreamOffsetFunction);
			when(getStreamOffsetFunction.getStreamOffset(any(), anyString())).thenReturn(streamOffset);
			try(MockedStatic<PDKInvocationMonitor> pdkInvocationMonitorMock = Mockito.mockStatic(PDKInvocationMonitor.class)){
				pdkInvocationMonitorMock.when(() -> PDKInvocationMonitor.invoke(any(io.tapdata.pdk.core.api.Node.class), any(PDKMethod.class), any(CommonUtils.AnyError.class), anyString())).thenAnswer(invocationOnMock -> {
					CommonUtils.AnyError argument1 = invocationOnMock.getArgument(2);
					argument1.run();
					return null;
				});
				instance.initStreamOffsetFromString("{\"fno\":0,\"timestamp\":1733241600094}");
				assertEquals(streamOffset, syncProgress.getStreamOffsetObj());
			}

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
				MonitorManager monitorManager = mock(MonitorManager.class);
				TableMonitor monitor = mock(TableMonitor.class);
				when(monitorManager.getMonitorByType(MonitorManager.MonitorType.TABLE_MONITOR)).thenAnswer(invocationOnMock -> monitor);
				ReflectionTestUtils.setField(instance, "monitorManager", monitorManager);

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
				verify(monitor).setAssociateId(anyString());
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

		@Test
		void testPartitionTable() {
			Node node = new DatabaseNode();
			node.setId("nodeId");
			node.setName("nodeName");
			DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
			when(dataProcessorContext.getNode()).thenReturn(node);
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
			HazelcastSourcePdkBaseNode baseNode = new HazelcastSourcePdkBaseNode(dataProcessorContext) {
				@Override
				void startSourceRunner() {

				}
			};

			ThreadPoolExecutorEx sourceRunner = mock(ThreadPoolExecutorEx.class);
			MonitorManager monitorManager = mock(MonitorManager.class);
			ReflectionTestUtils.setField(baseNode, "sourceRunner", sourceRunner);
			ReflectionTestUtils.setField(baseNode, "associateId", "associateId");
			ReflectionTestUtils.setField(baseNode, "monitorManager", monitorManager);
			ReflectionTestUtils.setField(baseNode, "jetContext", mock(Processor.Context.class));

			HazelcastSourcePdkBaseNode spyBaseNode = spy(baseNode);

			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(spyBaseNode.getConnectorNode()).thenReturn(connectorNode);
			doNothing().when(spyBaseNode).createPdkConnectorNode(any(), any());
			doNothing().when(spyBaseNode).connectorNodeInit(any());
			doNothing().when(spyBaseNode).initAndStartSourceRunner();

			Monitor monitor = mock(PartitionTableMonitor.class);
			when(monitorManager.getMonitorByType(eq(MonitorManager.MonitorType.TABLE_MONITOR))).thenReturn(null);
			when(monitorManager.getMonitorByType(eq(MonitorManager.MonitorType.PARTITION_TABLE_MONITOR)))
					.thenReturn(monitor);

			Assertions.assertDoesNotThrow(spyBaseNode::restartPdkConnector);
			verify(spyBaseNode, times(1)).initAndStartSourceRunner();
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
			verify(mockObsLogger, atLeastOnce()).warn(any());
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
			DynamicLinkedBlockingQueue<TapdataEvent> eventQueue = new DynamicLinkedBlockingQueue<>(Long.valueOf(countDownLatch.getCount()).intValue());
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

	@Test
	public void testHasMergeNode() {
		DataProcessorContext context = mock(DataProcessorContext.class);
		TaskDto taskDto = mock(TaskDto.class);
		DAG dag = mock(DAG.class);
		when(context.getTaskDto()).thenReturn(taskDto);
		when(taskDto.getType()).thenReturn("initial_sync");
		when(taskDto.getDag()).thenReturn(dag);

		List<Node> nodes = new ArrayList<>();

		when(dag.getNodes()).thenReturn(nodes);
		HazelcastSourcePdkBaseNode node = new HazelcastSourcePdkBaseNode(context) {
			@Override
			void startSourceRunner() {

			}
		};

		Assertions.assertFalse(node.hasMergeNode());

		nodes.add(new DatabaseNode());
		Assertions.assertFalse(node.hasMergeNode());

		nodes.add(new MergeTableNode());
		Assertions.assertTrue(node.hasMergeNode());

		nodes.clear();
		nodes.add(new UnionProcessorNode());
		Assertions.assertTrue(node.hasMergeNode());

		nodes.clear();
		nodes.add(new JoinProcessorNode());
		Assertions.assertTrue(node.hasMergeNode());

		nodes.clear();
		nodes.add(new DatabaseNode());
		nodes.add(new JoinProcessorNode());
		Assertions.assertTrue(node.hasMergeNode());
	}

	@Test
	public void testInitSyncPartitionTableEnable() {
		DataProcessorContext context = mock(DataProcessorContext.class);
		TaskDto taskDto = new TaskDto();
		taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		when(context.getTaskDto()).thenReturn(taskDto);
		Node node = new DatabaseNode();
		((DatabaseNode)node).setSyncSourcePartitionTableEnable(true);
		when(context.getNode()).thenReturn(node);

		List<TapdataEvent> result = new ArrayList<>();
		HazelcastSourcePdkBaseNode sourceNode = new HazelcastSourcePdkBaseNode(context) {
			@Override
			void startSourceRunner() {

			}
		};
		ReflectionTestUtils.setField(sourceNode, "obsLogger", mock(ObsLogger.class));
		sourceNode.initSyncPartitionTableEnable();
		Assertions.assertEquals(true, sourceNode.syncSourcePartitionTableEnable);
	}

	@Nested
	class testCheckSubPartitionTableHasBeCreated {
		private HazelcastSourcePdkBaseNode sourceBaseNode;
		private DataProcessorContext context;
		private TaskDto taskDto;

		@BeforeEach
		public void beforeEach() {
			context = mock(DataProcessorContext.class);
			taskDto = new TaskDto();
			taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			when(context.getTaskDto()).thenReturn(taskDto);

			Node node = new DatabaseNode();
			((DatabaseNode)node).setSyncSourcePartitionTableEnable(true);
			when(context.getNode()).thenReturn(node);

			sourceBaseNode = new HazelcastSourcePdkBaseNode(context) {
				@Override
				void startSourceRunner() {

				}
			};
		}

		@Test
		void testContainsInPartitionTableMap() {
			TapTable table = new TapTable();
			table.setId("test");
			sourceBaseNode.partitionTableSubMasterMap.put("test", table);

			Assertions.assertTrue(sourceBaseNode.checkSubPartitionTableHasBeCreated(table));
		}

		@Test
		void testSubPartitionTable() {
			TapTable table = new TapTable();
			table.setId("test_1");
			table.setPartitionMasterTableId("test");
			table.setPartitionInfo(new TapPartition());

			HazelcastSourcePdkBaseNode spySourceBaseNode = spy(sourceBaseNode);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapConnectorContext connectorContext = mock(TapConnectorContext.class);
			KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
				@Override
				public TapTable get(String key) {
					return null;
				}
			};
			when(connectorContext.getTableMap()).thenReturn(tableMap);
			when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
			when(spySourceBaseNode.getConnectorNode()).thenReturn(connectorNode);
			Assertions.assertFalse(spySourceBaseNode.checkSubPartitionTableHasBeCreated(table));

			tableMap = new KVReadOnlyMap<TapTable>() {
				@Override
				public TapTable get(String key) {
					TapTable tapTable = new TapTable();
					return tapTable;
				}
			};
			when(connectorContext.getTableMap()).thenReturn(tableMap);
			Assertions.assertFalse(spySourceBaseNode.checkSubPartitionTableHasBeCreated(table));
		}

		@Test
		void testSubPartitionTable1() {
			TapTable table = new TapTable();
			table.setId("test_1");
			table.setPartitionMasterTableId("test");
			table.setPartitionInfo(new TapPartition());

			HazelcastSourcePdkBaseNode spySourceBaseNode = spy(sourceBaseNode);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapConnectorContext connectorContext = mock(TapConnectorContext.class);

			when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
			when(spySourceBaseNode.getConnectorNode()).thenReturn(connectorNode);

			KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
				@Override
				public TapTable get(String key) {
					TapTable tapTable = new TapTable();
					tapTable.setPartitionInfo(new TapPartition());
					return tapTable;
				}
			};
			when(connectorContext.getTableMap()).thenReturn(tableMap);
			Assertions.assertFalse(spySourceBaseNode.checkSubPartitionTableHasBeCreated(table));

			tableMap = new KVReadOnlyMap<TapTable>() {
				@Override
				public TapTable get(String key) {
					TapTable tapTable = new TapTable();
					tapTable.setPartitionInfo(new TapPartition());
					List<TapSubPartitionTableInfo> subPartitionTableList = new ArrayList<>();

					TapSubPartitionTableInfo subPartitionTable = new TapSubPartitionTableInfo();
					subPartitionTable.setTableName("test_1");
					subPartitionTableList.add(subPartitionTable);

					tapTable.getPartitionInfo().setSubPartitionTableInfo(subPartitionTableList);
					return tapTable;
				}
			};

			when(connectorContext.getTableMap()).thenReturn(tableMap);
			Assertions.assertTrue(spySourceBaseNode.checkSubPartitionTableHasBeCreated(table));
		}


		@Test
		void testSubPartitionTable2() {
			TapTable table = new TapTable();
			table.setId("test_1");
			table.setPartitionMasterTableId("test");
			table.setPartitionInfo(new TapPartition());

			HazelcastSourcePdkBaseNode spySourceBaseNode = spy(sourceBaseNode);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapConnectorContext connectorContext = mock(TapConnectorContext.class);

			when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
			when(spySourceBaseNode.getConnectorNode()).thenReturn(connectorNode);

			KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
				@Override
				public TapTable get(String key) {
					TapTable tapTable = new TapTable();
					tapTable.setPartitionInfo(new TapPartition());
					List<TapSubPartitionTableInfo> subPartitionTableList = new ArrayList<>();

					TapSubPartitionTableInfo subPartitionTable = new TapSubPartitionTableInfo();
					subPartitionTable.setTableName("test_1");
					subPartitionTableList.add(subPartitionTable);

					tapTable.getPartitionInfo().setSubPartitionTableInfo(subPartitionTableList);
					return tapTable;
				}
			};

			when(connectorContext.getTableMap()).thenReturn(tableMap);
			Assertions.assertTrue(spySourceBaseNode.checkSubPartitionTableHasBeCreated(table));
		}
	}

	@Nested
	class testMergeSubInfoIntoMasterTableIfNeed {

		private HazelcastSourcePdkBaseNode sourceBaseNode;
		private DataProcessorContext context;
		private TaskDto taskDto;

		@BeforeEach
		public void beforeEach() {
			context = mock(DataProcessorContext.class);
			taskDto = new TaskDto();
			taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			when(context.getTaskDto()).thenReturn(taskDto);

			Node node = new DatabaseNode();
			((DatabaseNode)node).setSyncSourcePartitionTableEnable(true);
			when(context.getNode()).thenReturn(node);

			sourceBaseNode = new HazelcastSourcePdkBaseNode(context) {
				@Override
				void startSourceRunner() {

				}
			};
		}

		@Test
		void testMergeSubInfoIntoMasterTableIfNeed() {
			HazelcastSourcePdkBaseNode spySourceBaseNode = spy(sourceBaseNode);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapConnectorContext connectorContext = mock(TapConnectorContext.class);

			when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
			when(spySourceBaseNode.getConnectorNode()).thenReturn(connectorNode);

			Assertions.assertDoesNotThrow(() -> {
				TapTable table = new TapTable();
				table.setId("test");
				table.setPartitionInfo(new TapPartition());
				table.setPartitionMasterTableId("test");
				spySourceBaseNode.mergeSubInfoIntoMasterTableIfNeed(table);

				TapTable masterTable = new TapTable();
				masterTable.setId("test");
				masterTable.setName("test");
				masterTable.setPartitionMasterTableId("test");
				masterTable.setPartitionInfo(new TapPartition());
				KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
					@Override
					public TapTable get(String key) {
						return masterTable;
					}
				};
				when(connectorContext.getTableMap()).thenReturn(tableMap);

				table = new TapTable();
				table.setId("test_1");
				table.setPartitionInfo(new TapPartition());
				table.setPartitionMasterTableId("test");
				List<TapSubPartitionTableInfo> subPartitionList = new ArrayList<>();
				subPartitionList.add(new TapSubPartitionTableInfo());
				subPartitionList.get(0).setTableName("test_1");
				table.getPartitionInfo().setSubPartitionTableInfo(subPartitionList);
				spySourceBaseNode.mergeSubInfoIntoMasterTableIfNeed(table);

				Assertions.assertNotNull(masterTable.getPartitionInfo().getSubPartitionTableInfo());
				Assertions.assertEquals(1, masterTable.getPartitionInfo().getSubPartitionTableInfo().size());
				Assertions.assertEquals("test_1", masterTable.getPartitionInfo().getSubPartitionTableInfo().get(0).getTableName());

			});
		}
	}

	@Test
	void testSetPartitionMasterTableId() {
		DataProcessorContext context = mock(DataProcessorContext.class);
		taskDto = new TaskDto();
		taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		when(context.getTaskDto()).thenReturn(taskDto);

		Node node = new DatabaseNode();
		((DatabaseNode)node).setSyncSourcePartitionTableEnable(true);
		when(context.getNode()).thenReturn(node);

		HazelcastSourcePdkBaseNode sourceBaseNode = new HazelcastSourcePdkBaseNode(context) {
			@Override
			void startSourceRunner() {

			}
		};
		sourceBaseNode.syncSourcePartitionTableEnable = Boolean.TRUE;

		TapTable table = new TapTable();
		table.setId("test_1");
		table.setPartitionMasterTableId("test");
		table.setPartitionInfo(new TapPartition());
		List<TapEvent> events = Stream.generate(() -> {
			TapEvent event = new TapInsertRecordEvent();
			((TapInsertRecordEvent)event).setTableId("test_1");
			return event;
		}).limit(5).collect(Collectors.toList());

		sourceBaseNode.setPartitionMasterTableId(table, events);

		events.forEach(e -> {
			Assertions.assertEquals("test_1", ((TapInsertRecordEvent)e).getPartitionMasterTableId());
			Assertions.assertEquals("test", ((TapInsertRecordEvent)e).getTableId());
		});

		TapTableMap<String, TapTable> tableMap = TapTableMap.create("nodeId");
		tableMap.putNew("test_2", new TapTable(), "test_2");
		tableMap.putNew("test_3", new TapTable(), "test_3");
		tableMap.putNew("test_4", new TapTable(), "test_4");
		tableMap.putNew("test_5", new TapTable(), "test_5");
		when(context.getTapTableMap()).thenReturn(tableMap);
		AtomicInteger counter = new AtomicInteger(0);
		events = Stream.generate(() -> {
			TapEvent event = new TapInsertRecordEvent();
			if (counter.incrementAndGet() < 2) {
				((TapInsertRecordEvent)event).setTableId("test");
			} else {
				((TapInsertRecordEvent)event).setTableId("test_" + counter.get());
			}

			return event;
		}).limit(5).collect(Collectors.toList());
		sourceBaseNode.syncSourcePartitionTableEnable = Boolean.TRUE;
		table = new TapTable();
		table.setId("test");
		table.setName("test");
		sourceBaseNode.partitionTableSubMasterMap.put("test", table);
		table = new TapTable();
		table.setId("test_1");
		table.setName("test_1");
		sourceBaseNode.partitionTableSubMasterMap.put("test_1", table);

		sourceBaseNode.setPartitionMasterTableId(events);
		Optional<TapEvent> optional = events.stream().filter(TapInsertRecordEvent.class::isInstance)
				.filter(e -> "test".equals(((TapInsertRecordEvent) e).getTableId())).findFirst();
		Assertions.assertTrue(optional.isPresent());
		Assertions.assertEquals("test", ((TapInsertRecordEvent)optional.get()).getPartitionMasterTableId());

		TapInsertRecordEvent event = new TapInsertRecordEvent();
		event.setTableId("test");
		sourceBaseNode.setPartitionMasterTableId(event, "test");
		Assertions.assertEquals("test", event.getPartitionMasterTableId());
	}

	@Test
	public void testInitTapCodecsFilterManager() {
		DataProcessorContext context = mock(DataProcessorContext.class);
		taskDto = new TaskDto();
		taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		when(context.getTaskDto()).thenReturn(taskDto);

		Node node = new DatabaseNode();
		((DatabaseNode)node).setSyncSourcePartitionTableEnable(true);
		when(context.getNode()).thenReturn(node);

		HazelcastSourcePdkBaseNode sourceBaseNode = new HazelcastSourcePdkBaseNode(context) {
			@Override
			void startSourceRunner() {

			}
		};

		HazelcastSourcePdkBaseNode spySourceBaseNode = spy(sourceBaseNode);

		ConnectorNode connectorNode = mock(ConnectorNode.class);
		TapConnectorContext connectorContext = mock(TapConnectorContext.class);
		TapNodeSpecification spec = mock(TapNodeSpecification.class);

		List<String> tags = new ArrayList<>();
		tags.add("schema-free");

		when(spec.getTags()).thenReturn(tags);
		when(connectorContext.getSpecification()).thenReturn(spec);
		when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
		when(connectorNode.getCodecsFilterManager()).thenReturn(null);
		when(connectorNode.getCodecsFilterManagerSchemaEnforced()).thenReturn(null);
		when(spySourceBaseNode.getConnectorNode()).thenReturn(connectorNode);

		ReflectionTestUtils.invokeMethod(spySourceBaseNode, "initTapCodecsFilterManager");
	}

	@Test
	void testInitTapEventFilter() {
		DataProcessorContext context = mock(DataProcessorContext.class);
		taskDto = new TaskDto();
		taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		taskDto.setNeedFilterEventData(Boolean.TRUE);
		when(context.getTaskDto()).thenReturn(taskDto);

		Node node = new DatabaseNode();
		((DatabaseNode)node).setSyncSourcePartitionTableEnable(true);

		Graph<Element, Element> graph = new Graph<>();
		node.setGraph(graph);
		graph.setNode(node.getId(), node);

		when(context.getNode()).thenReturn(node);


		HazelcastSourcePdkBaseNode sourceBaseNode = new HazelcastSourcePdkBaseNode(context) {
			@Override
			void startSourceRunner() {

			}
		};

		ObsLogger obsLogger = mock(ObsLogger.class);
		ReflectionTestUtils.setField(sourceBaseNode, "obsLogger", obsLogger);

		Assertions.assertThrows(CoreException.class, () -> {
			when(context.getTapTableMap()).thenReturn(null);
			ReflectionTestUtils.invokeMethod(sourceBaseNode, "initTapEventFilter");
		});

		TapTableMap<String, TapTable> tableMap = TapTableMap.create("nodeId");
		tableMap.putNew("test", new TapTable(), "test");
		when(context.getTapTableMap()).thenReturn(tableMap);

		TargetTableDataEventFilter tapEventFilter = mock(TargetTableDataEventFilter.class);
		doAnswer(answer -> {
			TargetTableDataEventFilter.TapEventHandel handler = answer.getArgument(0);

			TapdataEvent event = new TapdataEvent();
			TapInsertRecordEvent recordEvent = new TapInsertRecordEvent();
			recordEvent.setTableId("test");
			event.setTapEvent(recordEvent);
			handler.handler(event);

			return null;
		}).when(tapEventFilter).addHandler(any());
		ReflectionTestUtils.setField(sourceBaseNode, "tapEventFilter", tapEventFilter);

		Assertions.assertDoesNotThrow(() -> {
			ReflectionTestUtils.invokeMethod(sourceBaseNode, "initTapEventFilter");
		});

	}

	@Test
	void testNeedDynamicPartitionTable() {
		DataProcessorContext context = mock(DataProcessorContext.class);
		taskDto = new TaskDto();
		taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		taskDto.setNeedFilterEventData(Boolean.TRUE);
		when(context.getTaskDto()).thenReturn(taskDto);

		Node node = new TableRenameProcessNode();
		when(context.getNode()).thenReturn(node);

		HazelcastSourcePdkBaseNode sourceBaseNode = new HazelcastSourcePdkBaseNode(context) {
			@Override
			void startSourceRunner() {

			}
		};

		Assertions.assertFalse(sourceBaseNode.needDynamicPartitionTable());

		node = new DatabaseNode();
		when(context.getNode()).thenReturn(node);
		Assertions.assertFalse(sourceBaseNode.needDynamicPartitionTable());

		HazelcastSourcePdkBaseNode spySourceBaseNode = spy(sourceBaseNode);
		ConnectorNode connectorNode = mock(ConnectorNode.class);
		TapConnectorContext connectorContext = mock(TapConnectorContext.class);
		KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
			@Override
			public TapTable get(String key) {
				return null;
			}

			@Override
			public Iterator<Entry<TapTable>> iterator() {

				Iterator<Entry<TapTable>> iterator = new Iterator<Entry<TapTable>>() {
					private int counter = 0;
					@Override
					public boolean hasNext() {
						counter++;
						return counter < 2;
					}

					@Override
					public Entry<TapTable> next() {
						TapTable table;
						if (counter == 1) {
							table = new TapTable();
							table.setId("test");
							table.setName("test");
						} else {
							table = new TapTable();
							table.setId("test");
							table.setName("test");
							table.setPartitionInfo(new TapPartition());
							table.setPartitionMasterTableId("test");
						}
						return new Entry<TapTable>() {
							@Override
							public String getKey() {
								return "test";
							}

							@Override
							public TapTable getValue() {
								return table;
							}
						};
					}
				};
				return iterator;
			}
		};
		when(connectorContext.getTableMap()).thenReturn(tableMap);
		when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
		when(spySourceBaseNode.getConnectorNode()).thenReturn(connectorNode);

		ReflectionTestUtils.setField(sourceBaseNode, "syncType", SyncTypeEnum.INITIAL_SYNC_CDC);
		sourceBaseNode.syncSourcePartitionTableEnable = Boolean.TRUE;

		Assertions.assertFalse(spySourceBaseNode.needDynamicPartitionTable());
	}

	@Test
	void testNeedDynamicPartitionTable_1() {
		DataProcessorContext context = mock(DataProcessorContext.class);
		taskDto = new TaskDto();
		taskDto.setType(SyncTypeEnum.INITIAL_SYNC_CDC.getSyncType());
		taskDto.setNeedFilterEventData(Boolean.TRUE);
		when(context.getTaskDto()).thenReturn(taskDto);

		Node node = new TableRenameProcessNode();
		when(context.getNode()).thenReturn(node);

		HazelcastSourcePdkBaseNode sourceBaseNode = new HazelcastSourcePdkBaseNode(context) {
			@Override
			void startSourceRunner() {

			}
		};

		Assertions.assertFalse(sourceBaseNode.needDynamicPartitionTable());

		node = new DatabaseNode();
		when(context.getNode()).thenReturn(node);
		Assertions.assertFalse(sourceBaseNode.needDynamicPartitionTable());

		sourceBaseNode.syncSourcePartitionTableEnable = Boolean.TRUE;

		HazelcastSourcePdkBaseNode spySourceBaseNode = spy(sourceBaseNode);
		ConnectorNode connectorNode = mock(ConnectorNode.class);
		ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
		QueryPartitionTablesByParentName fun = new QueryPartitionTablesByParentName() {
			@Override
			public void query(TapConnectorContext connectorContext, List<TapTable> table, Consumer<Collection<TapPartitionResult>> consumer) throws Exception {

			}
		};
		when(connectorFunctions.getQueryPartitionTablesByParentName()).thenReturn(fun);
		when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
		TapConnectorContext connectorContext = mock(TapConnectorContext.class);
		KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
			@Override
			public TapTable get(String key) {
				return null;
			}

			@Override
			public Iterator<Entry<TapTable>> iterator() {
				return new Iterator<Entry<TapTable>>() {
					@Override
					public boolean hasNext() {
						return false;
					}

					@Override
					public Entry<TapTable> next() {
						return null;
					}
				};
			}
		};
		when(connectorContext.getTableMap()).thenReturn(tableMap);
		when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
		when(spySourceBaseNode.getConnectorNode()).thenReturn(connectorNode);

		Assertions.assertFalse(spySourceBaseNode.needDynamicPartitionTable());
	}
	@Test
	void testNeedDynamicPartitionTable_2() {
		DataProcessorContext context = mock(DataProcessorContext.class);
		taskDto = new TaskDto();
		taskDto.setType(SyncTypeEnum.INITIAL_SYNC_CDC.getSyncType());
		taskDto.setNeedFilterEventData(Boolean.TRUE);
		when(context.getTaskDto()).thenReturn(taskDto);

		Node node = new DatabaseNode();
		when(context.getNode()).thenReturn(node);

		HazelcastSourcePdkBaseNode sourceBaseNode = new HazelcastSourcePdkBaseNode(context) {
			@Override
			void startSourceRunner() {

			}
		};

		sourceBaseNode.syncSourcePartitionTableEnable = Boolean.TRUE;

		HazelcastSourcePdkBaseNode spySourceBaseNode = spy(sourceBaseNode);
		ConnectorNode connectorNode = mock(ConnectorNode.class);
		ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
		QueryPartitionTablesByParentName fun = new QueryPartitionTablesByParentName() {
			@Override
			public void query(TapConnectorContext connectorContext, List<TapTable> table, Consumer<Collection<TapPartitionResult>> consumer) throws Exception {

			}
		};
		when(connectorFunctions.getQueryPartitionTablesByParentName()).thenReturn(fun);
		when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
		TapConnectorContext connectorContext = mock(TapConnectorContext.class);
		KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
			@Override
			public TapTable get(String key) {
				return null;
			}

			@Override
			public Iterator<Entry<TapTable>> iterator() {
				return new Iterator<Entry<TapTable>>() {
					private int counter = -1;
					@Override
					public boolean hasNext() {
						counter++;
						return counter < 1;
					}

					@Override
					public Entry<TapTable> next() {
						TapTable table = new TapTable();
						table.setPartitionMasterTableId("test");
						table.setId("test");
						table.setName("test");
						table.setPartitionInfo(new TapPartition());
						return new Entry<TapTable>() {
							@Override
							public String getKey() {
								return table.getId();
							}

							@Override
							public TapTable getValue() {
								return table;
							}
						};
					}
				};
			}
		};
		when(connectorContext.getTableMap()).thenReturn(tableMap);
		when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
		when(spySourceBaseNode.getConnectorNode()).thenReturn(connectorNode);

		Assertions.assertTrue(spySourceBaseNode.needDynamicPartitionTable());
	}

	@Test
	void testContainsMasterPartitionTable() {
		DataProcessorContext context = mock(DataProcessorContext.class);
		taskDto = new TaskDto();
		taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		taskDto.setNeedFilterEventData(Boolean.TRUE);
		when(context.getTaskDto()).thenReturn(taskDto);

		Node node = new DatabaseNode();
		when(context.getNode()).thenReturn(node);

		HazelcastSourcePdkBaseNode sourcePdkBaseNode = new HazelcastSourcePdkBaseNode(context) {
			@Override
			void startSourceRunner() {

			}
		};

		HazelcastSourcePdkBaseNode spySourcePdkBaseNode = spy(sourcePdkBaseNode);

		List<TapTable> tables = new ArrayList<>();

		ConnectorNode connectorNode = mock(ConnectorNode.class);
		TapConnectorContext ctx = mock(TapConnectorContext.class);
		KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
			@Override
			public TapTable get(String key) {
				return null;
			}

			@Override
			public Iterator<Entry<TapTable>> iterator() {
				AtomicInteger counter = new AtomicInteger(-1);
				return new Iterator<Entry<TapTable>>() {
					@Override
					public boolean hasNext() {
						return tables.size() > counter.incrementAndGet();
					}

					@Override
					public Entry<TapTable> next() {
						TapTable table = tables.get(counter.get());
						return new Entry<TapTable>() {
							@Override
							public String getKey() {
								return table.getId();
							}

							@Override
							public TapTable getValue() {
								return table;
							}
						};
					}
				};
			}
		};
		when(ctx.getTableMap()).thenReturn(tableMap);
		when(connectorNode.getConnectorContext()).thenReturn(ctx);
        when(spySourcePdkBaseNode.getConnectorNode()).thenReturn(connectorNode);

		boolean result = spySourcePdkBaseNode.containsMasterPartitionTable();
		Assertions.assertFalse(result);

		TapTable tapTable = new TapTable();
		tapTable.setId("test");
		tapTable.setPartitionMasterTableId("test");
		tapTable.setPartitionInfo(new TapPartition());
		tapTable.setName("test");
		tables.add(tapTable);

		result = spySourcePdkBaseNode.containsMasterPartitionTable();
		Assertions.assertTrue(result);
	}

	@Nested
	class testCheckDDLFilterPredicate {

		private HazelcastSourcePdkBaseNode sourcePdkBaseNode;
		private DataProcessorContext context;

		@BeforeEach
		void beforeEach() {
			context = mock(DataProcessorContext.class);
			taskDto = new TaskDto();
			taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			taskDto.setNeedFilterEventData(Boolean.TRUE);
			when(context.getTaskDto()).thenReturn(taskDto);

			sourcePdkBaseNode = new HazelcastSourcePdkBaseNode(context) {
				@Override
				void startSourceRunner() {

				}
			};

		}
		@Test
		void testCreateParititonTable() {


			sourcePdkBaseNode.syncSourcePartitionTableEnable = Boolean.TRUE;

			TapCreateTableEvent event = new TapCreateTableEvent();
			TapTable table = new TapTable();
			table.setId("test_1");
			table.setName("test_1");
			table.setPartitionInfo(new TapPartition());
			table.setPartitionMasterTableId("test");
			event.setTable(table);
			boolean result = sourcePdkBaseNode.checkDDLFilterPredicate(event);
			Assertions.assertTrue(result);

			Node node = new TableRenameProcessNode();
			when(context.getNode()).thenReturn(node);

			sourcePdkBaseNode.syncSourcePartitionTableEnable = Boolean.FALSE;
			result = sourcePdkBaseNode.checkDDLFilterPredicate(event);
			Assertions.assertTrue(result);
		}

		@Test
		void testCreatePartitionTableInDatabaseNode() {


			sourcePdkBaseNode.syncSourcePartitionTableEnable = Boolean.TRUE;

			TapCreateTableEvent event = new TapCreateTableEvent();
			TapTable table = new TapTable();
			table.setId("test_1");
			table.setName("test_1");
			table.setPartitionInfo(new TapPartition());
			table.setPartitionMasterTableId("test");
			event.setTable(table);
			event.setTableId(table.getId());

			DatabaseNode node = new DatabaseNode();
			when(context.getNode()).thenReturn((Node)node);

			sourcePdkBaseNode.syncSourcePartitionTableEnable = Boolean.FALSE;
			boolean result = sourcePdkBaseNode.checkDDLFilterPredicate(event);
			Assertions.assertTrue(result);

			node.setTableExpression("*");
			result = sourcePdkBaseNode.checkDDLFilterPredicate(event);
			Assertions.assertTrue(result);
		}
	}

	@Nested
	class testHandleNewTables {
		private DataProcessorContext context;
		private HazelcastSourcePdkBaseNode sourcePdkBaseNode;

		@BeforeEach
		void beforeEach() {
			context = mock(DataProcessorContext.class);
			taskDto = new TaskDto();
			taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			taskDto.setNeedFilterEventData(Boolean.TRUE);
			when(context.getTaskDto()).thenReturn(taskDto);

			sourcePdkBaseNode = new HazelcastSourcePdkBaseNode(context) {
				@Override
				void startSourceRunner() {

				}
			};

			ObsLogger obsLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(sourcePdkBaseNode, "obsLogger", obsLogger);
		}

		@Test
		void testEmptyNewTable() {
			List<String> tables = Collections.emptyList();
			boolean result = sourcePdkBaseNode.handleNewTables(tables);
			Assertions.assertFalse(result);
		}

		@Test
		void testNewPartitionTable() throws Throwable {
			List<TapTable> tapTables = new ArrayList<>();
			HazelcastSourcePdkBaseNode spySourcePdkBaseNod = spy(sourcePdkBaseNode);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapConnectorContext connectorContext = mock(TapConnectorContext.class);
			TapNodeSpecification spec = new TapNodeSpecification();
			spec.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
			when(connectorContext.getSpecification()).thenReturn(spec);
			KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
				@Override
				public TapTable get(String key) {
					return tapTables.stream().filter(t -> t.getId().equals(key)).findFirst().orElse(null);
				}
			};
			when(connectorContext.getTableMap()).thenReturn(tableMap);
			when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
			doAnswer(answer -> {
				Runnable runnable = answer.getArgument(0);
				runnable.run();
				return null;
			}).when(connectorNode).applyClassLoaderContext(any());
			TapConnector connector = mock(TapConnector.class);
			doAnswer(answer -> {
				Consumer<List<TapTable>> consumer = answer.getArgument(3);

				TapTable table = new TapTable();
				table.setId("test");
				table.setName("test");
				table.setPartitionMasterTableId("test");
				table.setPartitionInfo(new TapPartition());
				table.setNameFieldMap(new LinkedHashMap<>());
				table.getNameFieldMap().put("id", new TapField("id", "integer"));
				table.getNameFieldMap().put("name", new TapField("id", "string"));
				tapTables.add(table);
				table = new TapTable();
				table.setId("test_1");
				table.setName("test_1");
				table.setPartitionMasterTableId("test");
				table.setPartitionInfo(new TapPartition());
				table.setNameFieldMap(new LinkedHashMap<>());
				table.getNameFieldMap().put("id", new TapField("id", "integer"));
				table.getNameFieldMap().put("name", new TapField("id", "string"));
				tapTables.add(table);
				consumer.accept(tapTables);
				return null;
			}).
			when(connector).discoverSchema(any(), anyList(), anyInt(), any());
			when(connectorNode.getConnector()).thenReturn(connector);
			when(spySourcePdkBaseNod.getConnectorNode()).thenReturn(connectorNode);

			when(spySourcePdkBaseNod.wrapTapdataEvent(any(TapEvent.class), any(SyncStage.class), any(), anyBoolean())).thenAnswer(answer -> {
				TapEvent tapEvent = answer.getArgument(0);
				TapdataEvent event = new TapdataEvent();
				event.setTapEvent(tapEvent);
				return event;
			});
			doNothing().when(spySourcePdkBaseNod).enqueue(any(TapdataEvent.class));
			JetJobStatusMonitor jetJobStatusMonitor = mock(JetJobStatusMonitor.class);
			when(jetJobStatusMonitor.get()).thenReturn(JobStatus.RUNNING);
			ReflectionTestUtils.setField(sourcePdkBaseNode, "jetJobStatusMonitor", jetJobStatusMonitor);

			List<String> tables = new ArrayList<>();
			tables.add("test");

			spySourcePdkBaseNod.running.set(true);
			spySourcePdkBaseNod.syncSourcePartitionTableEnable = false;
			spySourcePdkBaseNod.syncProgress = new SyncProgress();
			spySourcePdkBaseNod.syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
			spySourcePdkBaseNod.newTables = new CopyOnWriteArrayList<>();
			spySourcePdkBaseNod.endSnapshotLoop = new AtomicBoolean(false);
			ReflectionTestUtils.setField(spySourcePdkBaseNod.noPrimaryKeyVirtualField, "addTable", (Consumer<TapTable>) tapTable -> {});
			spySourcePdkBaseNod.handleNewTables(tables);

			Assertions.assertNotNull(spySourcePdkBaseNod.newTables);
			Assertions.assertEquals(1, spySourcePdkBaseNod.newTables.size());
			Assertions.assertEquals("test_1", spySourcePdkBaseNod.newTables.get(0));

		}
		@Test
		void testLoggedTables() throws Throwable {
			List<TapTable> tapTables = new ArrayList<>();
			HazelcastSourcePdkBaseNode spySourcePdkBaseNod = spy(sourcePdkBaseNode);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapConnectorContext connectorContext = mock(TapConnectorContext.class);
			TapNodeSpecification spec = new TapNodeSpecification();
			spec.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
			when(connectorContext.getSpecification()).thenReturn(spec);
			KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
				@Override
				public TapTable get(String key) {
					return tapTables.stream().filter(t -> t.getId().equals(key)).findFirst().orElse(null);
				}
			};
			when(connectorContext.getTableMap()).thenReturn(tableMap);
			when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
			doAnswer(answer -> {
				Runnable runnable = answer.getArgument(0);
				runnable.run();
				return null;
			}).when(connectorNode).applyClassLoaderContext(any());
			TapConnector connector = mock(TapConnector.class);
			doAnswer(answer -> {
				Consumer<List<TapTable>> consumer = answer.getArgument(3);

				TapTable table = new TapTable();
				table.setId("test");
				table.setName("test");
				table.setPartitionMasterTableId("test");
				table.setPartitionInfo(new TapPartition());
				table.setNameFieldMap(new LinkedHashMap<>());
				table.getNameFieldMap().put("id", new TapField("id", "integer"));
				table.getNameFieldMap().put("name", new TapField("id", "string"));
				tapTables.add(table);
				table = new TapTable();
				table.setId("test_1");
				table.setName("test_1");
				table.setPartitionMasterTableId("test");
				table.setPartitionInfo(new TapPartition());
				table.setNameFieldMap(new LinkedHashMap<>());
				table.getNameFieldMap().put("id", new TapField("id", "integer"));
				table.getNameFieldMap().put("name", new TapField("id", "string"));
				tapTables.add(table);
				consumer.accept(tapTables);
				return null;
			}).
					when(connector).discoverSchema(any(), anyList(), anyInt(), any());
			when(connectorNode.getConnector()).thenReturn(connector);
			when(spySourcePdkBaseNod.getConnectorNode()).thenReturn(connectorNode);

			when(spySourcePdkBaseNod.wrapTapdataEvent(any(TapEvent.class), any(SyncStage.class), any(), anyBoolean())).thenAnswer(answer -> {
				TapEvent tapEvent = answer.getArgument(0);
				TapdataEvent event = new TapdataEvent();
				event.setTapEvent(tapEvent);
				return event;
			});
			doNothing().when(spySourcePdkBaseNod).enqueue(any(TapdataEvent.class));
			JetJobStatusMonitor jetJobStatusMonitor = mock(JetJobStatusMonitor.class);
			when(jetJobStatusMonitor.get()).thenReturn(JobStatus.RUNNING);
			ReflectionTestUtils.setField(sourcePdkBaseNode, "jetJobStatusMonitor", jetJobStatusMonitor);

			List<String> tables = new ArrayList<>();
			tables.add("test");

			spySourcePdkBaseNod.running.set(true);
			spySourcePdkBaseNod.syncSourcePartitionTableEnable = false;
			spySourcePdkBaseNod.syncProgress = new SyncProgress();
			spySourcePdkBaseNod.syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
			spySourcePdkBaseNod.newTables = new CopyOnWriteArrayList<>();
			spySourcePdkBaseNod.endSnapshotLoop = new AtomicBoolean(false);
			ReflectionTestUtils.setField(spySourcePdkBaseNod.noPrimaryKeyVirtualField, "addTable", (Consumer<TapTable>) tapTable -> {});
			List<String> loggedTables = tables;
			ReflectionTestUtils.setField(spySourcePdkBaseNod, "loggedTables", loggedTables);
			spySourcePdkBaseNod.handleNewTables(tables);

			Assertions.assertNotNull(spySourcePdkBaseNod.newTables);
			Assertions.assertEquals(1, spySourcePdkBaseNod.newTables.size());
			Assertions.assertEquals("test_1", spySourcePdkBaseNod.newTables.get(0));

		}

		@Test
		void testFilterTableByNoPrimaryKeyWithHasKeys() throws Throwable {
			List<TapTable> tapTables = new ArrayList<>();
			HazelcastSourcePdkBaseNode spySourcePdkBaseNod = spy(sourcePdkBaseNode);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapConnectorContext connectorContext = mock(TapConnectorContext.class);
			TapNodeSpecification spec = new TapNodeSpecification();
			spec.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
			when(connectorContext.getSpecification()).thenReturn(spec);
			KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
				@Override
				public TapTable get(String key) {
					return tapTables.stream().filter(t -> t.getId().equals(key)).findFirst().orElse(null);
				}
			};
			when(connectorContext.getTableMap()).thenReturn(tableMap);
			when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
			doAnswer(answer -> {
				Runnable runnable = answer.getArgument(0);
				runnable.run();
				return null;
			}).when(connectorNode).applyClassLoaderContext(any());
			TapConnector connector = mock(TapConnector.class);
			doAnswer(answer -> {
				Consumer<List<TapTable>> consumer = answer.getArgument(3);

				TapTable table = new TapTable();
				table.setId("test");
				table.setName("test");
				table.setPartitionMasterTableId("test");
				table.setPartitionInfo(new TapPartition());
				table.setNameFieldMap(new LinkedHashMap<>());
				table.getNameFieldMap().put("id", new TapField("id", "integer"));
				table.getNameFieldMap().put("name", new TapField("id", "string"));
				tapTables.add(table);
				table = new TapTable();
				table.setId("test_1");
				table.setName("test_1");
				table.setPartitionMasterTableId("test");
				table.setPartitionInfo(new TapPartition());
				table.setNameFieldMap(new LinkedHashMap<>());
				table.getNameFieldMap().put("id", new TapField("id", "integer"));
				table.getNameFieldMap().put("name", new TapField("id", "string"));
				TapIndex index = new TapIndex();
				index.setUnique(true);
				table.setIndexList(Arrays.asList(index));
				tapTables.add(table);
				consumer.accept(tapTables);
				return null;
			}).
					when(connector).discoverSchema(any(), anyList(), anyInt(), any());
			when(connectorNode.getConnector()).thenReturn(connector);
			when(spySourcePdkBaseNod.getConnectorNode()).thenReturn(connectorNode);
			DatabaseNode node = new DatabaseNode();
			node.setMigrateTableSelectType("expression");
			node.setNoPrimaryKeyTableSelectType("HasKeys");
			when(spySourcePdkBaseNod.getNode()).thenReturn((Node)node);

			when(spySourcePdkBaseNod.wrapTapdataEvent(any(TapEvent.class), any(SyncStage.class), any(), anyBoolean())).thenAnswer(answer -> {
				TapEvent tapEvent = answer.getArgument(0);
				TapdataEvent event = new TapdataEvent();
				event.setTapEvent(tapEvent);
				return event;
			});
			doNothing().when(spySourcePdkBaseNod).enqueue(any(TapdataEvent.class));
			JetJobStatusMonitor jetJobStatusMonitor = mock(JetJobStatusMonitor.class);
			when(jetJobStatusMonitor.get()).thenReturn(JobStatus.RUNNING);
			ReflectionTestUtils.setField(sourcePdkBaseNode, "jetJobStatusMonitor", jetJobStatusMonitor);

			List<String> tables = new ArrayList<>();
			tables.add("test");

			spySourcePdkBaseNod.running.set(true);
			spySourcePdkBaseNod.syncSourcePartitionTableEnable = false;
			spySourcePdkBaseNod.syncProgress = new SyncProgress();
			spySourcePdkBaseNod.syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
			spySourcePdkBaseNod.newTables = new CopyOnWriteArrayList<>();
			spySourcePdkBaseNod.endSnapshotLoop = new AtomicBoolean(false);
			ReflectionTestUtils.setField(spySourcePdkBaseNod.noPrimaryKeyVirtualField, "addTable", (Consumer<TapTable>) tapTable -> {});
			spySourcePdkBaseNod.handleNewTables(tables);

			Assertions.assertNotNull(spySourcePdkBaseNod.newTables);
			Assertions.assertEquals(1, spySourcePdkBaseNod.newTables.size());
			Assertions.assertEquals("test_1", spySourcePdkBaseNod.newTables.get(0));

		}

		@Test
		void testFilterTableByNoPrimaryKeyWithNoKeys() throws Throwable {
			List<TapTable> tapTables = new ArrayList<>();
			HazelcastSourcePdkBaseNode spySourcePdkBaseNod = spy(sourcePdkBaseNode);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapConnectorContext connectorContext = mock(TapConnectorContext.class);
			TapNodeSpecification spec = new TapNodeSpecification();
			spec.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
			when(connectorContext.getSpecification()).thenReturn(spec);
			KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
				@Override
				public TapTable get(String key) {
					return tapTables.stream().filter(t -> t.getId().equals(key)).findFirst().orElse(null);
				}
			};
			when(connectorContext.getTableMap()).thenReturn(tableMap);
			when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
			doAnswer(answer -> {
				Runnable runnable = answer.getArgument(0);
				runnable.run();
				return null;
			}).when(connectorNode).applyClassLoaderContext(any());
			TapConnector connector = mock(TapConnector.class);
			doAnswer(answer -> {
				Consumer<List<TapTable>> consumer = answer.getArgument(3);

				TapTable table = new TapTable();
				table.setId("test");
				table.setName("test");
				table.setPartitionMasterTableId("test");
				table.setPartitionInfo(new TapPartition());
				table.setNameFieldMap(new LinkedHashMap<>());
				table.getNameFieldMap().put("id", new TapField("id", "integer"));
				table.getNameFieldMap().put("name", new TapField("id", "string"));
				TapIndex index = new TapIndex();
				index.setUnique(true);
				table.setIndexList(Arrays.asList(index));
				tapTables.add(table);
				table = new TapTable();
				table.setId("test_1");
				table.setName("test_1");
				table.setPartitionMasterTableId("test");
				table.setPartitionInfo(new TapPartition());
				table.setNameFieldMap(new LinkedHashMap<>());
				table.getNameFieldMap().put("id", new TapField("id", "integer"));
				table.getNameFieldMap().put("name", new TapField("id", "string"));
				tapTables.add(table);
				consumer.accept(tapTables);
				return null;
			}).
					when(connector).discoverSchema(any(), anyList(), anyInt(), any());
			when(connectorNode.getConnector()).thenReturn(connector);
			when(spySourcePdkBaseNod.getConnectorNode()).thenReturn(connectorNode);
			DatabaseNode node = new DatabaseNode();
			node.setMigrateTableSelectType("expression");
			node.setNoPrimaryKeyTableSelectType("HasKeys");
			when(spySourcePdkBaseNod.getNode()).thenReturn((Node)node);

			when(spySourcePdkBaseNod.wrapTapdataEvent(any(TapEvent.class), any(SyncStage.class), any(), anyBoolean())).thenAnswer(answer -> {
				TapEvent tapEvent = answer.getArgument(0);
				TapdataEvent event = new TapdataEvent();
				event.setTapEvent(tapEvent);
				return event;
			});
			doNothing().when(spySourcePdkBaseNod).enqueue(any(TapdataEvent.class));
			JetJobStatusMonitor jetJobStatusMonitor = mock(JetJobStatusMonitor.class);
			when(jetJobStatusMonitor.get()).thenReturn(JobStatus.RUNNING);
			ReflectionTestUtils.setField(sourcePdkBaseNode, "jetJobStatusMonitor", jetJobStatusMonitor);

			List<String> tables = new ArrayList<>();
			tables.add("test");

			spySourcePdkBaseNod.running.set(true);
			spySourcePdkBaseNod.syncSourcePartitionTableEnable = false;
			spySourcePdkBaseNod.syncProgress = new SyncProgress();
			spySourcePdkBaseNod.syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
			spySourcePdkBaseNod.newTables = new CopyOnWriteArrayList<>();
			spySourcePdkBaseNod.endSnapshotLoop = new AtomicBoolean(false);
			ReflectionTestUtils.setField(spySourcePdkBaseNod.noPrimaryKeyVirtualField, "addTable", (Consumer<TapTable>) tapTable -> {});
			spySourcePdkBaseNod.handleNewTables(tables);

			Assertions.assertNotNull(spySourcePdkBaseNod.newTables);
			Assertions.assertEquals(0, spySourcePdkBaseNod.newTables.size());
		}

		@Test
		void testFilterTableByNoPrimaryKeyWithOnlyPrimaryKey() throws Throwable {
			List<TapTable> tapTables = new ArrayList<>();
			HazelcastSourcePdkBaseNode spySourcePdkBaseNod = spy(sourcePdkBaseNode);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapConnectorContext connectorContext = mock(TapConnectorContext.class);
			TapNodeSpecification spec = new TapNodeSpecification();
			spec.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
			when(connectorContext.getSpecification()).thenReturn(spec);
			KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
				@Override
				public TapTable get(String key) {
					return tapTables.stream().filter(t -> t.getId().equals(key)).findFirst().orElse(null);
				}
			};
			when(connectorContext.getTableMap()).thenReturn(tableMap);
			when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
			doAnswer(answer -> {
				Runnable runnable = answer.getArgument(0);
				runnable.run();
				return null;
			}).when(connectorNode).applyClassLoaderContext(any());
			TapConnector connector = mock(TapConnector.class);
			doAnswer(answer -> {
				Consumer<List<TapTable>> consumer = answer.getArgument(3);

				TapTable table = new TapTable();
				table.setId("test");
				table.setName("test");
				table.setPartitionMasterTableId("test");
				table.setPartitionInfo(new TapPartition());
				table.setNameFieldMap(new LinkedHashMap<>());
				table.getNameFieldMap().put("id", new TapField("id", "integer"));
				table.getNameFieldMap().put("name", new TapField("id", "string"));
				TapIndex index = new TapIndex();
				index.setUnique(true);
				table.setIndexList(Arrays.asList(index));
				tapTables.add(table);
				table = new TapTable();
				table.setId("test_1");
				table.setName("test_1");
				table.setPartitionMasterTableId("test");
				table.setPartitionInfo(new TapPartition());
				table.setNameFieldMap(new LinkedHashMap<>());
				table.getNameFieldMap().put("id", new TapField("id", "integer"));
				table.getNameFieldMap().put("name", new TapField("id", "string"));
				table.setIndexList(Arrays.asList(index));
				tapTables.add(table);
				consumer.accept(tapTables);
				return null;
			}).
					when(connector).discoverSchema(any(), anyList(), anyInt(), any());
			when(connectorNode.getConnector()).thenReturn(connector);
			when(spySourcePdkBaseNod.getConnectorNode()).thenReturn(connectorNode);
			DatabaseNode node = new DatabaseNode();
			node.setMigrateTableSelectType("expression");
			node.setNoPrimaryKeyTableSelectType("OnlyPrimaryKey");
			when(spySourcePdkBaseNod.getNode()).thenReturn((Node)node);

			when(spySourcePdkBaseNod.wrapTapdataEvent(any(TapEvent.class), any(SyncStage.class), any(), anyBoolean())).thenAnswer(answer -> {
				TapEvent tapEvent = answer.getArgument(0);
				TapdataEvent event = new TapdataEvent();
				event.setTapEvent(tapEvent);
				return event;
			});
			doNothing().when(spySourcePdkBaseNod).enqueue(any(TapdataEvent.class));
			JetJobStatusMonitor jetJobStatusMonitor = mock(JetJobStatusMonitor.class);
			when(jetJobStatusMonitor.get()).thenReturn(JobStatus.RUNNING);
			ReflectionTestUtils.setField(sourcePdkBaseNode, "jetJobStatusMonitor", jetJobStatusMonitor);

			List<String> tables = new ArrayList<>();
			tables.add("test");

			spySourcePdkBaseNod.running.set(true);
			spySourcePdkBaseNod.syncSourcePartitionTableEnable = false;
			spySourcePdkBaseNod.syncProgress = new SyncProgress();
			spySourcePdkBaseNod.syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
			spySourcePdkBaseNod.newTables = new CopyOnWriteArrayList<>();
			spySourcePdkBaseNod.endSnapshotLoop = new AtomicBoolean(false);
			ReflectionTestUtils.setField(spySourcePdkBaseNod.noPrimaryKeyVirtualField, "addTable", (Consumer<TapTable>) tapTable -> {});
			spySourcePdkBaseNod.handleNewTables(tables);

			Assertions.assertNotNull(spySourcePdkBaseNod.newTables);
			Assertions.assertEquals(0, spySourcePdkBaseNod.newTables.size());
		}

		@Test
		void testFilterTableByNoPrimaryKeyWithOnlyUniqueIndex() throws Throwable {
			List<TapTable> tapTables = new ArrayList<>();
			HazelcastSourcePdkBaseNode spySourcePdkBaseNod = spy(sourcePdkBaseNode);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapConnectorContext connectorContext = mock(TapConnectorContext.class);
			TapNodeSpecification spec = new TapNodeSpecification();
			spec.setDataTypesMap(new DefaultExpressionMatchingMap(new HashMap<>()));
			when(connectorContext.getSpecification()).thenReturn(spec);
			KVReadOnlyMap<TapTable> tableMap = new KVReadOnlyMap<TapTable>() {
				@Override
				public TapTable get(String key) {
					return tapTables.stream().filter(t -> t.getId().equals(key)).findFirst().orElse(null);
				}
			};
			when(connectorContext.getTableMap()).thenReturn(tableMap);
			when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
			doAnswer(answer -> {
				Runnable runnable = answer.getArgument(0);
				runnable.run();
				return null;
			}).when(connectorNode).applyClassLoaderContext(any());
			TapConnector connector = mock(TapConnector.class);
			doAnswer(answer -> {
				Consumer<List<TapTable>> consumer = answer.getArgument(3);

				TapTable table = new TapTable();
				table.setId("test");
				table.setName("test");
				table.setPartitionMasterTableId("test");
				table.setPartitionInfo(new TapPartition());
				table.setNameFieldMap(new LinkedHashMap<>());
				table.getNameFieldMap().put("id", new TapField("id", "integer"));
				table.getNameFieldMap().put("name", new TapField("id", "string"));
				TapIndex index = new TapIndex();
				index.setUnique(true);
				table.setIndexList(Arrays.asList(index));
				tapTables.add(table);
				table = new TapTable();
				table.setId("test_1");
				table.setName("test_1");
				table.setPartitionMasterTableId("test");
				table.setPartitionInfo(new TapPartition());
				table.setNameFieldMap(new LinkedHashMap<>());
				table.getNameFieldMap().put("id", new TapField("id", "integer"));
				table.getNameFieldMap().put("name", new TapField("id", "string"));
				table.setIndexList(Arrays.asList(index));
				tapTables.add(table);
				consumer.accept(tapTables);
				return null;
			}).
					when(connector).discoverSchema(any(), anyList(), anyInt(), any());
			when(connectorNode.getConnector()).thenReturn(connector);
			when(spySourcePdkBaseNod.getConnectorNode()).thenReturn(connectorNode);
			DatabaseNode node = new DatabaseNode();
			node.setMigrateTableSelectType("expression");
			node.setNoPrimaryKeyTableSelectType("OnlyUniqueIndex");
			when(spySourcePdkBaseNod.getNode()).thenReturn((Node)node);

			when(spySourcePdkBaseNod.wrapTapdataEvent(any(TapEvent.class), any(SyncStage.class), any(), anyBoolean())).thenAnswer(answer -> {
				TapEvent tapEvent = answer.getArgument(0);
				TapdataEvent event = new TapdataEvent();
				event.setTapEvent(tapEvent);
				return event;
			});
			doNothing().when(spySourcePdkBaseNod).enqueue(any(TapdataEvent.class));
			JetJobStatusMonitor jetJobStatusMonitor = mock(JetJobStatusMonitor.class);
			when(jetJobStatusMonitor.get()).thenReturn(JobStatus.RUNNING);
			ReflectionTestUtils.setField(sourcePdkBaseNode, "jetJobStatusMonitor", jetJobStatusMonitor);

			List<String> tables = new ArrayList<>();
			tables.add("test");

			spySourcePdkBaseNod.running.set(true);
			spySourcePdkBaseNod.syncSourcePartitionTableEnable = false;
			spySourcePdkBaseNod.syncProgress = new SyncProgress();
			spySourcePdkBaseNod.syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
			spySourcePdkBaseNod.newTables = new CopyOnWriteArrayList<>();
			spySourcePdkBaseNod.endSnapshotLoop = new AtomicBoolean(false);
			ReflectionTestUtils.setField(spySourcePdkBaseNod.noPrimaryKeyVirtualField, "addTable", (Consumer<TapTable>) tapTable -> {});
			spySourcePdkBaseNod.handleNewTables(tables);

			Assertions.assertNotNull(spySourcePdkBaseNod.newTables);
			Assertions.assertEquals(1, spySourcePdkBaseNod.newTables.size());
			Assertions.assertEquals("test_1", spySourcePdkBaseNod.newTables.get(0));

		}
	}

	@Nested
	class testInitTableMonitor {

		private DataProcessorContext context;
		private HazelcastSourcePdkBaseNode sourcePdkBaseNode;
		private MonitorManager monitorManager;

		@BeforeEach
		void beforeEach() {
			context = mock(DataProcessorContext.class);
			taskDto = new TaskDto();
			taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			taskDto.setNeedFilterEventData(Boolean.TRUE);
			when(context.getTaskDto()).thenReturn(taskDto);

			DatabaseNode node = new DatabaseNode();
			node.setCatalog(Node.NodeCatalog.data);
			when(context.getNode()).thenReturn((Node)node);

			TapTableMap<String, TapTable> tableMap = TapTableMap.create("nodeId");
			when(context.getTapTableMap()).thenReturn(tableMap);

			sourcePdkBaseNode = new HazelcastSourcePdkBaseNode(context) {
				@Override
				void startSourceRunner() {

				}
			};

			ObsLogger obsLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(sourcePdkBaseNode, "obsLogger", obsLogger);
			ReflectionTestUtils.setField(sourcePdkBaseNode, "associateId", "nodeId");

			monitorManager = mock(MonitorManager.class);
			ReflectionTestUtils.setField(sourcePdkBaseNode, "monitorManager", monitorManager);

		}

		@Test
		void testInitTableMonitor() {

			HazelcastSourcePdkBaseNode spySourcePdkBaseNode = spy(sourcePdkBaseNode);
			doReturn(true).when(spySourcePdkBaseNode).needDynamicTable();

			ConnectorNode connectorNode = mock(ConnectorNode.class);
			doReturn("nodeId").when(connectorNode).getAssociateId();
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			doReturn(connectorFunctions).when(connectorNode).getConnectorFunctions();
			GetTableNamesFunction tableNamesFunction = mock(GetTableNamesFunction.class);
			doReturn(tableNamesFunction).when(connectorFunctions).getGetTableNamesFunction();
			ConnectorNodeService.getInstance().putConnectorNode(connectorNode);

			Assertions.assertDoesNotThrow(() -> {

				try(MockedStatic<ObsLoggerFactory> mockLogFactory = mockStatic(ObsLoggerFactory.class)) {

					ObsLoggerFactory logFactory = mock(ObsLoggerFactory.class);
					ObsLogger obsLogger = mock(ObsLogger.class);
					when(logFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
					mockLogFactory.when(ObsLoggerFactory::getInstance).thenReturn(logFactory);

					spySourcePdkBaseNode.initTableMonitor();
					verify(monitorManager, times(1)).startMonitor(any());

				}
			});
		}

		@Test
		void testInitPartitionTableMonitor() {

			HazelcastSourcePdkBaseNode spySourcePdkBaseNode = spy(sourcePdkBaseNode);
			doReturn(false).when(spySourcePdkBaseNode).needDynamicTable();
			doReturn(true).when(spySourcePdkBaseNode).needDynamicPartitionTable();

			ConnectorNode connectorNode = mock(ConnectorNode.class);
			doReturn("nodeId").when(connectorNode).getAssociateId();
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			doReturn(connectorFunctions).when(connectorNode).getConnectorFunctions();
			GetTableNamesFunction tableNamesFunction = mock(GetTableNamesFunction.class);
			doReturn(tableNamesFunction).when(connectorFunctions).getGetTableNamesFunction();
			ConnectorNodeService.getInstance().putConnectorNode(connectorNode);

			Assertions.assertDoesNotThrow(() -> {

				try(MockedStatic<ObsLoggerFactory> mockLogFactory = mockStatic(ObsLoggerFactory.class)) {

					ObsLoggerFactory logFactory = mock(ObsLoggerFactory.class);
					ObsLogger obsLogger = mock(ObsLogger.class);
					when(logFactory.getObsLogger(any(TaskDto.class))).thenReturn(obsLogger);
					mockLogFactory.when(ObsLoggerFactory::getInstance).thenReturn(logFactory);

					spySourcePdkBaseNode.initTableMonitor();
					verify(monitorManager, times(1)).startMonitor(any());

				}
			});
		}
	}



	@Nested
	class doTableNameSynchronouslyTest{
		private BatchCountFunction batchCountFunction;
		private List<String> tableList;
		@BeforeEach
		void setUp() {
			tableList = Arrays.asList("table1", "table2");
			TapTableMap map = TapTableMap.create("nodeId");
			map.putNew("table1", new TapTable("table1"), "qualifiedName1");
			map.putNew("table2", new TapTable("table2"), "qualifiedName2");
			when(dataProcessorContext.getTapTableMap()).thenReturn(map);
			batchCountFunction = mock(BatchCountFunction.class);
		}

		@Test
		void testDoTableNameSynchronously_IsRunningFalse() {
			when(mockInstance.isRunning()).thenReturn(false);
			doCallRealMethod().when(mockInstance).doTableNameSynchronously(batchCountFunction, tableList);
			mockInstance.doTableNameSynchronously(batchCountFunction, tableList);
			verify(mockInstance, never()).executeDataFuncAspect(any(), any(), any());
		}

		@Test
		void testDoTableNameSynchronously_NormalExecution() {
			when(mockInstance.isRunning()).thenReturn(true);
			try (MockedStatic<PDKInvocationMonitor> mb = Mockito
					.mockStatic(PDKInvocationMonitor.class)) {
				mb.when(()->PDKInvocationMonitor.invoke(any(),any(),any())).thenAnswer(invocationOnMock -> {
					return null;
				});
				doCallRealMethod().when(mockInstance).doTableNameSynchronously(batchCountFunction, tableList);
				mockInstance.doTableNameSynchronously(batchCountFunction, tableList);
				verify(mockInstance, times(1)).asyncCountTable(batchCountFunction, tableList);
			}
		}
	}

	@Nested
	class asyncCountTableTest{
		private BatchCountFunction batchCountFunction;
		protected ObsLogger obsLogger;
		private List<String> tableList;
		private TaskDto taskDto;
		private Node node;
		private ExecutorService snapshotRowSizeThreadPool;
		private Logger logger = mock(Logger.class);
		@BeforeEach
		void setUp() {
			batchCountFunction = mock(BatchCountFunction.class);
			tableList = Arrays.asList("table1", "table2");

			taskDto = mock(TaskDto.class);
			node = mock(Node.class);

			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
			when(dataProcessorContext.getNode()).thenReturn(node);
			when(taskDto.getName()).thenReturn("TestTask");
			when(taskDto.getId()).thenReturn(new ObjectId("507f1f77bcf86cd799439011"));
			when(node.getName()).thenReturn("TestNode");
			when(node.getId()).thenReturn("12345");
			snapshotRowSizeThreadPool = mock(ExecutorService.class);
			ReflectionTestUtils.setField(mockInstance, "snapshotRowSizeThreadPool", snapshotRowSizeThreadPool);
			obsLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(mockInstance, "obsLogger", obsLogger);
			logger = mock(Logger.class);
			ReflectionTestUtils.setField(mockInstance, "logger", logger);
			doCallRealMethod().when(mockInstance).asyncCountTable(batchCountFunction, tableList);

		}

		@Test
		void testAsyncCountTable_Success() throws InterruptedException {
			doNothing().when(mockInstance).doCountSynchronously(batchCountFunction, tableList, false);
			doNothing().when(snapshotRowSizeThreadPool).shutdown();

			mockInstance.asyncCountTable(batchCountFunction, tableList);

			verify(logger).info(contains("Start to asynchronously count the size of rows for the source table(s)"));

			Thread.sleep(100); // Wait for async execution

			verify(mockInstance).doCountSynchronously(batchCountFunction, tableList, false);
			verify(obsLogger).trace(contains("Query snapshot row size completed"));
		}

		@Test
		void testAsyncCountTable_ExceptionHandling() throws InterruptedException {
			doThrow(new RuntimeException("Count error")).when(mockInstance).doCountSynchronously(any(), any(), eq(false));
			mockInstance.asyncCountTable(batchCountFunction, tableList);
			Thread.sleep(100);
			verify(obsLogger).warn(contains("Query snapshot row size failed"));
		}
	}
	@Nested
	class doCountSynchronouslyTest{
		private BatchCountFunction batchCountFunction;
		private DataProcessorContext dataProcessorContext;
		private ObsLogger obsLogger;

		@BeforeEach
		void setUp() {
			batchCountFunction = mock(BatchCountFunction.class);
			dataProcessorContext = mock(DataProcessorContext.class);
			obsLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(mockInstance,"dataProcessorContext",dataProcessorContext);
			ReflectionTestUtils.setField(mockInstance,"obsLogger",obsLogger);
		}

		@Test
		void testDoCountSynchronously_withBatchCountFunctionNull() {
			doCallRealMethod().when(mockInstance).doCountSynchronously(null, Arrays.asList("table1"), false);
			mockInstance.doCountSynchronously(null, Arrays.asList("table1"), false);

			verify(obsLogger, times(1)).warn(anyString());
		}

		@Test
		void testDoCountSynchronously_withException() throws Throwable {
			String tableName = "table1";
			TapTable tapTable = mock(TapTable.class);
			TapTableMap map = TapTableMap.create("nodeId");
			map.putNew("table1",tapTable,"qualifiedName1");
			when(dataProcessorContext.getTapTableMap()).thenReturn(map);

			when(batchCountFunction.count(any(), any())).thenThrow(new RuntimeException("Test Exception"));

			try {
				doCallRealMethod().when(mockInstance).doCountSynchronously(batchCountFunction, Arrays.asList(tableName), false);
				mockInstance.doCountSynchronously(batchCountFunction, Arrays.asList(tableName), false);
			} catch (Exception e) {
				fail("Exception should be handled");
			}
			verify(obsLogger, never()).warn(anyString());
		}
	}

    @Nested
    class completeTest {

        TaskDto taskDto;
        Node node = mock(Node.class);

        @BeforeEach
        void setUp() {
            taskDto = mock(TaskDto.class);
            when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
            node = mock(Node.class);
            when(mockInstance.getNode()).thenReturn(node);
        }

        @Test
        void testRunningFalse() {
            when(mockInstance.isRunning()).thenReturn(false);
            doCallRealMethod().when(mockInstance).complete();
            assertTrue(mockInstance.complete());
        }

        @Test
        void testDisabledNodeTrue() {
            when(node.disabledNode()).thenReturn(true);
            when(mockInstance.isRunning()).thenReturn(true);
            doCallRealMethod().when(mockInstance).complete();
            assertTrue(mockInstance.complete());
        }

        @Test
        void testDisabledNodeFalse() {
            when(node.disabledNode()).thenReturn(false);
            when(mockInstance.isRunning()).thenReturn(true);
            doCallRealMethod().when(mockInstance).complete();
            assertFalse(mockInstance.complete());
        }
    }

	@Nested
	@DisplayName("Method fillConnectorPropertiesIntoEvent test")
	class fillConnectorPropertiesIntoEventTest {

		private DatabaseTypeEnum.DatabaseType databaseType = null;
		private Connections connections = null;

		private void initDatabaseType() {
			databaseType = new DatabaseTypeEnum.DatabaseType();
			databaseType.setPdkId("test");
			databaseType.setGroup("group");
			databaseType.setVersion("1.1.1");
		}

		private void initConnection() {
			connections = new Connections();
			connections.setDatabase_name("db");
			connections.setDatabase_owner("schema");
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			initDatabaseType();
			ReflectionTestUtils.setField(mockInstance, "databaseType", databaseType);
			initConnection();
			when(dataProcessorContext.getConnections()).thenReturn(connections);
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			doCallRealMethod().when(mockInstance).fillConnectorPropertiesIntoEvent(tapInsertRecordEvent);
			assertDoesNotThrow(() -> mockInstance.fillConnectorPropertiesIntoEvent(tapInsertRecordEvent));

			assertEquals(databaseType.getPdkId(), tapInsertRecordEvent.getPdkId());
			assertEquals(databaseType.getGroup(), tapInsertRecordEvent.getPdkGroup());
			assertEquals(databaseType.getVersion(), tapInsertRecordEvent.getPdkVersion());
			assertEquals(connections.getDatabase_name(), tapInsertRecordEvent.getDatabase());
			assertEquals(connections.getDatabase_owner(), tapInsertRecordEvent.getSchema());
		}

		@Test
		@DisplayName("test database type is null")
		void test2() {
			initConnection();
			when(dataProcessorContext.getConnections()).thenReturn(connections);
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			doCallRealMethod().when(mockInstance).fillConnectorPropertiesIntoEvent(tapInsertRecordEvent);
			assertDoesNotThrow(() -> mockInstance.fillConnectorPropertiesIntoEvent(tapInsertRecordEvent));

			assertNull(tapInsertRecordEvent.getPdkId());
			assertNull(tapInsertRecordEvent.getPdkGroup());
			assertNull(tapInsertRecordEvent.getPdkVersion());
			assertEquals(connections.getDatabase_name(), tapInsertRecordEvent.getDatabase());
			assertEquals(connections.getDatabase_owner(), tapInsertRecordEvent.getSchema());
		}

		@Test
		@DisplayName("test connections is null")
		void test3() {
			initDatabaseType();
			ReflectionTestUtils.setField(mockInstance, "databaseType", databaseType);
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			doCallRealMethod().when(mockInstance).fillConnectorPropertiesIntoEvent(tapInsertRecordEvent);
			assertDoesNotThrow(() -> mockInstance.fillConnectorPropertiesIntoEvent(tapInsertRecordEvent));

			assertEquals(databaseType.getPdkId(), tapInsertRecordEvent.getPdkId());
			assertEquals(databaseType.getGroup(), tapInsertRecordEvent.getPdkGroup());
			assertEquals(databaseType.getVersion(), tapInsertRecordEvent.getPdkVersion());
			assertNull(tapInsertRecordEvent.getDatabase());
			assertNull(tapInsertRecordEvent.getSchema());
		}
	}

	@Nested
	@DisplayName("getCountResult method test")
	class GetCountResultTest {

		@Test
		@DisplayName("test when syncProgress is null")
		void testSyncProgressIsNull() {
			Long count = 100L;
			String tableId = "test_table";
			ReflectionTestUtils.setField(mockInstance, "syncProgress", null);
			doCallRealMethod().when(mockInstance).getCountResult(count, tableId);

			CountResult result = mockInstance.getCountResult(count, tableId);

			assertNotNull(result);
			assertEquals(count, result.getCount());
			assertFalse(result.getDone(), "Done should be false when syncProgress is null");
		}

		@Test
		@DisplayName("test when syncProgress is not null and batchIsOverOfTable returns true")
		void testSyncProgressNotNullAndBatchIsOver() {
			Long count = 200L;
			String tableId = "test_table";
			SyncProgress syncProgress = new SyncProgress();
			ReflectionTestUtils.setField(mockInstance, "syncProgress", syncProgress);
			doCallRealMethod().when(mockInstance).getCountResult(count, tableId);

			try (MockedStatic<BatchOffsetUtil> batchOffsetUtilMock = mockStatic(BatchOffsetUtil.class)) {
				batchOffsetUtilMock.when(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId))
						.thenReturn(true);

				CountResult result = mockInstance.getCountResult(count, tableId);

				assertNotNull(result);
				assertEquals(count, result.getCount());
				assertTrue(result.getDone(), "Done should be true when batchIsOverOfTable returns true");
				batchOffsetUtilMock.verify(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId), times(1));
			}
		}

		@Test
		@DisplayName("test when syncProgress is not null and batchIsOverOfTable returns false")
		void testSyncProgressNotNullAndBatchIsNotOver() {
			Long count = 300L;
			String tableId = "test_table";
			SyncProgress syncProgress = new SyncProgress();
			ReflectionTestUtils.setField(mockInstance, "syncProgress", syncProgress);
			doCallRealMethod().when(mockInstance).getCountResult(count, tableId);

			try (MockedStatic<BatchOffsetUtil> batchOffsetUtilMock = mockStatic(BatchOffsetUtil.class)) {
				batchOffsetUtilMock.when(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId))
						.thenReturn(false);

				CountResult result = mockInstance.getCountResult(count, tableId);

				assertNotNull(result);
				assertEquals(count, result.getCount());
				assertFalse(result.getDone(), "Done should be false when batchIsOverOfTable returns false");
				batchOffsetUtilMock.verify(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId), times(1));
			}
		}

		@Test
		@DisplayName("test with null count")
		void testNullCount() {
			Long count = null;
			String tableId = "test_table";
			SyncProgress syncProgress = new SyncProgress();
			ReflectionTestUtils.setField(mockInstance, "syncProgress", syncProgress);
			doCallRealMethod().when(mockInstance).getCountResult(count, tableId);

			try (MockedStatic<BatchOffsetUtil> batchOffsetUtilMock = mockStatic(BatchOffsetUtil.class)) {
				batchOffsetUtilMock.when(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId))
						.thenReturn(true);

				CountResult result = mockInstance.getCountResult(count, tableId);

				assertNotNull(result);
				assertNull(result.getCount(), "Count should be null when input count is null");
				assertTrue(result.getDone());
			}
		}



		@Test
		@DisplayName("test with different table IDs")
		void testDifferentTableIds() {
			Long count = 500L;
			String tableId1 = "table_1";
			String tableId2 = "table_2";
			SyncProgress syncProgress = new SyncProgress();
			ReflectionTestUtils.setField(mockInstance, "syncProgress", syncProgress);
			doCallRealMethod().when(mockInstance).getCountResult(any(Long.class), anyString());

			try (MockedStatic<BatchOffsetUtil> batchOffsetUtilMock = mockStatic(BatchOffsetUtil.class)) {
				batchOffsetUtilMock.when(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId1))
						.thenReturn(true);
				batchOffsetUtilMock.when(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId2))
						.thenReturn(false);

				CountResult result1 = mockInstance.getCountResult(count, tableId1);
				CountResult result2 = mockInstance.getCountResult(count, tableId2);

				assertNotNull(result1);
				assertEquals(count, result1.getCount());
				assertTrue(result1.getDone(), "Table 1 should be done");

				assertNotNull(result2);
				assertEquals(count, result2.getCount());
				assertFalse(result2.getDone(), "Table 2 should not be done");

				batchOffsetUtilMock.verify(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId1), times(1));
				batchOffsetUtilMock.verify(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId2), times(1));
			}
		}

	}

	@Nested
	@DisplayName("Method reportBatchSize test")
	class ReportBatchSizeTest {
		@Test
		@DisplayName("test reportBatchSize with valid parameters")
		void testReportBatchSizeNormal() {
			HazelcastSourcePdkBaseNode spyInstance = spy(instance);
			TaskDto taskDto = new TaskDto();
			taskDto.setId(new ObjectId());
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
			ReflectionTestUtils.setField(spyInstance, "processorBaseContext", dataProcessorContext);

			try (MockedStatic<TaskAspectManager> taskAspectManagerMock = mockStatic(TaskAspectManager.class)) {
				AspectTask mockContext = mock(AspectTask.class);
				taskAspectManagerMock.when(() -> TaskAspectManager.get(anyString())).thenReturn(mockContext);

				spyInstance.reportBatchSize(100, 5000L);

				verify(mockContext, times(1)).onObserveAspect(any(BatchSizeAspect.class));
			}
		}

		@Test
		@DisplayName("test reportBatchSize when TaskAspectManager returns null")
		void testReportBatchSizeWhenTaskAspectManagerNull() {
			HazelcastSourcePdkBaseNode spyInstance = spy(instance);
			TaskDto taskDto = new TaskDto();
			taskDto.setId(new ObjectId());
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
			ReflectionTestUtils.setField(spyInstance, "processorBaseContext", dataProcessorContext);

			try (MockedStatic<TaskAspectManager> taskAspectManagerMock = mockStatic(TaskAspectManager.class)) {
				taskAspectManagerMock.when(() -> TaskAspectManager.get(anyString())).thenReturn(null);

				assertDoesNotThrow(() -> spyInstance.reportBatchSize(200, 3000L));
			}
		}

		@Test
		@DisplayName("test reportBatchSize with zero batch size")
		void testReportBatchSizeWithZero() {
			HazelcastSourcePdkBaseNode spyInstance = spy(instance);
			TaskDto taskDto = new TaskDto();
			taskDto.setId(new ObjectId());
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
			ReflectionTestUtils.setField(spyInstance, "processorBaseContext", dataProcessorContext);

			try (MockedStatic<TaskAspectManager> taskAspectManagerMock = mockStatic(TaskAspectManager.class)) {
				AspectTask mockContext = mock(AspectTask.class);
				taskAspectManagerMock.when(() -> TaskAspectManager.get(anyString())).thenReturn(mockContext);

				spyInstance.reportBatchSize(0, 1000L);

				verify(mockContext, times(1)).onObserveAspect(any(BatchSizeAspect.class));
			}
		}

		@Test
		@DisplayName("test reportBatchSize with negative values")
		void testReportBatchSizeWithNegative() {
			HazelcastSourcePdkBaseNode spyInstance = spy(instance);
			TaskDto taskDto = new TaskDto();
			taskDto.setId(new ObjectId());
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
			ReflectionTestUtils.setField(spyInstance, "processorBaseContext", dataProcessorContext);

			try (MockedStatic<TaskAspectManager> taskAspectManagerMock = mockStatic(TaskAspectManager.class)) {
				AspectTask mockContext = mock(AspectTask.class);
				taskAspectManagerMock.when(() -> TaskAspectManager.get(anyString())).thenReturn(mockContext);

				spyInstance.reportBatchSize(-10, -500L);

				verify(mockContext, times(1)).onObserveAspect(any(BatchSizeAspect.class));
			}
		}
	}

	@Nested
	@DisplayName("Method getSnapshotProgressManager test")
	class GetSnapshotProgressManagerTest {
		@Test
		@DisplayName("test getSnapshotProgressManager returns non-null")
		void testGetSnapshotProgressManagerNonNull() {
			HazelcastSourcePdkBaseNode spyInstance = spy(instance);
			SnapshotProgressManager mockManager = mock(SnapshotProgressManager.class);
			ReflectionTestUtils.setField(spyInstance, "snapshotProgressManager", mockManager);

			SnapshotProgressManager result = spyInstance.getSnapshotProgressManager();

			assertNotNull(result);
			assertSame(mockManager, result);
		}

		@Test
		@DisplayName("test getSnapshotProgressManager returns null")
		void testGetSnapshotProgressManagerNull() {
			HazelcastSourcePdkBaseNode spyInstance = spy(instance);
			ReflectionTestUtils.setField(spyInstance, "snapshotProgressManager", null);

			SnapshotProgressManager result = spyInstance.getSnapshotProgressManager();

			assertNull(result);
		}
	}

	@Nested
	@DisplayName("Method getEventQueue test")
	class GetEventQueueTest {
		@Test
		@DisplayName("test getEventQueue returns queue")
		void testGetEventQueueNormal() {
			HazelcastSourcePdkBaseNode spyInstance = spy(instance);
			DynamicLinkedBlockingQueue<TapdataEvent> mockEventQueue = mock(DynamicLinkedBlockingQueue.class);
			LinkedBlockingQueue<TapdataEvent> mockQueue = new LinkedBlockingQueue<>();
			when(mockEventQueue.getQueue()).thenReturn(mockQueue);
			ReflectionTestUtils.setField(spyInstance, "eventQueue", mockEventQueue);

			LinkedBlockingQueue<TapdataEvent> result = spyInstance.getEventQueue();

			assertNotNull(result);
			assertSame(mockQueue, result);
			verify(mockEventQueue, times(1)).getQueue();
		}

		@Test
		@DisplayName("test getEventQueue with empty queue")
		void testGetEventQueueEmpty() {
			HazelcastSourcePdkBaseNode spyInstance = spy(instance);
			DynamicLinkedBlockingQueue<TapdataEvent> mockEventQueue = mock(DynamicLinkedBlockingQueue.class);
			LinkedBlockingQueue<TapdataEvent> emptyQueue = new LinkedBlockingQueue<>();
			when(mockEventQueue.getQueue()).thenReturn(emptyQueue);
			ReflectionTestUtils.setField(spyInstance, "eventQueue", mockEventQueue);

			LinkedBlockingQueue<TapdataEvent> result = spyInstance.getEventQueue();

			assertNotNull(result);
			assertTrue(result.isEmpty());
		}

		@Test
		@DisplayName("test getEventQueue with items in queue")
		void testGetEventQueueWithItems() {
			HazelcastSourcePdkBaseNode spyInstance = spy(instance);
			DynamicLinkedBlockingQueue<TapdataEvent> mockEventQueue = mock(DynamicLinkedBlockingQueue.class);
			LinkedBlockingQueue<TapdataEvent> queueWithItems = new LinkedBlockingQueue<>();
			TapdataEvent event1 = mock(TapdataEvent.class);
			TapdataEvent event2 = mock(TapdataEvent.class);
			queueWithItems.offer(event1);
			queueWithItems.offer(event2);
			when(mockEventQueue.getQueue()).thenReturn(queueWithItems);
			ReflectionTestUtils.setField(spyInstance, "eventQueue", mockEventQueue);

			LinkedBlockingQueue<TapdataEvent> result = spyInstance.getEventQueue();

			assertNotNull(result);
			assertEquals(2, result.size());
		}
	}

	@Nested
	@DisplayName("Method startSourceConsumer test")
	class StartSourceConsumerTest {
		@Test
		@DisplayName("test startSourceConsumer with pending event")
		void testStartSourceConsumerWithPendingEvent() throws Exception {
			HazelcastSourcePdkBaseNodeImp spyInstance = spy(instance);
			AtomicInteger loopCount = new AtomicInteger(0);

			// Mock isRunning to run once then stop
			doAnswer(invocation -> loopCount.getAndIncrement() < 1).when(spyInstance).isRunning();

			// Setup pending event
			TapdataEvent pendingEvent = mock(TapdataEvent.class);
			TapEvent tapEvent = mock(TapInsertRecordEvent.class);
			when(pendingEvent.getTapEvent()).thenReturn(tapEvent);
			ReflectionTestUtils.setField(spyInstance, "pendingEvent", pendingEvent);

			// Mock offer to succeed
			doReturn(true).when(spyInstance).offer(any(TapdataEvent.class));

			// Mock getConnectorNode
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapCodecsFilterManager codecsFilterManager = mock(TapCodecsFilterManager.class);
			when(connectorNode.getCodecsFilterManager()).thenReturn(codecsFilterManager);
			doReturn(connectorNode).when(spyInstance).getConnectorNode();

			// Mock snapshotProgressManager
			SnapshotProgressManager mockProgressManager = mock(SnapshotProgressManager.class);
			ReflectionTestUtils.setField(spyInstance, "snapshotProgressManager", mockProgressManager);

			when(spyInstance.tapRecordToTapValue(any(TapEvent.class), any(TapCodecsFilterManager.class))).thenReturn(mock(TransformToTapValueResult.class));

			spyInstance.startSourceConsumer();

			// Verify pendingEvent was processed
			verify(spyInstance, times(1)).offer(pendingEvent);
			assertNull(ReflectionTestUtils.getField(spyInstance, "pendingEvent"));
		}

		@Test
		@DisplayName("test startSourceConsumer with event from queue")
		void testStartSourceConsumerWithEventFromQueue() throws Exception {
			HazelcastSourcePdkBaseNodeImp spyInstance = spy(instance);
			AtomicInteger loopCount = new AtomicInteger(0);

			// Mock isRunning to run once then stop
			doAnswer(invocation -> loopCount.getAndIncrement() < 1).when(spyInstance).isRunning();

			// Setup event queue
			DynamicLinkedBlockingQueue<TapdataEvent> mockEventQueue = mock(DynamicLinkedBlockingQueue.class);
			TapdataEvent event = mock(TapdataEvent.class);
			TapEvent tapEvent = mock(TapInsertRecordEvent.class);
			when(event.getTapEvent()).thenReturn(tapEvent);
			when(mockEventQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn(event);
			ReflectionTestUtils.setField(spyInstance, "eventQueue", mockEventQueue);

			// Mock offer to succeed
			when(spyInstance.offer(any(TapdataEvent.class))).thenReturn(true);

			// Mock getConnectorNode
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapCodecsFilterManager codecsFilterManager = mock(TapCodecsFilterManager.class);
			when(connectorNode.getCodecsFilterManager()).thenReturn(codecsFilterManager);
			doReturn(connectorNode).when(spyInstance).getConnectorNode();

			// Mock snapshotProgressManager
			SnapshotProgressManager mockProgressManager = mock(SnapshotProgressManager.class);
			ReflectionTestUtils.setField(spyInstance, "snapshotProgressManager", mockProgressManager);

			when(spyInstance.tapRecordToTapValue(any(TapEvent.class), any(TapCodecsFilterManager.class))).thenReturn(mock(TransformToTapValueResult.class));

			try (MockedStatic<TapEventUtil> tapEventUtilMock = mockStatic(TapEventUtil.class)) {
				tapEventUtilMock.when(() -> TapEventUtil.getTableId(any(TapEvent.class))).thenReturn("testTable");

				spyInstance.startSourceConsumer();

				verify(spyInstance, times(1)).offer(event);
				verify(mockProgressManager, times(1)).incrementEdgeFinishNumber("testTable");
			}
		}

		@Test
		@DisplayName("test startSourceConsumer when offer fails")
		void testStartSourceConsumerWhenOfferFails() throws Exception {
			HazelcastSourcePdkBaseNodeImp spyInstance = spy(instance);
			AtomicInteger loopCount = new AtomicInteger(0);

			// Mock isRunning to run twice then stop
			doAnswer(invocation -> loopCount.getAndIncrement() < 2).when(spyInstance).isRunning();

			// Setup event queue
			DynamicLinkedBlockingQueue<TapdataEvent> mockEventQueue = mock(DynamicLinkedBlockingQueue.class);
			TapdataEvent event = mock(TapdataEvent.class);
			TapEvent tapEvent = mock(TapInsertRecordEvent.class);
			when(event.getTapEvent()).thenReturn(tapEvent);
			when(mockEventQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn(event, (TapdataEvent) null);
			ReflectionTestUtils.setField(spyInstance, "eventQueue", mockEventQueue);

			// Mock offer to fail first time, succeed second time
			doReturn(false, true).when(spyInstance).offer(any(TapdataEvent.class));

			// Mock getConnectorNode
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapCodecsFilterManager codecsFilterManager = mock(TapCodecsFilterManager.class);
			when(connectorNode.getCodecsFilterManager()).thenReturn(codecsFilterManager);
			doReturn(connectorNode).when(spyInstance).getConnectorNode();

			when(spyInstance.tapRecordToTapValue(any(TapEvent.class), any(TapCodecsFilterManager.class))).thenReturn(mock(TransformToTapValueResult.class));

			spyInstance.startSourceConsumer();

			// Verify event was set as pending after first failed offer
			verify(spyInstance, times(2)).offer(event);
		}

		@Test
		@DisplayName("test startSourceConsumer with InterruptedException")
		void testStartSourceConsumerWithInterruptedException() throws Exception {
			HazelcastSourcePdkBaseNodeImp spyInstance = spy(instance);
			ReflectionTestUtils.setField(spyInstance, "obsLogger", log);

			// Mock isRunning to always return true
			doReturn(true).when(spyInstance).isRunning();

			// Setup event queue to throw InterruptedException
			DynamicLinkedBlockingQueue<TapdataEvent> mockEventQueue = mock(DynamicLinkedBlockingQueue.class);
			when(mockEventQueue.poll(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());
			ReflectionTestUtils.setField(spyInstance, "eventQueue", mockEventQueue);

			// Should exit loop on InterruptedException
			assertDoesNotThrow(spyInstance::startSourceConsumer);
		}

		@Test
		@DisplayName("test startSourceConsumer with null event from queue")
		void testStartSourceConsumerWithNullEvent() throws Exception {
			HazelcastSourcePdkBaseNodeImp spyInstance = spy(instance);
			AtomicInteger loopCount = new AtomicInteger(0);

			// Mock isRunning to run once then stop
			doAnswer(invocation -> loopCount.getAndIncrement() < 1).when(spyInstance).isRunning();

			// Setup event queue to return null
			DynamicLinkedBlockingQueue<TapdataEvent> mockEventQueue = mock(DynamicLinkedBlockingQueue.class);
			when(mockEventQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn(null);
			ReflectionTestUtils.setField(spyInstance, "eventQueue", mockEventQueue);

			spyInstance.startSourceConsumer();

			// Verify offer was never called since event is null
			verify(spyInstance, never()).offer(any(TapdataEvent.class));
		}

		@Test
		@DisplayName("test startSourceConsumer without snapshotProgressManager")
		void testStartSourceConsumerWithoutProgressManager() throws Exception {
			HazelcastSourcePdkBaseNodeImp spyInstance = spy(instance);
			AtomicInteger loopCount = new AtomicInteger(0);

			// Mock isRunning to run once then stop
			doAnswer(invocation -> loopCount.getAndIncrement() < 1).when(spyInstance).isRunning();

			// Setup event queue
			DynamicLinkedBlockingQueue<TapdataEvent> mockEventQueue = mock(DynamicLinkedBlockingQueue.class);
			TapdataEvent event = mock(TapdataEvent.class);
			TapEvent tapEvent = mock(TapInsertRecordEvent.class);
			when(event.getTapEvent()).thenReturn(tapEvent);
			when(mockEventQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn(event);
			ReflectionTestUtils.setField(spyInstance, "eventQueue", mockEventQueue);

			// Mock offer to succeed
			doReturn(true).when(spyInstance).offer(any(TapdataEvent.class));

			// Mock getConnectorNode
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapCodecsFilterManager codecsFilterManager = mock(TapCodecsFilterManager.class);
			when(connectorNode.getCodecsFilterManager()).thenReturn(codecsFilterManager);
			doReturn(connectorNode).when(spyInstance).getConnectorNode();

			// Set snapshotProgressManager to null
			ReflectionTestUtils.setField(spyInstance, "snapshotProgressManager", null);

			when(spyInstance.tapRecordToTapValue(any(TapEvent.class), any(TapCodecsFilterManager.class))).thenReturn(mock(TransformToTapValueResult.class));

			// Should not throw exception even without progress manager
			assertDoesNotThrow(spyInstance::startSourceConsumer);
			verify(spyInstance, times(1)).offer(event);
		}
	}
}

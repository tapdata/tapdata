package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.ex.TestException;
import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.Connections;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import io.tapdata.aspect.TableCountFuncAspect;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.exception.ManagementException;
import io.tapdata.flow.engine.V2.ddl.DDLSchemaHandler;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.GetCurrentTimestampFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.async.AsyncUtils;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
	}

	@Nested
	@DisplayName("doInit methods test")
	class DoInitTest {
		@BeforeEach
		void beforeEach() {
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
	class HandleSchemaChangeTest{
		@Test
		@DisplayName("cover setTableAttr test")
		void testHandleSchemaChangeCoverSetTableAttr(){
			TapEvent tapEvent = mock(TapDDLEvent.class);
			when(((TapDDLEvent) tapEvent).getTableId()).thenReturn("111");
			doCallRealMethod().when(mockInstance).handleSchemaChange(tapEvent);
			when(processorBaseContext.getTapTableMap()).thenReturn(mock(TapTableMap.class));
			DDLSchemaHandler handler = mock(DDLSchemaHandler.class);
			when(mockInstance.ddlSchemaHandler()).thenReturn(handler);
			doNothing().when(handler).updateSchemaByDDLEvent(any(),any());
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
			ReflectionTestUtils.setField(mockInstance,"transformerWsMessageDto",transformerWsMessageDto);
			when(transformerWsMessageDto.getMetadataInstancesDtoList()).thenReturn(mock(ArrayList.class));
			DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
			ReflectionTestUtils.setField(mockInstance,"dagDataService",dagDataService);
			TapTableMap tableMap = mock(TapTableMap.class);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tableMap);
			when(tableMap.getQualifiedName(anyString())).thenReturn("qualifiedName");
			when(dag.transformSchema(any(),any(),any())).thenReturn(mock(HashMap.class));
			MetadataInstancesDto metadata = mock(MetadataInstancesDto.class);
			when(dagDataService.getMetadata(anyString())).thenReturn(metadata);
			ObjectId objectId = mock(ObjectId.class);
			when(metadata.getId()).thenReturn(objectId);
			HashMap attr = mock(HashMap.class);
			when(metadata.getTableAttr()).thenReturn(attr);
			when(objectId.toHexString()).thenReturn("objectId");
			mockInstance.handleSchemaChange(tapEvent);
			assertEquals(attr,metadata.getTableAttr());
		}
	}
	@Nested
	class DdlSchemaHandlerTest{
		@Test
		void testDdlSchemaHandler(){
			try (MockedStatic<InstanceFactory> factory = Mockito
					.mockStatic(InstanceFactory.class)) {
				DDLSchemaHandler handler = mock(DDLSchemaHandler.class);
				factory.when(()->InstanceFactory.bean(DDLSchemaHandler.class)).thenReturn(handler);
				doCallRealMethod().when(mockInstance).ddlSchemaHandler();
				assertEquals(handler,mockInstance.ddlSchemaHandler());
			}
		}
	}
	@Nested
	class initSourceAndEngineTimeDifferenceTest{
		HazelcastSourcePdkBaseNode spyInstance;
		HttpClientMongoOperator clientMongoOperator;
		@BeforeEach
		void beforeEach() {
			spyInstance = Mockito.spy(instance);
			clientMongoOperator = Mockito.mock(HttpClientMongoOperator.class);
			ReflectionTestUtils.setField(spyInstance,"clientMongoOperator",clientMongoOperator);
		}

		@Test
		@SneakyThrows
		@DisplayName("Source and Engine Time difference is too big ")
		void testInitSourceAndEngineTimeDifferenceBig (){
			TaskConfig taskConfig = TaskConfig.create();
			taskConfig.taskRetryConfig(TaskRetryConfig.create());
			taskConfig.getTaskRetryConfig().retryIntervalSecond(1000L);
			when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);

			ConnectorNode connectorNode = new ConnectorNode();
			ConnectorFunctions connectorFunctions = new ConnectorFunctions();
			GetCurrentTimestampFunction getCurrentTimestampFunction = mock(GetCurrentTimestampFunction.class);
			connectorFunctions.supportGetCurrentTimestampFunction(getCurrentTimestampFunction);
			connectorNode.init(null,null,connectorFunctions);
			long time = new Date().getTime() - 2000L;
			when(getCurrentTimestampFunction.now(null)).thenReturn(time);
			doReturn(connectorNode).when(spyInstance).getConnectorNode();
			when(clientMongoOperator.update(any(Query.class),any(Update.class),anyString())).thenAnswer(invocationOnMock -> {
				Update update = invocationOnMock.getArgument(1);
				Document set = (Document) update.getUpdateObject().get("$set");
				Long timeDifference = (Long)set.get("timeDifference");
				Assertions.assertTrue(timeDifference > 1000);
				return null;
			});
			spyInstance.initSourceAndEngineTimeDifference();

		}

		@Test
		@SneakyThrows
		@DisplayName("Source and Engine Time difference ")
		void testInitSourceAndEngineTimeDifference(){
			TaskConfig taskConfig = TaskConfig.create();
			taskConfig.taskRetryConfig(TaskRetryConfig.create());
			taskConfig.getTaskRetryConfig().retryIntervalSecond(1000L);
			when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);

			ConnectorNode connectorNode = new ConnectorNode();
			ConnectorFunctions connectorFunctions = new ConnectorFunctions();
			GetCurrentTimestampFunction getCurrentTimestampFunction = mock(GetCurrentTimestampFunction.class);
			connectorFunctions.supportGetCurrentTimestampFunction(getCurrentTimestampFunction);
			connectorNode.init(null,null,connectorFunctions);
			long time = new Date().getTime() + 1000L;
			when(getCurrentTimestampFunction.now(null)).thenReturn(time);
			doReturn(connectorNode).when(spyInstance).getConnectorNode();
			when(clientMongoOperator.update(any(Query.class),any(Update.class),anyString())).thenAnswer(invocationOnMock -> {
				Update update = invocationOnMock.getArgument(1);
				Document set = (Document) update.getUpdateObject().get("$set");
				assertEquals(0, (int) set.get("timeDifference"));
				return null;
			});
			spyInstance.initSourceAndEngineTimeDifference();
		}

		@Test
		@DisplayName("throw exception test")
		void testThrowException() throws Throwable {
			TaskConfig taskConfig = TaskConfig.create();
			taskConfig.taskRetryConfig(TaskRetryConfig.create());
			taskConfig.getTaskRetryConfig().retryIntervalSecond(1000L);
			when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);

			ConnectorNode connectorNode = new ConnectorNode();
			ConnectorFunctions connectorFunctions = new ConnectorFunctions();
			GetCurrentTimestampFunction getCurrentTimestampFunction = mock(GetCurrentTimestampFunction.class);
			connectorFunctions.supportGetCurrentTimestampFunction(getCurrentTimestampFunction);
			connectorNode.init(null,null,connectorFunctions);
			when(getCurrentTimestampFunction.now(null)).thenThrow(new Throwable());
			doReturn(connectorNode).when(spyInstance).getConnectorNode();
			Assertions.assertThrows(NodeException.class,()->spyInstance.initSourceAndEngineTimeDifference());
		}

		@Test
		@DisplayName("throw exception update test")
		void testUpdateThrowException() throws Throwable {
			TaskConfig taskConfig = TaskConfig.create();
			taskConfig.taskRetryConfig(TaskRetryConfig.create());
			taskConfig.getTaskRetryConfig().retryIntervalSecond(1000L);
			when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);

			ConnectorNode connectorNode = new ConnectorNode();
			ConnectorFunctions connectorFunctions = new ConnectorFunctions();
			GetCurrentTimestampFunction getCurrentTimestampFunction = mock(GetCurrentTimestampFunction.class);
			connectorFunctions.supportGetCurrentTimestampFunction(getCurrentTimestampFunction);
			connectorNode.init(null,null,connectorFunctions);
			long time = new Date().getTime() + 1000L;
			when(getCurrentTimestampFunction.now(null)).thenReturn(time);
			doReturn(connectorNode).when(spyInstance).getConnectorNode();
			when(clientMongoOperator.update(any(Query.class),any(Update.class),anyString())).thenThrow(new ManagementException());
			ObsLogger obsLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(spyInstance,"obsLogger",obsLogger);
			spyInstance.initSourceAndEngineTimeDifference();
			verify(obsLogger,times(1)).warn("Failed to save engine and source time difference errors: {}",null);
		}

		@Test
		@DisplayName("ConnectorNode is Null test")
		void testConnectorNodeIsNull(){
			spyInstance.initSourceAndEngineTimeDifference();
			verify(clientMongoOperator,times(0)).update(any(Query.class),any(Update.class),anyString());
		}

		@Test
		@DisplayName("CurrentTimestampFunction is Null test")
		void testCurrentTimestampFunctionIsNull(){
			TaskConfig taskConfig = TaskConfig.create();
			taskConfig.taskRetryConfig(TaskRetryConfig.create());
			taskConfig.getTaskRetryConfig().retryIntervalSecond(1000L);
			when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);
			ConnectorNode connectorNode = new ConnectorNode();
			ConnectorFunctions connectorFunctions = new ConnectorFunctions();
			connectorNode.init(null,null,connectorFunctions);
			spyInstance.initSourceAndEngineTimeDifference();
			verify(clientMongoOperator,times(0)).update(any(Query.class),any(Update.class),anyString());
		}

		@Test
		@DisplayName("SourceTimestamp is Null test")
		void testSourceTimestampIsNull() throws Throwable {
			TaskConfig taskConfig = TaskConfig.create();
			taskConfig.taskRetryConfig(TaskRetryConfig.create());
			taskConfig.getTaskRetryConfig().retryIntervalSecond(1000L);
			when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);
			ConnectorNode connectorNode = new ConnectorNode();
			ConnectorFunctions connectorFunctions = new ConnectorFunctions();
			GetCurrentTimestampFunction getCurrentTimestampFunction = mock(GetCurrentTimestampFunction.class);
			connectorFunctions.supportGetCurrentTimestampFunction(getCurrentTimestampFunction);
			connectorNode.init(null,null,connectorFunctions);
			when(getCurrentTimestampFunction.now(null)).thenReturn(0L);
			spyInstance.initSourceAndEngineTimeDifference();
			verify(clientMongoOperator,times(0)).update(any(Query.class),any(Update.class),anyString());
		}
	}
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import com.hazelcast.jet.core.Processor;
import com.tapdata.constant.BeanUtil;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.TableBatchReadStatus;
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.cdcdelay.CdcDelay;
import com.tapdata.tm.commons.cdcdelay.ICdcDelay;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.SourceStateAspect;
import io.tapdata.aspect.taskmilestones.*;
import io.tapdata.common.TapInterfaceUtil;
import io.tapdata.dao.DoSnapshotFunctions;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.schema.partition.TapSubPartitionTableInfo;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.NodeException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskContext;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TerminalMode;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapTimeForm;
import io.tapdata.pdk.apis.entity.TapTimeUnit;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.CountByPartitionFilterFunction;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.functions.connector.target.CreateIndexFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.SchemaProxy;
import io.tapdata.schema.TapTableMap;
import javassist.*;
import lombok.SneakyThrows;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;


@DisplayName("HazelcastSourcePdkDataNode Class Test")
public class HazelcastSourcePdkDataNodeTest extends BaseHazelcastNodeTest {

	private HazelcastSourcePdkDataNode hazelcastSourcePdkDataNode;

	@BeforeEach
	void beforeEach() {
		super.allSetup();
		hazelcastSourcePdkDataNode = spy(new HazelcastSourcePdkDataNode(dataProcessorContext));
	}

	@Nested
	@DisplayName("flushPollingCDCOffset input TapEvent list method test")
	class flushPollingCDCOffsetTapEventListTest {
		@BeforeEach
		void setUp() {
			doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).flushPollingCDCOffset(any(TapEvent.class));
		}

		@Test
		@SneakyThrows
		@DisplayName("main process test")
		void testMainProcess() {
			List<TapEvent> tapEvents = new ArrayList<>();
			tapEvents.add(new TapInsertRecordEvent().after(new HashMap<>()));
			tapEvents.add(new TapUpdateRecordEvent().after(new HashMap<>()));
			tapEvents.add(new TapDropFieldEvent().fieldName("test"));
			hazelcastSourcePdkDataNode.flushPollingCDCOffset(tapEvents);
			verify(hazelcastSourcePdkDataNode, times(1)).flushPollingCDCOffset(any(TapEvent.class));
		}

		@Test
		@SneakyThrows
		@DisplayName("input null or empty list test")
		void testInputNullOrEmptyList() {
			hazelcastSourcePdkDataNode.getClass().getDeclaredMethod("flushPollingCDCOffset", List.class).invoke(hazelcastSourcePdkDataNode, new Object[]{null});
			verify(hazelcastSourcePdkDataNode, times(0)).flushPollingCDCOffset(any(TapEvent.class));

			hazelcastSourcePdkDataNode.flushPollingCDCOffset(new ArrayList<>());
			verify(hazelcastSourcePdkDataNode, times(0)).flushPollingCDCOffset(any(TapEvent.class));
		}
	}

	@Nested
	@DisplayName("flushPollingCDCOffset input TapEvent method test")
	class flushPollingCDCOffsetTapEventTest {

		private SyncProgress syncProgress;
		private Map<String, Object> streamOffsetObj;
		private Map<String, Object> data;
		private TapCodecsFilterManager tapCodecsFilterManager;

		@BeforeEach
		void setUp() {
			syncProgress = new SyncProgress();
			streamOffsetObj = new HashMap<>();
			syncProgress.setStreamOffsetObj(streamOffsetObj);
			List<String> conditionFields = new ArrayList<>();
			conditionFields.add("id");
			conditionFields.add("code");
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode, "syncProgress", syncProgress);
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode, "conditionFields", conditionFields);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			tapCodecsFilterManager = mock(TapCodecsFilterManager.class);
			when(connectorNode.getCodecsFilterManager()).thenReturn(tapCodecsFilterManager);
			doReturn(connectorNode).when(hazelcastSourcePdkDataNode).getConnectorNode();
			doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).toTapValue(any(Map.class), anyString(), eq(tapCodecsFilterManager));
			doAnswer(invocationOnMock -> null).when(hazelcastSourcePdkDataNode).fromTapValue(any(Map.class), eq(tapCodecsFilterManager), anyString());

			data = new HashMap<>();
			data.put("id", 1);
			data.put("code", "a");
			data.put("test", "test");
		}

		@Test
		@DisplayName("test input TapInsertEvent")
		void testTapInsertEvent() {
			doReturn(true).when(hazelcastSourcePdkDataNode).isPollingCDC(any(Node.class));
			TapInsertRecordEvent tapInsertRecordEvent = spy(new TapInsertRecordEvent().after(data).table("dummy_test"));

			hazelcastSourcePdkDataNode.flushPollingCDCOffset(tapInsertRecordEvent);

			verify(tapInsertRecordEvent, times(1)).getAfter();
			assertTrue(streamOffsetObj.containsKey("dummy_test"));
			Object tableOffsetObj = streamOffsetObj.get("dummy_test");
			assertInstanceOf(Map.class, tableOffsetObj);
			assertEquals(1, ((Map) tableOffsetObj).get("id"));
			assertEquals("a", ((Map) tableOffsetObj).get("code"));
			verify(hazelcastSourcePdkDataNode, times(1)).toTapValue(any(Map.class), anyString(), eq(tapCodecsFilterManager));
		}

		@Test
		@DisplayName("test input TapUpdateEvent")
		void testTapUpdateEvent() {
			doReturn(true).when(hazelcastSourcePdkDataNode).isPollingCDC(any(Node.class));
			TapUpdateRecordEvent tapUpdateRecordEvent = spy(new TapUpdateRecordEvent().after(data).table("dummy_test"));

			hazelcastSourcePdkDataNode.flushPollingCDCOffset(tapUpdateRecordEvent);

			verify(tapUpdateRecordEvent, times(1)).getAfter();
			assertTrue(streamOffsetObj.containsKey("dummy_test"));
			Object tableOffsetObj = streamOffsetObj.get("dummy_test");
			assertInstanceOf(Map.class, tableOffsetObj);
			assertEquals(1, ((Map) tableOffsetObj).get("id"));
			assertEquals("a", ((Map) tableOffsetObj).get("code"));
			verify(hazelcastSourcePdkDataNode, times(1)).toTapValue(any(Map.class), anyString(), eq(tapCodecsFilterManager));
		}

		@Test
		@DisplayName("test input TapDeleteEvent")
		void testTapDeleteEvent() {
			doReturn(true).when(hazelcastSourcePdkDataNode).isPollingCDC(any(Node.class));
			TapDeleteRecordEvent tapDeleteRecordEvent = spy(new TapDeleteRecordEvent().before(data).table("dummy_test"));

			hazelcastSourcePdkDataNode.flushPollingCDCOffset(tapDeleteRecordEvent);

			verify(tapDeleteRecordEvent, times(1)).getBefore();
			assertTrue(streamOffsetObj.containsKey("dummy_test"));
			Object tableOffsetObj = streamOffsetObj.get("dummy_test");
			assertInstanceOf(Map.class, tableOffsetObj);
			assertEquals(1, ((Map) tableOffsetObj).get("id"));
			assertEquals("a", ((Map) tableOffsetObj).get("code"));
			verify(hazelcastSourcePdkDataNode, times(1)).toTapValue(any(Map.class), anyString(), eq(tapCodecsFilterManager));
		}

		@Test
		@DisplayName("test input TapDDLEvent")
		void testTapDDLEvent() {
			doReturn(true).when(hazelcastSourcePdkDataNode).isPollingCDC(any(Node.class));
			TapDDLEvent tapDDLEvent = spy(new TapAlterFieldNameEvent());

			hazelcastSourcePdkDataNode.flushPollingCDCOffset(tapDDLEvent);

			verify(hazelcastSourcePdkDataNode, times(1)).getNode();
			verify(hazelcastSourcePdkDataNode, times(0)).toTapValue(any(Map.class), anyString(), any(TapCodecsFilterManager.class));
		}

		@Test
		@DisplayName("test isPollingCDC is false")
		void testIsNotPollingCDC() {
			doReturn(false).when(hazelcastSourcePdkDataNode).isPollingCDC(any(Node.class));
			TapInsertRecordEvent tapInsertRecordEvent = spy(new TapInsertRecordEvent());

			hazelcastSourcePdkDataNode.flushPollingCDCOffset(tapInsertRecordEvent);

			verify(hazelcastSourcePdkDataNode, times(1)).getNode();
			verify(tapInsertRecordEvent, times(0)).getAfter();
		}
	}

	@Test
	@DisplayName("test QueryOperator ")
	void testQueryOperator() {
		String dateTime = "2023-12-20 12:23:20";
		QueryOperator queryOperator = new QueryOperator("createTime", dateTime, 1);
		queryOperator(dateTime, queryOperator);
		Assert.assertTrue(queryOperator.getOriginalValue() == dateTime);
	}


	@Test
	@DisplayName("test QueryOperator HasOriginalValue ")
	void testQueryOperatorHasOriginalValue() {
		String dateTime = "2021-12-20 12:23:24";
		QueryOperator queryOperator = new QueryOperator("createTime", dateTime, 1);
		queryOperator.setOriginalValue(dateTime);
		queryOperator(dateTime, queryOperator);
		LocalDateTime localDateTime;
		String datetimeFormat = "yyyy-MM-dd HH:mm:ss";
		try {
			localDateTime = LocalDateTime.parse(dateTime, DateTimeFormatter.ofPattern(datetimeFormat));
		} catch (Exception e) {
			throw new RuntimeException("The input string format is incorrect, expected format: " + datetimeFormat + ", actual value: " + dateTime);
		}
		ZonedDateTime gmtZonedDateTime = localDateTime.atZone(ZoneId.of("GMT"));
		DateTime expectedValue = new DateTime(gmtZonedDateTime);
		Assert.assertTrue(expectedValue.compareTo((DateTime) queryOperator.getValue()) == 0);
	}


	public void queryOperator(String dateTime,QueryOperator queryOperator){
		List<QueryOperator> conditions = new ArrayList<>();

		conditions.add(queryOperator);
		TableNode tableNodeTemp =new TableNode();
		tableNodeTemp.setIsFilter(true);
		tableNodeTemp.setConditions(conditions);
		tableNodeTemp.setTableName("test");
		when(dataProcessorContext.getNode()).thenReturn((Node) tableNodeTemp);
		TapTable tapTable = new TapTable();
		LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap<>();
		TapField tapField = new TapField("createTime", "TapDateTime");
		tapField.setTapType(new TapDateTime());
		nameFieldMap.put("createTime",tapField);
		tapTable.setNameFieldMap(nameFieldMap);
		tapTable.setName("test");
		tapTable.setId("test");
		TapTableMap<String, TapTable>  tapTableMap = TapTableMap.create("test",tapTable);

		when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
		ReflectionTestUtils.invokeMethod(hazelcastSourcePdkDataNode,"batchFilterRead");

	}

	@Nested
	@DisplayName("test HazelcastSourcePdkDataNode by MergeHazelcastSourcePdkDataNode")
	class MergeHazelcastSourcePdkDataNodeTest {
		class MergeHazelcastSourcePdkDataNode extends HazelcastSourcePdkDataNode {

			public MergeHazelcastSourcePdkDataNode(DataProcessorContext dataProcessorContext) {
				super(dataProcessorContext);
			}

			@Override
			protected boolean isRunning() {
				return true;
			}
		}

		MergeHazelcastSourcePdkDataNode instance;
		SyncProgress syncProgress;
		ObsLogger obsLogger;
		AtomicBoolean sourceRunnerFirstTime;
		SourceStateAspect sourceStateAspect;
		ReentrantLock sourceRunnerLock;
		CopyOnWriteArrayList<String> removeTables;
		CopyOnWriteArrayList<String> newTables;
		AtomicBoolean endSnapshotLoop;

		@BeforeEach
		void init() {
			instance = mock(MergeHazelcastSourcePdkDataNode.class);

			syncProgress = mock(SyncProgress.class);
			ReflectionTestUtils.setField(instance, "syncProgress", syncProgress);
			obsLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(instance, "obsLogger", obsLogger);
			sourceRunnerFirstTime = mock(AtomicBoolean.class);
			ReflectionTestUtils.setField(instance, "sourceRunnerFirstTime", sourceRunnerFirstTime);
			endSnapshotLoop = mock(AtomicBoolean.class);
			ReflectionTestUtils.setField(instance, "endSnapshotLoop", endSnapshotLoop);
			sourceStateAspect = mock(SourceStateAspect.class);
			ReflectionTestUtils.setField(instance, "sourceStateAspect", sourceStateAspect);
			sourceRunnerLock = mock(ReentrantLock.class);
			ReflectionTestUtils.setField(instance, "sourceRunnerLock", sourceRunnerLock);
			removeTables = mock(CopyOnWriteArrayList.class);
			ReflectionTestUtils.setField(instance, "removeTables", removeTables);
			newTables = mock(CopyOnWriteArrayList.class);
			ReflectionTestUtils.setField(instance, "newTables", newTables);
			ReflectionTestUtils.setField(instance, "dataProcessorContext", dataProcessorContext);
			instance.readBatchSize = 100;
		}

		@Nested
		@DisplayName("Method doSnapshot test")
		class DoSnapshotTest {
			List<String> tableList;
			ConnectorNode connectorNode;
			ConnectorFunctions connectorFunctions;
			BatchCountFunction batchCountFunction;
			DatabaseTypeEnum.DatabaseType databaseType;
			BatchReadFunction batchReadFunction;
			QueryByAdvanceFilterFunction queryByAdvanceFilterFunction;
			ExecuteCommandFunction executeCommandFunction;
			TapTableMap<String, TapTable> tapTableMap;
			TapTable tapTable;
			String tableId;
			Object tableOffset;
			PDKMethodInvoker pdkMethodInvoker;
			AutoCloseable ignoreTableCountCloseable;
			@BeforeEach
			void init() throws Throwable {
				tableId = "id";
				tableList = new ArrayList<>();
				tableList.add(tableId);

				doCallRealMethod().when(instance).checkFunctions(tableList);
				doCallRealMethod().when(instance).doSnapshotInvoke(anyString(),any(DoSnapshotFunctions.class), any(TapTable.class), any(AtomicBoolean.class), anyString());
				doCallRealMethod().when(instance).handleThrowable(anyString(),any(Throwable.class));
				when(instance.executeAspect(any(SnapshotReadBeginAspect.class))).thenReturn(mock(AspectInterceptResult.class));
				doNothing().when(syncProgress).setSyncStage(SyncStage.INITIAL_SYNC.name());

				connectorNode = mock(ConnectorNode.class);
				when(instance.getConnectorNode()).thenReturn(connectorNode);
				connectorFunctions = mock(ConnectorFunctions.class);
				when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
				batchCountFunction = mock(BatchCountFunction.class);
				when(connectorFunctions.getBatchCountFunction()).thenReturn(batchCountFunction);
				databaseType = mock(DatabaseTypeEnum.DatabaseType.class);
				when(dataProcessorContext.getDatabaseType()).thenReturn(databaseType);

				// null == batchCountFunction
				doNothing().when(obsLogger).warn("PDK node does not support table batch count: {}", databaseType);
				doNothing().when(instance).setDefaultRowSizeMap();

				batchReadFunction = mock(BatchReadFunction.class);
				when(connectorFunctions.getBatchReadFunction()).thenReturn(batchReadFunction);
				queryByAdvanceFilterFunction = mock(QueryByAdvanceFilterFunction.class);
				when(connectorFunctions.getQueryByAdvanceFilterFunction()).thenReturn(queryByAdvanceFilterFunction);
				executeCommandFunction = mock(ExecuteCommandFunction.class);
				when(connectorFunctions.getExecuteCommandFunction()).thenReturn(executeCommandFunction);

				// ourceRunnerFirstTime.get() == true
				when(sourceRunnerFirstTime.get()).thenReturn(true);
				when(sourceStateAspect.state(SourceStateAspect.STATE_INITIAL_SYNC_START)).thenReturn(mock(SourceStateAspect.class));
				when(instance.executeAspect(any(SourceStateAspect.class))).thenReturn(mock(AspectInterceptResult.class));

				when(instance.isRunning()).thenReturn(true, true, false, true);

				tapTableMap = mock(TapTableMap.class);
				when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
				tapTable = mock(TapTable.class);
				when(tapTableMap.get(tableId)).thenReturn(tapTable);
				when(tapTable.getId()).thenReturn(tableId);

				//
				doNothing().when(obsLogger).info("Skip table [{}] in batch read, reason: last task, this table has been completed batch read", "id");

				tableOffset = mock(Object.class);

				when(instance.executeAspect(any(SnapshotReadTableBeginAspect.class))).thenReturn(mock(AspectInterceptResult.class));

				doNothing().when(instance).lockBySourceRunnerLock();
				doNothing().when(instance).unLockBySourceRunnerLock();

				when(removeTables.contains("id")).thenReturn(false);

				// removeTables.contains("id") == true
				doNothing().when(obsLogger).info("Table {} is detected that it has been removed, the snapshot read will be skipped", "id");
				when(removeTables.remove("id")).thenReturn(true);

				doNothing().when(obsLogger).info("Starting batch read, table name: {}, offset: {}", "id", tableOffset);
				pdkMethodInvoker = mock(PDKMethodInvoker.class);
				when(instance.createPdkMethodInvoker()).thenReturn(pdkMethodInvoker);
				ignoreTableCountCloseable = mock(AutoCloseable.class);
				doNothing().when(ignoreTableCountCloseable).close();
				when(instance.doAsyncTableCount(batchCountFunction, tableId)).thenReturn(ignoreTableCountCloseable);

				when(connectorNode.getConnectorContext()).thenReturn(mock(TapConnectorContext.class));
				when(instance.getDataProcessorContext()).thenReturn(mock(DataProcessorContext.class));


				when(pdkMethodInvoker.runnable(any(CommonUtils.AnyError.class))).thenAnswer(a -> null);

				when(instance.executeDataFuncAspect(any(Class.class), any(Callable.class), any(CommonUtils.AnyErrorConsumer.class))).thenAnswer(a -> {
					Callable<?> callable = a.getArgument(1, Callable.class);
					CommonUtils.AnyErrorConsumer<BatchReadFuncAspect> errorConsumer = a.getArgument(2, CommonUtils.AnyErrorConsumer.class);
					Object call = callable.call();

					try(MockedStatic<PDKInvocationMonitor> pdk = mockStatic(PDKInvocationMonitor.class)) {
						pdk.when(() -> PDKInvocationMonitor.invoke(
								connectorNode,
								PDKMethod.SOURCE_BATCH_READ,
								pdkMethodInvoker)).thenAnswer(ans -> null);
						errorConsumer.accept(mock(BatchReadFuncAspect.class));
					}

					Assertions.assertNotNull(call);
					Assertions.assertEquals(BatchReadFuncAspect.class.getName(), call.getClass().getName());
					return mock(AspectInterceptResult.class);
				});

				doNothing().when(obsLogger).info("Table [{}] has been completed batch read, will skip batch read on the next run", "id");
				doNothing().when(instance).removePdkMethodInvoker(pdkMethodInvoker);

				//fonally
				when(instance.executeAspect(any(SnapshotReadTableEndAspect.class))).thenReturn(mock(AspectInterceptResult.class));
				doNothing().when(instance).enqueue(any(TapdataCompleteTableSnapshotEvent.class));

				//catch
				when(instance.executeAspect(any(SnapshotReadTableErrorAspect.class))).thenReturn(mock(AspectInterceptResult.class));
				doNothing().when(sourceRunnerLock).unlock();

				when(newTables.isEmpty()).thenReturn(false);
				when(newTables.toArray()).thenReturn(new String[0]);
				doNothing().when(newTables).clear();

				doNothing().when(endSnapshotLoop).set(true);


				doNothing().when(instance).enqueue(any(TapdataCompleteSnapshotEvent.class));
				when(sourceStateAspect.state(SourceStateAspect.STATE_INITIAL_SYNC_COMPLETED)).thenReturn(mock(SourceStateAspect.class));
				when(instance.executeAspect(any(SourceStateAspect.class))).thenReturn(mock(AspectInterceptResult.class));

				when(instance.executeAspect(any(SnapshotReadEndAspect.class))).thenReturn(mock(AspectInterceptResult.class));

				when(instance.getProcessorBaseContext()).thenReturn(mock(ProcessorBaseContext.class));
			}

			void assertVerifySame() {
				verify(instance, times(1)).executeAspect(any(SnapshotReadBeginAspect.class));
				verify(syncProgress, times(1)).setSyncStage(SyncStage.INITIAL_SYNC.name());
				verify(instance, times(1)).getConnectorNode();
				verify(connectorNode, times(1)).getConnectorFunctions();
				verify(connectorFunctions, times(1)).getBatchCountFunction();
				verify(connectorFunctions, times(1)).getBatchReadFunction();
				verify(connectorFunctions, times(1)).getQueryByAdvanceFilterFunction();
				verify(connectorFunctions, times(1)).getExecuteCommandFunction();
				verify(dataProcessorContext, times(1)).getDatabaseType();
			}

			void verifyAssert(VerifyDifferent v) throws Exception {
				assertVerifySame();
				verify(instance, times(v.isRunning())).isRunning();
				verify(sourceRunnerFirstTime, times(v.sourceRunnerFirstTimeGet())).get();
				verify(instance, times(v.setDefaultRowSizeMap())).setDefaultRowSizeMap();
				verify(sourceStateAspect, times(v.stateINITIAL())).state(SourceStateAspect.STATE_INITIAL_SYNC_START);
				verify(instance, times(v.executeAspect())).executeAspect(any(SourceStateAspect.class));
				verify(dataProcessorContext, times(v.getTapTableMap())).getTapTableMap();
				verify(tapTableMap, times(v.getTable())).get(tableId);
				verify(tapTable, times(v.tableGetId())).getId();
				verify(obsLogger, times(v.obsLoggerInfo1())).trace("Skip table [{}] in batch read, reason: last task, this table has been completed batch read", "id");
				verify(instance, times(v.snapshotReadTableBeginAspect())).executeAspect(any(SnapshotReadTableBeginAspect.class));
				verify(instance, times(v.lockBySourceRunnerLock())).lockBySourceRunnerLock();
				verify(removeTables, times(v.removeTablesContains())).contains(tableId);
				verify(removeTables, times(v.removeTablesRemove())).remove(tableId);
				//verify(obsLogger, times(v.obsLoggerInfo2())).trace("Table {} is detected that it has been removed, the snapshot read will be skipped", "id");
				verify(instance, times(v.createPdkMethodInvoker())).createPdkMethodInvoker();
				verify(instance, times(v.doAsyncTableCount())).doAsyncTableCount(batchCountFunction, tableId);
//				verify(ignoreTableCountCloseable, times(v.close())).close();
				verify(instance,times(v.executeDataFuncAspect())).executeDataFuncAspect(any(Class.class), any(Callable.class), any(CommonUtils.AnyErrorConsumer.class));
				//verify(obsLogger, times(v.obsLoggerInfo4())).trace("Table [{}] has been completed batch read, will skip batch read on the next run", "id");
				verify(instance, times(v.removePdkMethodInvoker())).removePdkMethodInvoker(pdkMethodInvoker);
				verify(instance, times(v.snapshotReadTableEndAspect())).executeAspect(any(SnapshotReadTableEndAspect.class));
				verify(instance, times(v.tapDataCompleteTableSnapshotEvent())).enqueue(any(TapdataCompleteTableSnapshotEvent.class));
				verify(instance, times(v.snapshotReadTableErrorAspect())).executeAspect(any(SnapshotReadTableErrorAspect.class));
				verify(instance, times(v.unlock())).unLockBySourceRunnerLock();
				verify(newTables, times(v.newTablesIsEmpty())).isEmpty();
				verify(newTables, times(v.newTablesToArray())).toArray();
				verify(newTables, times(v.newTablesToArray())).clear();
				verify(endSnapshotLoop, times(v.endSnapshotLoopSet())).set(true);
				verify(instance, times(v.tapdataCompleteSnapshotEvent())).enqueue(any(TapdataCompleteSnapshotEvent.class));
				verify(sourceStateAspect, times(v.stateCOMPLETED())).state(SourceStateAspect.STATE_INITIAL_SYNC_COMPLETED);
				verify(instance, times(v.snapshotReadEndAspect())).executeAspect(any(SnapshotReadEndAspect.class));
				verify(obsLogger, times(v.warn())).warn("PDK node does not support table batch count: {}", databaseType);
				verify(instance, times(v.getProcessorBaseContext())).getProcessorBaseContext();
			}

			class VerifyDifferent {
				int isRunning;
				int getProcessorBaseContext;
				int sourceRunnerFirstTimeGet;
				int warn;
				int setDefaultRowSizeMap;
				int stateINITIAL;
				int executeAspect;
				int getTapTableMap;
				int getTable;
				int tableGetId;
				int batchIsOverOfTable;
				int getBatchOffsetOfTable;
				int obsLoggerInfo1;
				int obsLoggerInfo2;
				int obsLoggerInfo3;
				int obsLoggerInfo4;
				int snapshotReadTableBeginAspect;
				int lockBySourceRunnerLock;
				int removeTablesContains;
				int removeTablesRemove;
				int createPdkMethodInvoker;
				int doAsyncTableCount;
				int close;
				int executeDataFuncAspect;
				int updateBatchOffset;
				int removePdkMethodInvoker;
				int snapshotReadTableEndAspect;
				int tapDataCompleteTableSnapshotEvent;
				int snapshotReadTableErrorAspect;
				int unlock;
				int stateCOMPLETED;
				int newTablesIsEmpty;
				int newTablesToArray;
				int endSnapshotLoopSet;
				int tapdataCompleteSnapshotEvent;
				int snapshotReadEndAspect;

				public int isRunning() {
					return isRunning;
				}
				public VerifyDifferent isRunning(int isRunning) {
					this.isRunning = isRunning;
					return this;
				}
				public int getProcessorBaseContext() {
					return getProcessorBaseContext;
				}
				public VerifyDifferent getProcessorBaseContext(int getProcessorBaseContext) {
					this.getProcessorBaseContext = getProcessorBaseContext;
					return this;
				}
				public int warn() {
					return warn;
				}
				public VerifyDifferent warn(int warn) {
					this.warn = warn;
					return this;
				}
				public int sourceRunnerFirstTimeGet() {
					return sourceRunnerFirstTimeGet;
				}
				public VerifyDifferent sourceRunnerFirstTimeGet(int sourceRunnerFirstTimeGet) {
					this.sourceRunnerFirstTimeGet = sourceRunnerFirstTimeGet;
					return this;
				}
				public int setDefaultRowSizeMap() {
					return setDefaultRowSizeMap;
				}
				public VerifyDifferent setDefaultRowSizeMap(int setDefaultRowSizeMap) {
					this.setDefaultRowSizeMap = setDefaultRowSizeMap;
					return this;
				}
				public int snapshotReadEndAspect() {
					return snapshotReadEndAspect;
				}
				public VerifyDifferent snapshotReadEndAspect(int snapshotReadEndAspect) {
					this.snapshotReadEndAspect = snapshotReadEndAspect;
					return this;
				}
				public int stateCOMPLETED() {
					return stateCOMPLETED;
				}
				public VerifyDifferent stateCOMPLETED(int stateCOMPLETED) {
					this.stateCOMPLETED = stateCOMPLETED;
					return this;
				}
				public int tapdataCompleteSnapshotEvent() {
					return tapdataCompleteSnapshotEvent;
				}
				public VerifyDifferent tapdataCompleteSnapshotEvent(int tapdataCompleteSnapshotEvent) {
					this.tapdataCompleteSnapshotEvent = tapdataCompleteSnapshotEvent;
					return this;
				}
				public int endSnapshotLoopSet() {
					return endSnapshotLoopSet;
				}
				public VerifyDifferent endSnapshotLoopSet(int endSnapshotLoopSet) {
					this.endSnapshotLoopSet = endSnapshotLoopSet;
					return this;
				}
				public int newTablesToArray() {
					return newTablesToArray;
				}
				public VerifyDifferent newTablesToArray(int newTablesToArray) {
					this.newTablesToArray = newTablesToArray;
					return this;
				}
				public int newTablesIsEmpty() {
					return newTablesIsEmpty;
				}
				public VerifyDifferent newTablesIsEmpty(int newTablesIsEmpty) {
					this.newTablesIsEmpty = newTablesIsEmpty;
					return this;
				}
				public int unlock() {
					return unlock;
				}
				public VerifyDifferent unlock(int unlock) {
					this.unlock = unlock;
					return this;
				}
				public int snapshotReadTableErrorAspect() {
					return snapshotReadTableErrorAspect;
				}
				public VerifyDifferent snapshotReadTableErrorAspect(int snapshotReadTableErrorAspect) {
					this.snapshotReadTableErrorAspect = snapshotReadTableErrorAspect;
					return this;
				}
				public int tapDataCompleteTableSnapshotEvent() {
					return tapDataCompleteTableSnapshotEvent;
				}
				public VerifyDifferent tapDataCompleteTableSnapshotEvent(int tapdataCompleteTableSnapshotEvent) {
					this.tapDataCompleteTableSnapshotEvent = tapdataCompleteTableSnapshotEvent;
					return this;
				}
				public int snapshotReadTableEndAspect() {
					return snapshotReadTableEndAspect;
				}
				public VerifyDifferent snapshotReadTableEndAspect(int snapshotReadTableEndAspect) {
					this.snapshotReadTableEndAspect = snapshotReadTableEndAspect;
					return this;
				}
				public int removePdkMethodInvoker() {
					return removePdkMethodInvoker;
				}
				public VerifyDifferent removePdkMethodInvoker(int removePdkMethodInvoker) {
					this.removePdkMethodInvoker = removePdkMethodInvoker;
					return this;
				}
				public int obsLoggerInfo4() {
					return obsLoggerInfo4;
				}
				public VerifyDifferent obsLoggerInfo4(int obsLoggerInfo4) {
					this.obsLoggerInfo4 = obsLoggerInfo4;
					return this;
				}
				public int updateBatchOffset() {
					return updateBatchOffset;
				}
				public VerifyDifferent updateBatchOffset(int updateBatchOffset) {
					this.updateBatchOffset = updateBatchOffset;
					return this;
				}
				public int stateINITIAL() {
					return stateINITIAL;
				}
				public VerifyDifferent stateINITIAL(int stateINITIAL) {
					this.stateINITIAL = stateINITIAL;
					return this;
				}

				public int executeAspect() {
					return executeAspect;
				}

				public VerifyDifferent executeAspect(int executeAspect) {
					this.executeAspect = executeAspect;
					return this;
				}

				public int getTapTableMap() {
					return getTapTableMap;
				}

				public VerifyDifferent getTapTableMap(int getTapTableMap) {
					this.getTapTableMap = getTapTableMap;
					return this;
				}
				public int getTable() {
					return getTable;
				}

				public VerifyDifferent getTable(int getTable) {
					this.getTable = getTable;
					return this;
				}
				public int tableGetId() {
					return tableGetId;
				}

				public VerifyDifferent tableGetId(int tableGetId) {
					this.tableGetId = tableGetId;
					return this;
				}
				public int batchIsOverOfTable() {
					return batchIsOverOfTable;
				}

				public VerifyDifferent batchIsOverOfTable(int batchIsOverOfTable) {
					this.batchIsOverOfTable = batchIsOverOfTable;
					return this;
				}
				public int obsLoggerInfo1() {
					return obsLoggerInfo1;
				}
				public VerifyDifferent obsLoggerInfo1(int obsLoggerInfo1) {
					this.obsLoggerInfo1 = obsLoggerInfo1;
					return this;
				}
				public int getBatchOffsetOfTable() {
					return getBatchOffsetOfTable;
				}
				public VerifyDifferent getBatchOffsetOfTable(int getBatchOffsetOfTable) {
					this.getBatchOffsetOfTable = getBatchOffsetOfTable;
					return this;
				}
				public int snapshotReadTableBeginAspect() {
					return snapshotReadTableBeginAspect;
				}
				public VerifyDifferent snapshotReadTableBeginAspect(int snapshotReadTableBeginAspect) {
					this.snapshotReadTableBeginAspect = snapshotReadTableBeginAspect;
					return this;
				}
				public int lockBySourceRunnerLock() {
					return lockBySourceRunnerLock;
				}
				public VerifyDifferent lockBySourceRunnerLock(int lockBySourceRunnerLock) {
					this.lockBySourceRunnerLock = lockBySourceRunnerLock;
					return this;
				}
				public int removeTablesContains() {
					return removeTablesContains;
				}
				public VerifyDifferent removeTablesContains(int removeTablesContains) {
					this.removeTablesContains = removeTablesContains;
					return this;
				}
				public int obsLoggerInfo2() {
					return obsLoggerInfo2;
				}
				public VerifyDifferent obsLoggerInfo2(int obsLoggerInfo2) {
					this.obsLoggerInfo2 = obsLoggerInfo2;
					return this;
				}
				public int removeTablesRemove() {
					return removeTablesRemove;
				}
				public VerifyDifferent removeTablesRemove(int removeTablesRemove) {
					this.removeTablesRemove = removeTablesRemove;
					return this;
				}
				public int obsLoggerInfo3() {
					return obsLoggerInfo3;
				}
				public VerifyDifferent obsLoggerInfo3(int obsLoggerInfo3) {
					this.obsLoggerInfo3 = obsLoggerInfo3;
					return this;
				}
				public int createPdkMethodInvoker() {
					return createPdkMethodInvoker;
				}
				public VerifyDifferent createPdkMethodInvoker(int createPdkMethodInvoker) {
					this.createPdkMethodInvoker = createPdkMethodInvoker;
					return this;
				}
				public int doAsyncTableCount() {
					return doAsyncTableCount;
				}
				public VerifyDifferent doAsyncTableCount(int doAsyncTableCount) {
					this.doAsyncTableCount = doAsyncTableCount;
					return this;
				}
				public int close() {
					return close;
				}
				public VerifyDifferent close(int close) {
					this.close = close;
					return this;
				}
				public int executeDataFuncAspect() {
					return executeDataFuncAspect;
				}
				public VerifyDifferent executeDataFuncAspect(int executeDataFuncAspect) {
					this.executeDataFuncAspect = executeDataFuncAspect;
					return this;
				}
			}

			void verifyAll(VerifyDifferent v) throws Exception  {
				verifyAll(v, false);
			}
			void verifyAll(VerifyDifferent v, boolean batchIsOverOfTable) throws Exception {
				try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
					bou.when(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId)).thenReturn(batchIsOverOfTable);
					bou.when(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId)).thenReturn(tableOffset);
					bou.when(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, null,  TableBatchReadStatus.OVER.name())).thenAnswer(a->null);
					Assertions.assertDoesNotThrow(() -> instance.doSnapshot(tableList));
					bou.verify(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId), times(v.batchIsOverOfTable()));
					bou.verify(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, null,  TableBatchReadStatus.OVER.name()), times(v.updateBatchOffset()));
					verifyAssert(v);
				}
			}
			@Test
			void testNormal() throws Exception {
				doCallRealMethod().when(instance).doSnapshot(tableList);
				VerifyDifferent v = new VerifyDifferent()
						.getProcessorBaseContext(0)
						.setDefaultRowSizeMap(0)
						.warn(0)
						.isRunning(4)
						.sourceRunnerFirstTimeGet(1)
						.stateINITIAL(1)
						.executeAspect(1)
						.getTapTableMap(1)
						.getTable(1)
						.tableGetId(1)
						.batchIsOverOfTable(1)
						.getBatchOffsetOfTable(1)
						.obsLoggerInfo1(0).obsLoggerInfo2(0).obsLoggerInfo3(1).obsLoggerInfo4(1)
						.snapshotReadTableBeginAspect(1)
						.lockBySourceRunnerLock(2).unlock(2)
						.removeTablesContains(1)
						.removeTablesRemove(0)
						.createPdkMethodInvoker(1)
//						.doAsyncTableCount(1)
						.close(1)
						.executeDataFuncAspect(1)
						.updateBatchOffset(1)
						.removePdkMethodInvoker(1)
						.snapshotReadTableEndAspect(1)
						.tapDataCompleteTableSnapshotEvent(1)
						.snapshotReadTableErrorAspect(0)
						.stateCOMPLETED(1)
						.newTablesIsEmpty(1).newTablesToArray(1)
						.endSnapshotLoopSet(0)
						.tapdataCompleteSnapshotEvent(1)
						.snapshotReadEndAspect(1);
				verifyAll(v);
			}
			@Test
			void testLastIsRunningIsFalse() throws Exception {
				VerifyDifferent v = new VerifyDifferent()
						.isRunning(4)
						.sourceRunnerFirstTimeGet(1)
						.stateINITIAL(1)
						.executeAspect(1)
						.getTapTableMap(1)
						.getTable(1)
						.tableGetId(1)
						.batchIsOverOfTable(1)
						.getBatchOffsetOfTable(1).obsLoggerInfo3(1).obsLoggerInfo4(1)
						.snapshotReadTableBeginAspect(1)
						.lockBySourceRunnerLock(2).unlock(2)
						.removeTablesContains(1)
						.createPdkMethodInvoker(1)
//						.doAsyncTableCount(1)
						.close(1)
						.executeDataFuncAspect(1)
						.updateBatchOffset(1)
						.removePdkMethodInvoker(1)
						.snapshotReadTableEndAspect(1)
						.tapDataCompleteTableSnapshotEvent(1)
						.stateCOMPLETED(1)
						.newTablesIsEmpty(1).newTablesToArray(1)
						.snapshotReadEndAspect(1);
				when(instance.isRunning()).thenReturn(true, true, false, false);
				doCallRealMethod().when(instance).doSnapshot(tableList);
				verifyAll(v);
			}
			@Test
			void testNewTablesIsEmpty() throws Exception {
				VerifyDifferent v = new VerifyDifferent()
						.isRunning(3)
						.sourceRunnerFirstTimeGet(1)
						.stateINITIAL(1)
						.executeAspect(1)
						.getTapTableMap(1)
						.getTable(1)
						.tableGetId(1)
						.batchIsOverOfTable(1)
						.getBatchOffsetOfTable(1)
						.obsLoggerInfo3(1).obsLoggerInfo4(1)
						.snapshotReadTableBeginAspect(1)
						.lockBySourceRunnerLock(2).unlock(2)
						.removeTablesContains(1)
						.createPdkMethodInvoker(1)
//						.doAsyncTableCount(1)
						.close(1)
						.executeDataFuncAspect(1)
						.updateBatchOffset(1)
						.removePdkMethodInvoker(1)
						.snapshotReadTableEndAspect(1)
						.tapDataCompleteTableSnapshotEvent(1)
						.stateCOMPLETED(1)
						.newTablesIsEmpty(1)
						.endSnapshotLoopSet(1)
						.tapdataCompleteSnapshotEvent(1)
						.snapshotReadEndAspect(1);
				when(instance.isRunning()).thenReturn(true, true, true);
				when(newTables.isEmpty()).thenReturn(true);
				doCallRealMethod().when(instance).doSnapshot(tableList);
				verifyAll(v);
			}

			@Test
			void testThrowableWhenExecuteDataFuncAspect() throws Exception {
				VerifyDifferent v = new VerifyDifferent()
						.isRunning(3)
						.sourceRunnerFirstTimeGet(1)
						.stateINITIAL(1)
						.executeAspect(1)
						.getTapTableMap(1)
						.getTable(1)
						.tableGetId(1)
						.batchIsOverOfTable(1)
						.getBatchOffsetOfTable(1)
						.obsLoggerInfo1(0).obsLoggerInfo2(0).obsLoggerInfo3(1)
						.snapshotReadTableBeginAspect(1)
						.lockBySourceRunnerLock(1).unlock(1)
						.removeTablesContains(1)
						.removeTablesRemove(0)
						.createPdkMethodInvoker(1)
//						.doAsyncTableCount(1)
						.close(1)
						.executeDataFuncAspect(1)
						.removePdkMethodInvoker(1)
						.snapshotReadTableErrorAspect(1)
						.stateCOMPLETED(1)
						.tapdataCompleteSnapshotEvent(1);
				when(instance.isRunning()).thenReturn(true, true, true);

				when(instance.executeDataFuncAspect(any(Class.class), any(Callable.class), any(CommonUtils.AnyErrorConsumer.class))).thenAnswer(a -> {
					Callable<?> callable = a.getArgument(1, Callable.class);
					CommonUtils.AnyErrorConsumer<BatchReadFuncAspect> errorConsumer = a.getArgument(2, CommonUtils.AnyErrorConsumer.class);
					Object call = callable.call();

					try(MockedStatic<PDKInvocationMonitor> pdk = mockStatic(PDKInvocationMonitor.class)) {
						pdk.when(() -> PDKInvocationMonitor.invoke(
								connectorNode,
								PDKMethod.SOURCE_BATCH_READ,
								pdkMethodInvoker)).thenAnswer(ans -> null);
						errorConsumer.accept(mock(BatchReadFuncAspect.class));
					}

					Assertions.assertNotNull(call);
					Assertions.assertEquals(BatchReadFuncAspect.class.getName(), call.getClass().getName());
					throw new IllegalArgumentException();
				});
				doCallRealMethod().when(instance).doSnapshot(tableList);
				try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
					bou.when(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId)).thenReturn(false);
					bou.when(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId)).thenReturn(tableOffset);
					bou.when(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, null,  TableBatchReadStatus.OVER.name())).thenAnswer(a->null);
					Assertions.assertThrows(TapCodeException.class, () -> {
						try{
							instance.doSnapshot(tableList);
						} catch (TapCodeException e) {
							Assertions.assertEquals(TaskProcessorExCode_11.UNKNOWN_ERROR, e.getCode());
							throw e;
						}
					});
					bou.verify(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId), times(v.batchIsOverOfTable()));
					bou.verify(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, null,  TableBatchReadStatus.OVER.name()), times(v.updateBatchOffset()));
				}
				verifyAssert(v);
			}

			@Test
			void testTapCodeExceptionWhenExecuteDataFuncAspect() throws Exception {
				VerifyDifferent v = new VerifyDifferent()
						.isRunning(3)
						.sourceRunnerFirstTimeGet(1)
						.stateINITIAL(1)
						.executeAspect(1)
						.getTapTableMap(1)
						.getTable(1)
						.tableGetId(1)
						.batchIsOverOfTable(1)
						.getBatchOffsetOfTable(1)
						.obsLoggerInfo1(0).obsLoggerInfo2(0).obsLoggerInfo3(1)
						.snapshotReadTableBeginAspect(1)
						.lockBySourceRunnerLock(1).unlock(1)
						.removeTablesContains(1)
						.removeTablesRemove(0)
						.createPdkMethodInvoker(1)
//						.doAsyncTableCount(1)
						.close(1)
						.executeDataFuncAspect(1)
						.removePdkMethodInvoker(1)
						.snapshotReadTableErrorAspect(1)
						.stateCOMPLETED(1)
						.tapdataCompleteSnapshotEvent(1);
				when(instance.isRunning()).thenReturn(true, true, true);

				when(instance.executeDataFuncAspect(any(Class.class), any(Callable.class), any(CommonUtils.AnyErrorConsumer.class))).thenAnswer(a -> {
					Callable<?> callable = a.getArgument(1, Callable.class);
					CommonUtils.AnyErrorConsumer<BatchReadFuncAspect> errorConsumer = a.getArgument(2, CommonUtils.AnyErrorConsumer.class);
					Object call = callable.call();

					try(MockedStatic<PDKInvocationMonitor> pdk = mockStatic(PDKInvocationMonitor.class)) {
						pdk.when(() -> PDKInvocationMonitor.invoke(
								connectorNode,
								PDKMethod.SOURCE_BATCH_READ,
								pdkMethodInvoker)).thenAnswer(ans -> null);
						errorConsumer.accept(mock(BatchReadFuncAspect.class));
					}

					Assertions.assertNotNull(call);
					Assertions.assertEquals(BatchReadFuncAspect.class.getName(), call.getClass().getName());
					throw new TapCodeException("");
				});
				doCallRealMethod().when(instance).doSnapshot(tableList);
				try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
					bou.when(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId)).thenReturn(false);
					bou.when(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, null,  TableBatchReadStatus.OVER.name())).thenAnswer(a->null);
					Assertions.assertThrows(TapCodeException.class, () -> {
						try {
							instance.doSnapshot(tableList);
						} catch (TapCodeException e) {
							Assertions.assertNotEquals(TaskProcessorExCode_11.UNKNOWN_ERROR, e.getCode());
							throw e;
						}
					});
					bou.verify(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId), times(v.batchIsOverOfTable()));
					bou.verify(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, null,  TableBatchReadStatus.OVER.name()), times(v.updateBatchOffset()));
				}

				verifyAssert(v);
			}

			@Test
			void testRemoveTablesIsNull() throws Exception {
				VerifyDifferent v = new VerifyDifferent()
						.isRunning(4)
						.sourceRunnerFirstTimeGet(1)
						.stateINITIAL(1)
						.executeAspect(1)
						.getTapTableMap(1)
						.getTable(1)
						.tableGetId(1)
						.batchIsOverOfTable(1)
						.getBatchOffsetOfTable(1)
						.obsLoggerInfo3(1).obsLoggerInfo4(1)
						.snapshotReadTableBeginAspect(1)
						.lockBySourceRunnerLock(2).unlock(2)
						.createPdkMethodInvoker(1)
//						.doAsyncTableCount(1)
						.close(1)
						.executeDataFuncAspect(1)
						.updateBatchOffset(1)
						.removePdkMethodInvoker(1)
						.snapshotReadTableEndAspect(1)
						.tapDataCompleteTableSnapshotEvent(1)
						.stateCOMPLETED(1)
						.newTablesIsEmpty(1).newTablesToArray(1)
						.tapdataCompleteSnapshotEvent(1)
						.snapshotReadEndAspect(1);
				ReflectionTestUtils.setField(instance, "removeTables", null);
				doCallRealMethod().when(instance).doSnapshot(tableList);
				verifyAll(v);
			}

			@Test
			void testRemoveTablesContainsId() throws Exception {
				VerifyDifferent v = new VerifyDifferent()
						.getProcessorBaseContext(0)
						.setDefaultRowSizeMap(0)
						.warn(0)
						.isRunning(4)
						.sourceRunnerFirstTimeGet(1)
						.stateINITIAL(1)
						.executeAspect(1)
						.getTapTableMap(1)
						.getTable(1)
						.obsLoggerInfo2(1)
						.tableGetId(1)
						.batchIsOverOfTable(1)
						.getBatchOffsetOfTable(1)
						.snapshotReadTableBeginAspect(1)
						.lockBySourceRunnerLock(2).unlock(2)
						.stateCOMPLETED(1)
						.newTablesIsEmpty(1).newTablesToArray(1)
						.endSnapshotLoopSet(0)
						.tapdataCompleteSnapshotEvent(1)
						.snapshotReadEndAspect(1)
						.removeTablesContains(1)
						.removeTablesRemove(1);
				when(removeTables.contains("id")).thenReturn(true);
				doCallRealMethod().when(instance).doSnapshot(tableList);
				Assertions.assertDoesNotThrow(() -> instance.doSnapshot(tableList));
				verifyAssert(v);
			}
			@Test
			void testSecIsRunningIsFalse() throws Exception {
				VerifyDifferent v = new VerifyDifferent()
						.getProcessorBaseContext(0)
						.setDefaultRowSizeMap(0)
						.warn(0)
						.isRunning(4)
						.sourceRunnerFirstTimeGet(1)
						.stateINITIAL(1)
						.executeAspect(1)
						.getTapTableMap(1)
						.getTable(1)
						.tableGetId(1)
						.batchIsOverOfTable(1)
						.getBatchOffsetOfTable(1)
						.snapshotReadTableBeginAspect(1)
						.lockBySourceRunnerLock(2).unlock(2)
						.stateCOMPLETED(1)
						.newTablesIsEmpty(1).newTablesToArray(1)
						.endSnapshotLoopSet(0)
						.tapdataCompleteSnapshotEvent(1)
						.snapshotReadEndAspect(1);
				when(instance.isRunning()).thenReturn(true, false, false, true);
				doCallRealMethod().when(instance).doSnapshot(tableList);
				verifyAll(v);
			}
			@Test
			void testSyncProgressBatchIsOverOfTableIsTrue() throws Exception {
				VerifyDifferent v = new VerifyDifferent()
						.isRunning(3)
						.sourceRunnerFirstTimeGet(1)
						.stateINITIAL(1)
						.executeAspect(1)
						.getTapTableMap(1)
						.getTable(1)
						.tableGetId(1)
						.batchIsOverOfTable(1)
						.obsLoggerInfo1(1)
						.lockBySourceRunnerLock(1).unlock(1)
						.stateCOMPLETED(1)
						.newTablesIsEmpty(1).newTablesToArray(1)
						.tapdataCompleteSnapshotEvent(1)
						.snapshotReadEndAspect(1);
				when(instance.isRunning()).thenReturn(true, false, true);
				doCallRealMethod().when(instance).doSnapshot(tableList);
				verifyAll(v, true);
			}

			@Test
			void testSourceRunnerFirstTimeGetIsFalse() throws Exception {
				VerifyDifferent v = new VerifyDifferent()
						.getProcessorBaseContext(0)
						.setDefaultRowSizeMap(0)
						.warn(0)
						.isRunning(4)
						.sourceRunnerFirstTimeGet(1)
						.stateINITIAL(0)
						.executeAspect(0)
						.getTapTableMap(1)
						.getTable(1)
						.tableGetId(1)
						.batchIsOverOfTable(1)
						.getBatchOffsetOfTable(1)
						.obsLoggerInfo1(0).obsLoggerInfo2(0).obsLoggerInfo3(1).obsLoggerInfo4(1)
						.snapshotReadTableBeginAspect(1)
						.lockBySourceRunnerLock(2).unlock(2)
						.removeTablesContains(1)
						.removeTablesRemove(0)
						.createPdkMethodInvoker(1)
//						.doAsyncTableCount(1)
						.close(1)
						.executeDataFuncAspect(1)
						.updateBatchOffset(1)
						.removePdkMethodInvoker(1)
						.snapshotReadTableEndAspect(1)
						.tapDataCompleteTableSnapshotEvent(1)
						.snapshotReadTableErrorAspect(0)
						.stateCOMPLETED(1)
						.newTablesIsEmpty(1).newTablesToArray(1)
						.endSnapshotLoopSet(0)
						.tapdataCompleteSnapshotEvent(1)
						.snapshotReadEndAspect(1);
				when(sourceRunnerFirstTime.get()).thenReturn(false);

				doCallRealMethod().when(instance).doSnapshot(tableList);
				verifyAll(v);
			}

			@Test
			void butchCountFunctionIsNull() throws Exception {
				VerifyDifferent v = new VerifyDifferent()
						.setDefaultRowSizeMap(1)
						.getProcessorBaseContext(0)
						.warn(1)
						.isRunning(4)
						.sourceRunnerFirstTimeGet(1)
						.stateINITIAL(1)
						.executeAspect(1)
						.getTapTableMap(1)
						.getTable(1)
						.tableGetId(1)
						.batchIsOverOfTable(1)
						.getBatchOffsetOfTable(1)
						.obsLoggerInfo1(0).obsLoggerInfo2(0).obsLoggerInfo3(1).obsLoggerInfo4(1)
						.snapshotReadTableBeginAspect(1)
						.lockBySourceRunnerLock(2).unlock(2)
						.removeTablesContains(1)
						.removeTablesRemove(0)
						.createPdkMethodInvoker(1)
//						.doAsyncTableCount(1)
						.close(1)
						.executeDataFuncAspect(1)
						.updateBatchOffset(1)
						.removePdkMethodInvoker(1)
						.snapshotReadTableEndAspect(1)
						.tapDataCompleteTableSnapshotEvent(1)
						.snapshotReadTableErrorAspect(0)
						.stateCOMPLETED(1)
						.newTablesIsEmpty(1).newTablesToArray(1)
						.endSnapshotLoopSet(0)
						.tapdataCompleteSnapshotEvent(1)
						.snapshotReadEndAspect(1);
				batchCountFunction = null;
				when(connectorFunctions.getBatchCountFunction()).thenReturn(batchCountFunction);
				when(instance.doAsyncTableCount(batchCountFunction, tableId)).thenReturn(ignoreTableCountCloseable);

				doCallRealMethod().when(instance).doSnapshot(tableList);
				try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
					bou.when(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId)).thenReturn(false);
					bou.when(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId)).thenReturn(tableOffset);
					bou.when(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, null,  TableBatchReadStatus.OVER.name())).thenAnswer(a->null);
					Assertions.assertDoesNotThrow(() -> instance.doSnapshot(tableList));
					bou.verify(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId), times(v.batchIsOverOfTable()));
					bou.verify(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId), times(v.getBatchOffsetOfTable()));
					bou.verify(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, null,  TableBatchReadStatus.OVER.name()), times(v.updateBatchOffset()));
					verifyAssert(v);
				}
			}

			@Test
			void testBatchReadFunctionIsNull() throws Exception {
				VerifyDifferent v = new VerifyDifferent().getProcessorBaseContext(1);
				when(connectorFunctions.getBatchReadFunction()).thenReturn(null);
				doCallRealMethod().when(instance).doSnapshot(tableList);
				try(MockedStatic<BatchOffsetUtil> bou = mockStatic(BatchOffsetUtil.class)) {
					bou.when(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId)).thenReturn(false);
					bou.when(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId)).thenReturn(tableOffset);
					bou.when(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, null,  TableBatchReadStatus.OVER.name())).thenAnswer(a->null);
					Assertions.assertThrows(NodeException.class, () -> instance.doSnapshot(tableList));
					bou.verify(() -> BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId), times(v.batchIsOverOfTable()));
					bou.verify(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId), times(v.getBatchOffsetOfTable()));
					bou.verify(() -> BatchOffsetUtil.updateBatchOffset(syncProgress, tableId, null,  TableBatchReadStatus.OVER.name()), times(v.updateBatchOffset()));
					verifyAssert(v);
				}
			}

			@Nested
			class AnyErrorTest {

				@BeforeEach
				void init() {
					when(pdkMethodInvoker.runnable(any(CommonUtils.AnyError.class))).thenAnswer(a -> {
						CommonUtils.AnyError argument = a.getArgument(0, CommonUtils.AnyError.class);
						argument.run();
						return pdkMethodInvoker;
					});
				}

				@Test
				void testAnyError() {
					doCallRealMethod().when(instance).doSnapshot(tableList);
					//Assertions.assertDoesNotThrow(() -> instance.doSnapshot(tableList));
				}
			}
		}

		@Nested
		@DisplayName("Method lockBySourceRunnerLock test")
		class LockBySourceRunnerLockTest {
			@BeforeEach
			void init() {
				when(instance.isRunning()).thenReturn(true, false);
				doCallRealMethod().when(instance).lockBySourceRunnerLock();
			}

			@Test
			void testNormal() throws InterruptedException {
				when(sourceRunnerLock.tryLock(1L, TimeUnit.SECONDS)).thenReturn(true);
				Assertions.assertDoesNotThrow(() -> instance.lockBySourceRunnerLock());
				verify(instance, times(1)).isRunning();
				verify(sourceRunnerLock, times(1)).tryLock(1L, TimeUnit.SECONDS);
			}

			@Test
			void testNotBreak() throws InterruptedException {
				when(sourceRunnerLock.tryLock(1L, TimeUnit.SECONDS)).thenReturn(false);
				Assertions.assertDoesNotThrow(() -> instance.lockBySourceRunnerLock());
				verify(instance, times(2)).isRunning();
				verify(sourceRunnerLock, times(1)).tryLock(1L, TimeUnit.SECONDS);
			}

			@Test
			void testInterruptedException() throws InterruptedException {
				when(sourceRunnerLock.tryLock(1L, TimeUnit.SECONDS)).then(a-> {
					throw new InterruptedException("");
				});
				Assertions.assertDoesNotThrow(() -> instance.lockBySourceRunnerLock());
			}
		}
		@Nested
		@DisplayName("Method unLockBySourceRunnerLock test")
		class UlLockBySourceRunnerLockTest {
			Exception e;
			@BeforeEach
			void init() {
				e = new Exception("exp");
				when(obsLogger.isDebugEnabled()).thenReturn(true);
				doNothing().when(obsLogger).debug("An error when sourceRunnerLock.unlock(), message: {}", "exp", e);
				doCallRealMethod().when(instance).unLockBySourceRunnerLock();
			}

			@Test
			void testNormal() {
				doNothing().when(sourceRunnerLock).unlock();
				Assertions.assertDoesNotThrow(() -> instance.unLockBySourceRunnerLock());
				verify(sourceRunnerLock, times(1)).unlock();
				verify(obsLogger, times(0)).isDebugEnabled();
				verify(obsLogger, times(0)).debug("An error when sourceRunnerLock.unlock(), message: {}", "exp", e);
			}

			@Test
			void testException() {
				doAnswer(a -> {
					throw e;
				}).when(sourceRunnerLock).unlock();
				Assertions.assertDoesNotThrow(() -> instance.unLockBySourceRunnerLock());
				verify(sourceRunnerLock, times(1)).unlock();
				verify(obsLogger, times(1)).isDebugEnabled();
				verify(obsLogger, times(1)).debug("An error when sourceRunnerLock.unlock(), message: {}", "exp", e);
			}
			@Test
			void testExceptionButNotSupportDebug() {
				when(obsLogger.isDebugEnabled()).thenReturn(false);
				doAnswer(a -> {
					throw e;
				}).when(sourceRunnerLock).unlock();
				Assertions.assertDoesNotThrow(() -> instance.unLockBySourceRunnerLock());
				verify(sourceRunnerLock, times(1)).unlock();
				verify(obsLogger, times(1)).isDebugEnabled();
				verify(obsLogger, times(0)).debug("An error when sourceRunnerLock.unlock(), message: {}", "exp", e);
			}
		}
	}

	@Nested
	class timeTransformationTest{
		@DisplayName("Query the data of the previous day")
		@Test
		void test(){
			List<QueryOperator> conditions = new ArrayList<>();
			QueryOperator queryOperator = new QueryOperator();
			queryOperator.setFastQuery(true);
			queryOperator.setForm(TapTimeForm.BEFORE);
			queryOperator.setUnit(TapTimeUnit.HOUR);
			queryOperator.setNumber(1L);
			conditions.add(queryOperator);
			List<QueryOperator> result =  hazelcastSourcePdkDataNode.timeTransformation(conditions,null);
			Assertions.assertEquals(result.size(),2);
			Assertions.assertEquals(result.get(0).getOperator(),2);
			Assertions.assertEquals(result.get(1).getOperator(),4);
		}
		@DisplayName("Specify query conditions")
		@Test
		void test1(){
			List<QueryOperator> conditions = new ArrayList<>();
			QueryOperator queryOperator = new QueryOperator();
			queryOperator.setFastQuery(false);
			conditions.add(queryOperator);
			List<QueryOperator> result =  hazelcastSourcePdkDataNode.timeTransformation(conditions,null);
			Assertions.assertEquals(result.size(),1);
		}
	}
	@Nested
	class constructQueryOperatorTest{
		@DisplayName("timeList is null")
		@Test
		void test(){
			List<QueryOperator> result = hazelcastSourcePdkDataNode.constructQueryOperator(null,new QueryOperator());
			Assertions.assertEquals(0,result.size());
		}

		@DisplayName("Main process")
		@Test
		void test1(){
			List<String> timeList = new ArrayList<>();
			timeList.add("test1");
			timeList.add("test2");
			List<QueryOperator> result = hazelcastSourcePdkDataNode.constructQueryOperator(timeList,new QueryOperator());
			Assertions.assertEquals(2,result.size());
		}
	}
	@Nested
	@Disabled
	class StreamReadConsumerStateListenerTest{
		private ConnectorNode connectorNode;
		@BeforeEach
		void before(){
			connectorNode = mock(ConnectorNode.class);
			TapNodeInfo tapNodeInfo = mock(TapNodeInfo.class);
			when(connectorNode.getTapNodeInfo()).thenReturn(tapNodeInfo);
			TapNodeSpecification tapNodeSpecification = mock(TapNodeSpecification.class);
			when(tapNodeInfo.getTapNodeSpecification()).thenReturn(tapNodeSpecification);
			when(tapNodeSpecification.getName()).thenReturn("123");
		}
		@DisplayName("test stateListener STATE_STREAM_READ_STARTED")
		@Test
		void test1(){
			try (MockedStatic<PDKInvocationMonitor> pdkInvocationMonitorMockedStatic = mockStatic(PDKInvocationMonitor.class)) {
				ObsLogger obsLogger = mock(ObsLogger.class);
				ReflectionTestUtils.setField(hazelcastSourcePdkDataNode, "obsLogger", obsLogger);
				doNothing().when(obsLogger).trace(anyString(), any(), any());
				PDKMethodInvoker pdkMethodInvoker = mock(PDKMethodInvoker.class);
				pdkInvocationMonitorMockedStatic.when(() -> PDKInvocationMonitor.invokerRetrySetter(pdkMethodInvoker)).thenAnswer((invocationOnMock) -> {
					PDKMethodInvoker pdkMethodArgs = (PDKMethodInvoker) invocationOnMock.getArgument(0);
					assertEquals(pdkMethodInvoker, pdkMethodArgs);
					return null;
				});
				StreamReadConsumer streamReadConsumer = hazelcastSourcePdkDataNode.generateStreamReadConsumer(connectorNode, pdkMethodInvoker);
				streamReadConsumer.streamReadStarted();
				verify(obsLogger, times(1)).trace(anyString(), any(), any());
			}
		}

	}

	@Nested
	class doBatchCountFunction {
		long expectedValue = 99;
		TapTable testTable = new TapTable("testTable");
		BatchCountFunction mockBatchCountFunction = mock(BatchCountFunction.class);

		@BeforeEach
		void init(){
			TaskConfig taskConfig = TaskConfig.create();
			taskConfig.taskRetryConfig(TaskRetryConfig.create());
			taskConfig.getTaskRetryConfig().retryIntervalSecond(1000L);
			when(dataProcessorContext.getTaskConfig()).thenReturn(taskConfig);

		}

		@Test
		@SneakyThrows
		@DisplayName("Exception test")
		void testException() {

			when(dataProcessorContext.getNode()).thenReturn((Node) new DatabaseNode());

			HazelcastSourcePdkBaseNode spyInstance = Mockito.spy(hazelcastSourcePdkDataNode);
			doReturn(new ConnectorNode()).when(spyInstance).getConnectorNode();
			when(mockBatchCountFunction.count(any(), any())).thenThrow(new Exception());

			try {
				spyInstance.doBatchCountFunction(mockBatchCountFunction, testTable);
			} catch (Exception e) {
				assertTrue(null != e.getCause() && null != e.getCause().getCause() && (e.getCause().getCause() instanceof NodeException), e.getMessage());
			}
		}

		@Test
		@SneakyThrows
		@DisplayName("Normal  BatchCount test")
		void testNormalBatchCount() {

			when(dataProcessorContext.getNode()).thenReturn((Node) new TableNode());
			HazelcastSourcePdkBaseNode spyInstance = Mockito.spy(hazelcastSourcePdkDataNode);
			doReturn(new ConnectorNode()).when(spyInstance).getConnectorNode();
			when(mockBatchCountFunction.count(any(), any())).thenReturn(expectedValue);


			long actualData = spyInstance.doBatchCountFunction(mockBatchCountFunction, testTable);
			Assertions.assertTrue(expectedValue == actualData);
		}


		@Test
		@SneakyThrows
		@DisplayName("Normal  BatchCount WithDataNode test")
		void testNormalBatchCountWithDataNode() {

			when(dataProcessorContext.getNode()).thenReturn((Node) new DatabaseNode());
			HazelcastSourcePdkBaseNode spyInstance = Mockito.spy(hazelcastSourcePdkDataNode);
			doReturn(new ConnectorNode()).when(spyInstance).getConnectorNode();
			when(mockBatchCountFunction.count(any(), any())).thenReturn(expectedValue);


			long actualData = spyInstance.doBatchCountFunction(mockBatchCountFunction, testTable);
			Assertions.assertTrue(expectedValue == actualData);
		}
		@Test
		@SneakyThrows
		@DisplayName("Filter BatchCount test")
		void testFilterBatchCount() {

			TableNode testTableTemp = new TableNode();
			testTableTemp.setIsFilter(true);
			testTableTemp.setTableName("testTable");
			List<QueryOperator> conditions = new ArrayList<>();
			QueryOperator queryOperator = new QueryOperator();
			queryOperator.setKey("id");
			queryOperator.setValue("1");
			queryOperator.setOperator(5);

			conditions.add(queryOperator);
			QueryOperator queryOperator1 = new QueryOperator();
			queryOperator1.setKey("created");
			queryOperator1.setValue("2025-07-01 00:00:00");
			queryOperator1.setOperator(1);
			conditions.add(queryOperator1);
			testTableTemp.setConditions(conditions);
			when(dataProcessorContext.getNode()).thenReturn((Node) testTableTemp);
			HazelcastSourcePdkBaseNode spyInstance = Mockito.spy(hazelcastSourcePdkDataNode);
			ConnectorNode connectorNode = new ConnectorNode();
			ConnectorFunctions connectorFunctions = new ConnectorFunctions();
			CountByPartitionFilterFunction countByPartitionFilterFunction = mock(CountByPartitionFilterFunction.class);
			connectorFunctions.supportCountByPartitionFilterFunction(countByPartitionFilterFunction);
			ReflectionTestUtils.setField(connectorNode, "connectorFunctions", connectorFunctions);
			doReturn(connectorNode).when(spyInstance).getConnectorNode();
			LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap<>();
			TapField tapField = new TapField();
			tapField.setTapType(new TapString());
			nameFieldMap.put("id",tapField);
			TapField tapField1 = new TapField();
			tapField1.setTapType(new TapDate());
			nameFieldMap.put("created",tapField1);
			testTable.setNameFieldMap(nameFieldMap);
			TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("testTable", testTable);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			when(countByPartitionFilterFunction.countByPartitionFilter(any(),any(),any())).thenReturn(expectedValue);

			long actualData = spyInstance.doBatchCountFunction(mockBatchCountFunction, testTable);
			Assertions.assertTrue(expectedValue == actualData);

		}


		@Test
		@SneakyThrows
		@DisplayName("Filter filterBatchCount  not support")
		void testFilterBatchCountNotSupport() {

			TableNode testTableTemp = new TableNode();
			testTableTemp.setIsFilter(true);
			testTableTemp.setTableName("testTable");
			List<QueryOperator> conditions = new ArrayList<>();
			QueryOperator queryOperator = new QueryOperator();
			queryOperator.setKey("id");
			queryOperator.setValue("1");
			queryOperator.setOperator(5);

			conditions.add(queryOperator);
			testTableTemp.setConditions(conditions);
			when(dataProcessorContext.getNode()).thenReturn((Node) testTableTemp);
			HazelcastSourcePdkBaseNode spyInstance = Mockito.spy(hazelcastSourcePdkDataNode);
			ConnectorNode connectorNode = new ConnectorNode();
			ConnectorFunctions connectorFunctions = new ConnectorFunctions();
			ReflectionTestUtils.setField(connectorNode, "connectorFunctions", connectorFunctions);
			doReturn(connectorNode).when(spyInstance).getConnectorNode();
			when(mockBatchCountFunction.count(any(), any())).thenReturn(expectedValue);

			long actualData = spyInstance.doBatchCountFunction(mockBatchCountFunction, testTable);
			Assertions.assertTrue(expectedValue == actualData);

		}

	}

	@Nested
	class createTargetIndexTest{
		private List<String> updateConditionFields;
		private boolean createUnique;
		private String tableId;
		private TapTable tapTable;
		private ClientMongoOperator clientMongoOperator;
		@BeforeEach
		void beforeEach(){
			updateConditionFields = new ArrayList<>();
			updateConditionFields.add("field");
			createUnique = true;
			tableId = "test";
			tapTable = mock(TapTable.class);
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode,"obsLogger",mockObsLogger);
			clientMongoOperator = mock(ClientMongoOperator.class);
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode,"clientMongoOperator",clientMongoOperator);
		}
		@Test
		@DisplayName("test createTargetIndex method for build error consumer")
		void test1(){
			try (MockedStatic<TapInterfaceUtil> mb = Mockito
					.mockStatic(TapInterfaceUtil.class)) {
				Connections connections = mock(Connections.class);
				when(dataProcessorContext.getConnections()).thenReturn(connections);
				when(connections.getDatabase_type()).thenReturn("test");
				mb.when(()->TapInterfaceUtil.getTapInterface("test",null)).thenReturn(null);
				ConnectorNode connectorNode = mock(ConnectorNode.class);
				when(hazelcastSourcePdkDataNode.getConnectorNode()).thenReturn(connectorNode);
				ConnectorFunctions functions = mock(ConnectorFunctions.class);
				when(connectorNode.getConnectorFunctions()).thenReturn(functions);
				when(functions.getCreateIndexFunction()).thenReturn(mock(CreateIndexFunction.class));
				ArrayList<String> pks = new ArrayList<>();
				when(tapTable.primaryKeys()).thenReturn(pks);
				when(hazelcastSourcePdkDataNode.usePkAsUpdateConditions(updateConditionFields,pks)).thenReturn(false);
				doCallRealMethod().when(hazelcastSourcePdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
				try (MockedStatic<SchemaProxy> schemaProxyMockedStatic = Mockito
						.mockStatic(SchemaProxy.class)) {
					schemaProxyMockedStatic.when(SchemaProxy::getSchemaProxy).thenReturn(mock(SchemaProxy.class));
					hazelcastSourcePdkDataNode.createTargetIndex(updateConditionFields,createUnique,tableId,tapTable);
					verify(hazelcastSourcePdkDataNode,new Times(1)).buildErrorConsumer(tableId);
				}
			}
		}
	}
	@Nested
	class DoShareCdcTest{
		class HazelcastSourcePdkDataNodeTestNested extends HazelcastSourcePdkDataNode{
			public HazelcastSourcePdkDataNodeTestNested(DataProcessorContext dataProcessorContext) {
				super(dataProcessorContext);
			}
			@Override
			public void doShareCdc() throws Exception {
				super.doShareCdc();
			}

			@Override
			protected boolean isRunning() {
				return super.isRunning();
			}
		}
		class TestTapTableMap extends TapTableMap<String, TapTable> {
			public TestTapTableMap(String nodeId, long time, Map<String, String> tableNameAndQualifiedNameMap) {
				super(nodeId, time, tableNameAndQualifiedNameMap);
			}
		}

		private HazelcastSourcePdkDataNodeTestNested hazelcastSourcePdkDataNode;
		private ICdcDelay cdcDelay;
		@BeforeEach
		void setUp(){
			hazelcastSourcePdkDataNode = mock(HazelcastSourcePdkDataNodeTestNested.class);

			Map<String, String> tableNameAndQualifiedNameMap =new HashMap<>();
			tableNameAndQualifiedNameMap.put("testTable","QualifiedNameTestTable");
			TapTableMap<String, TapTable> tapTableMap=new TestTapTableMap(",",System.currentTimeMillis(),tableNameAndQualifiedNameMap);
			TapTable tapTable=new TapTable();
			tapTableMap.put("testTable",tapTable);

			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode,"dataProcessorContext",dataProcessorContext);
			when(hazelcastSourcePdkDataNode.isRunning()).thenReturn(true);
			cdcDelay=mock(CdcDelay.class);
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode,"cdcDelayCalculation", cdcDelay);

		}
		@DisplayName("test do ShareCdc addHeartbeatTable")
		@Test
		void test1() throws Exception {
			doCallRealMethod().when(hazelcastSourcePdkDataNode).doShareCdc();
			ShareCdcTaskContext shareCdcTaskContext = mock(ShareCdcTaskContext.class);
			when(hazelcastSourcePdkDataNode.createShareCDCTaskContext()).thenReturn(shareCdcTaskContext);
			hazelcastSourcePdkDataNode.doShareCdc();
			verify(cdcDelay,times(1)).addHeartbeatTable(any());
		}
	}

	@Nested
	class generateStreamReadConsumerTest{
		class HazelcastSourceStreamReadNested extends HazelcastSourcePdkDataNode{
			public HazelcastSourceStreamReadNested(DataProcessorContext dataProcessorContext) {
				super(dataProcessorContext);
			}

			@Override
			protected boolean isRunning() {
				return super.isRunning();
			}
		}
		private HazelcastSourceStreamReadNested hazelcastSourcePdkDataNode;
		private CdcDelay cdcDelay;
		private SyncProgress syncProgress;
		@BeforeEach
		void setUp(){
			hazelcastSourcePdkDataNode = mock(HazelcastSourceStreamReadNested.class);
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode,"dataProcessorContext",dataProcessorContext);
			cdcDelay=mock(CdcDelay.class);
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode,"cdcDelayCalculation", cdcDelay);
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode,"logger",mock(Logger.class));
			syncProgress=new SyncProgress();
		}
		@DisplayName("test generateStreamReadConsumer")
		@Test
		void test1() throws InterruptedException {
			PDKMethodInvoker pdkMethodInvoker = mock(PDKMethodInvoker.class);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastSourcePdkDataNode.isRunning()).thenReturn(true);
			ReentrantLock reentrantLock = mock(ReentrantLock.class);
			when(reentrantLock.tryLock(1L, TimeUnit.SECONDS)).thenReturn(true);
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode,"sourceRunnerLock",reentrantLock);
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode,"cdcDelayCalculation", cdcDelay);
			ReflectionTestUtils.setField(hazelcastSourcePdkDataNode,"syncProgress", syncProgress);
			List<TapEvent> tapEvents = new ArrayList<>();
			TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent();
			tapUpdateRecordEvent.setTableId("testTableId");
			tapUpdateRecordEvent.setTime(System.currentTimeMillis());
			tapEvents.add(tapUpdateRecordEvent);
			doCallRealMethod().when(hazelcastSourcePdkDataNode).generateStreamReadConsumer(any(),any());
			StreamReadConsumer streamReadConsumer = hazelcastSourcePdkDataNode.generateStreamReadConsumer(connectorNode,pdkMethodInvoker);
			streamReadConsumer.accept(tapEvents,null);
			verify(cdcDelay,times(1)).filterAndCalcDelay(any(),any());
		}
	}

	@Nested
	class addLdpNewTablesIfNeedTest {
		@DisplayName("test addLdpNewTables when new table is not empty")
		@Test
		void test1(){
			TaskDto taskDto1=new TaskDto();
			List<String> ldpNewTables = new ArrayList<>();
			ldpNewTables.add("testAddTable");
			taskDto1.setLdpNewTables(ldpNewTables);
			HazelcastSourcePdkDataNode hazelcastSourcePdkDataNode1 = mock(HazelcastSourcePdkDataNode.class);
			doCallRealMethod().when(hazelcastSourcePdkDataNode1).addLdpNewTablesIfNeed(taskDto1);
			hazelcastSourcePdkDataNode1.addLdpNewTablesIfNeed(taskDto1);
			Object addLdpNewTablesFlag = ReflectionTestUtils.getField(hazelcastSourcePdkDataNode1, "addLdpNewTables");
			assertEquals(true, addLdpNewTablesFlag);
		}
		@DisplayName("test addLdpNewTables when new table is empty")
		@Test
		void test2(){
			TaskDto taskDto1=new TaskDto();
			List<String> ldpNewTables = new ArrayList<>();
			taskDto1.setLdpNewTables(ldpNewTables);
			HazelcastSourcePdkDataNode hazelcastSourcePdkDataNode1 = mock(HazelcastSourcePdkDataNode.class);
			doCallRealMethod().when(hazelcastSourcePdkDataNode1).addLdpNewTablesIfNeed(taskDto1);
			hazelcastSourcePdkDataNode1.addLdpNewTablesIfNeed(taskDto1);
			Object addLdpNewTablesFlag = ReflectionTestUtils.getField(hazelcastSourcePdkDataNode1, "addLdpNewTables");
			assertEquals(false, addLdpNewTablesFlag);
		}
	}

	@Nested
	class testFilterSubTableIfMasterExists{

		private HazelcastSourcePdkDataNode sourceDataNode;

		@BeforeEach
		public void beforeEach() {
			DataProcessorContext context = mock(DataProcessorContext.class);
			TaskDto taskDto = new TaskDto();
			taskDto.setId(new ObjectId());
			taskDto.setSyncType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			when(context.getTaskDto()).thenReturn(taskDto);

			TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("nodeId");
			TapTable tapTable = new TapTable();
			tapTable.setId("test");
			tapTable.setName("test");
			TapPartition partitionInfo = new TapPartition();
			partitionInfo.setSubPartitionTableInfo(new ArrayList<>());
			partitionInfo.getSubPartitionTableInfo().add(new TapSubPartitionTableInfo());
			tapTable.setPartitionInfo(partitionInfo);
			tapTableMap.putNew("test", tapTable, "test");

			tapTable = new TapTable();
			tapTable.setId("test_1");
			tapTable.setName("test_1");
			tapTable.setPartitionMasterTableId("test");
			tapTable.setPartitionInfo(partitionInfo);
			tapTableMap.putNew("test_1", tapTable, "test_1");

			tapTable = new TapTable();
			tapTable.setId("test_table");
			tapTable.setName("test_table");
			tapTableMap.putNew("test_table", tapTable, "test_table");

			when(context.getTapTableMap()).thenReturn(tapTableMap);

			Node node = new DatabaseNode();
			node.setId("nodeId");
			node.setDisabled(false);
			when(context.getNode()).thenReturn(node);

			sourceDataNode = new HazelcastSourcePdkDataNode(context);
			SyncProgress syncProgress = new SyncProgress();
			Map batchOffsetObj = (Map) syncProgress.getBatchOffsetObj();
			batchOffsetObj.put("test", new HashMap<String, Object>(){{
				put("batch_read_connector_offset", true);
			}});
			batchOffsetObj.put("user_tbl", new HashMap<String, Object>(){{
				put("batch_read_connector_offset", true);
			}});
			ReflectionTestUtils.setField(sourceDataNode, "syncProgress", syncProgress);
		}

		@Test
		public void testNotEnableSyncPartitionTable () throws Exception {
			ReflectionTestUtils.setField(sourceDataNode, "syncSourcePartitionTableEnable", null);
			Set<String> tableNames = sourceDataNode.filterSubTableIfMasterExists();
			Assertions.assertNotNull(tableNames);
			Assertions.assertEquals(3, tableNames.size());
			Assertions.assertTrue(tableNames.contains("test"));
		}

		@Test
		public void testEnableSyncPartitionTable () throws Exception {
			ReflectionTestUtils.setField(sourceDataNode, "syncSourcePartitionTableEnable", Boolean.TRUE);

			HazelcastSourcePdkDataNode spySourceDataNode = spy(sourceDataNode);
			when(spySourceDataNode.handleNewTables(anyList())).thenReturn(true);

			Set<String> tableNames = spySourceDataNode.filterSubTableIfMasterExists();
			Assertions.assertNotNull(tableNames);
			Assertions.assertEquals(2, tableNames.size());
			Assertions.assertTrue(tableNames.contains("test"));
		}
	}

	@Test
	public void testGetTerminatedMode() {
		DataProcessorContext context = mock(DataProcessorContext.class);
		TaskDto taskDto = new TaskDto();
		taskDto.setId(new ObjectId());
		taskDto.setSyncType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		when(context.getTaskDto()).thenReturn(taskDto);

		HazelcastSourcePdkDataNode sourceDataNode = new HazelcastSourcePdkDataNode(context);

		Assertions.assertDoesNotThrow(() -> {
			TerminalMode mode = sourceDataNode.getTerminatedMode();
			Assertions.assertNull(mode);
		});

		ConfigurableApplicationContext applicationContext = mock(ConfigurableApplicationContext.class);
		BeanUtil.configurableApplicationContext = applicationContext;
		TapdataTaskScheduler taskScheduler = mock(TapdataTaskScheduler.class);
		when(applicationContext.getBean(TapdataTaskScheduler.class)).thenReturn(taskScheduler);

		TaskClient<TaskDto> taskClient = mock(TaskClient.class);
		when(taskScheduler.getTaskClient(anyString())).thenReturn(taskClient);

		when(taskClient.getTerminalMode()).thenReturn(TerminalMode.COMPLETE);

		/*Assertions.assertDoesNotThrow(() -> {
			TerminalMode model = sourceDataNode.getTerminatedMode();
			Assertions.assertNotNull(model);
            assertSame(model, TerminalMode.COMPLETE);
		});*/

	}

	@Test
	public void testInitPartitionMap() {

		DataProcessorContext context = mock(DataProcessorContext.class);
		TaskDto taskDto = new TaskDto();
		taskDto.setId(new ObjectId());
		taskDto.setSyncType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		when(context.getTaskDto()).thenReturn(taskDto);
		HazelcastSourcePdkDataNode sourceDataNode = new HazelcastSourcePdkDataNode(context);

		Assertions.assertDoesNotThrow(() -> {
			ReflectionTestUtils.setField(sourceDataNode, "syncSourcePartitionTableEnable", Boolean.FALSE);
			sourceDataNode.initPartitionMap();
			ReflectionTestUtils.setField(sourceDataNode, "syncSourcePartitionTableEnable", null);
			sourceDataNode.initPartitionMap();
		});

		HazelcastSourcePdkDataNode spySourceDataNode = spy(sourceDataNode);
		ConnectorNode connectorNode = mock(ConnectorNode.class);

		TapConnectorContext connectorContext = mock(TapConnectorContext.class);
		when(connectorContext.getTableMap()).thenReturn(new KVReadOnlyMap<TapTable>() {
			@Override
			public TapTable get(String key) {
				TapTable table = new TapTable();
				table.setId(key);
				table.setName(key);
				return table;
			}

			@Override
			public Iterator<Entry<TapTable>> iterator() {
				AtomicInteger counter = new AtomicInteger(0);

				Iterator<Entry<TapTable>> iterator = new Iterator<Entry<TapTable>>() {

					@Override
					public boolean hasNext() {
						return counter.incrementAndGet() < 10;
					}

					@Override
					public Entry<TapTable> next() {
						TapTable table = new TapTable();
						table.setId("test_" + counter.get());
						table.setName("test_" + counter.get());

						if (counter.get() %2 == 0) {
							table.setPartitionMasterTableId(table.getId());
							table.setPartitionInfo(new TapPartition());
							List<TapSubPartitionTableInfo> subPartitionInfo = new ArrayList<>();

							TapSubPartitionTableInfo partitionInfo = new TapSubPartitionTableInfo();
							partitionInfo.setTableName(table.getId() + "_1");
							subPartitionInfo.add(partitionInfo);
							partitionInfo = new TapSubPartitionTableInfo();
							partitionInfo.setTableName(table.getId() + "_2");
							subPartitionInfo.add(partitionInfo);

							table.getPartitionInfo().setSubPartitionTableInfo(subPartitionInfo);

						}

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
				return iterator;
			}
		});
		when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
		when(spySourceDataNode.getConnectorNode()).thenReturn(connectorNode);

		sourceDataNode.syncSourcePartitionTableEnable = Boolean.TRUE;
		spySourceDataNode.syncSourcePartitionTableEnable = Boolean.TRUE;
		spySourceDataNode.initPartitionMap();

		Assertions.assertNotNull(sourceDataNode.partitionTableSubMasterMap);
		Assertions.assertEquals(8, sourceDataNode.partitionTableSubMasterMap.keySet().size());

	}

}

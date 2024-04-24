package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataCompleteSnapshotEvent;
import com.tapdata.entity.TapdataCompleteTableSnapshotEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.TableBatchReadStatus;
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.SourceStateAspect;
import io.tapdata.aspect.taskmilestones.SnapshotReadBeginAspect;
import io.tapdata.aspect.taskmilestones.SnapshotReadEndAspect;
import io.tapdata.aspect.taskmilestones.SnapshotReadTableBeginAspect;
import io.tapdata.aspect.taskmilestones.SnapshotReadTableEndAspect;
import io.tapdata.aspect.taskmilestones.SnapshotReadTableErrorAspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import com.tapdata.tm.commons.dag.nodes.TableNode;
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
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.NodeException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableMap;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
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
			verify(hazelcastSourcePdkDataNode, times(1)).fromTapValue(any(Map.class), eq(tapCodecsFilterManager), anyString());
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
			verify(hazelcastSourcePdkDataNode, times(1)).fromTapValue(any(Map.class), eq(tapCodecsFilterManager), anyString());
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
			verify(hazelcastSourcePdkDataNode, times(1)).fromTapValue(any(Map.class), eq(tapCodecsFilterManager), anyString());
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
		TapTableMap<String, TapTable>  tapTableMap =TapTableMap.create("test",tapTable);

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
			void init() throws Exception {
				tableId = "id";
				tableList = new ArrayList<>();
				tableList.add(tableId);

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
				verify(obsLogger, times(v.obsLoggerInfo1())).info("Skip table [{}] in batch read, reason: last task, this table has been completed batch read", "id");
				verify(instance, times(v.snapshotReadTableBeginAspect())).executeAspect(any(SnapshotReadTableBeginAspect.class));
				verify(instance, times(v.lockBySourceRunnerLock())).lockBySourceRunnerLock();
				verify(removeTables, times(v.removeTablesContains())).contains(tableId);
				verify(removeTables, times(v.removeTablesRemove())).remove(tableId);
				verify(obsLogger, times(v.obsLoggerInfo2())).info("Table {} is detected that it has been removed, the snapshot read will be skipped", "id");
				verify(obsLogger, times(v.obsLoggerInfo3())).info("Starting batch read, table name: {}, offset: {}", "id", tableOffset);
				verify(instance, times(v.createPdkMethodInvoker())).createPdkMethodInvoker();
				verify(instance, times(v.doAsyncTableCount())).doAsyncTableCount(batchCountFunction, tableId);
				verify(ignoreTableCountCloseable, times(v.close())).close();
				verify(instance,times(v.executeDataFuncAspect())).executeDataFuncAspect(any(Class.class), any(Callable.class), any(CommonUtils.AnyErrorConsumer.class));
				verify(obsLogger, times(v.obsLoggerInfo4())).info("Table [{}] has been completed batch read, will skip batch read on the next run", "id");
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
					bou.verify(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId), times(v.getBatchOffsetOfTable()));
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
						.doAsyncTableCount(1)
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
						.doAsyncTableCount(1)
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
						.doAsyncTableCount(1)
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
						.doAsyncTableCount(1)
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
					bou.verify(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId), times(v.getBatchOffsetOfTable()));
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
						.doAsyncTableCount(1)
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
					bou.when(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId)).thenReturn(tableOffset);
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
					bou.verify(() -> BatchOffsetUtil.getBatchOffsetOfTable(syncProgress, tableId), times(v.getBatchOffsetOfTable()));
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
						.doAsyncTableCount(1)
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
						.doAsyncTableCount(1)
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
						.doAsyncTableCount(1)
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
		TapTableMap<String, TapTable>  tapTableMap =TapTableMap.create("test",tapTable);

		when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
		ReflectionTestUtils.invokeMethod(hazelcastSourcePdkDataNode,"batchFilterRead");

	}
}

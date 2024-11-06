package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.BaseTaskTest;
import com.tapdata.constant.ConnectorContext;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.ExistsDataProcessEnum;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.MockTaskUtil;
import io.tapdata.aspect.TableInitFuncAspect;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.simplify.pretty.ClassHandlers;
import io.tapdata.error.TapEventException;
import io.tapdata.error.TaskTargetProcessorExCode_15;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
import io.tapdata.metric.collector.ISyncMetricCollector;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.GetTableInfoFunction;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ReflectionUtils;

import java.sql.Ref;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.tapdata.aspect.utils.AspectUtils.executeDataFuncAspect;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2023-12-13 10:38
 **/
class HazelcastTargetPdkDataNodeTest extends BaseTaskTest {
	private HazelcastTargetPdkDataNode hazelcastTargetPdkDataNode = mock(HazelcastTargetPdkDataNode.class);;

	@Nested
	@DisplayName("ProcessEvents Method Test")
	class processEventsTest {
		@BeforeEach
		void beforeEach() {
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode, "obsLogger", mockObsLogger);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).processEvents(anyList());
		}

		@Test
		@SneakyThrows
		@DisplayName("Main process test, all dml event")
		void mainProcessTest() {
			int tableCount = 10;
			int rows = 3000;
			List<TapEvent> tapEvents = mockTapEvents(tableCount, rows);
			doAnswer(invocationOnMock -> {
				Object argument = invocationOnMock.getArgument(0);
				assertNotNull(argument);
				assertInstanceOf(List.class, argument);
				List list = (List) argument;
				assertEquals(rows / tableCount, list.size());
				return null;
			}).when(hazelcastTargetPdkDataNode).writeRecord(anyList());
			hazelcastTargetPdkDataNode.processEvents(tapEvents);
			verify(hazelcastTargetPdkDataNode, times(tableCount)).writeRecord(anyList());
		}
		@DisplayName("test write record without tableName")
		@Test
		void test1(){
			List<TapEvent> tapEvents=new ArrayList<>();
			TapUpdateRecordEvent tapEvent=TapUpdateRecordEvent.create();
			tapEvents.add(tapEvent);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).writeRecord(tapEvents);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastTargetPdkDataNode.writeRecord(tapEvents);
			});
			assertEquals(TaskTargetProcessorExCode_15.WRITE_RECORD_GET_TARGET_TABLE_NAME_FAILED,tapCodeException.getCode());

		}

		@NotNull
		private List<TapEvent> mockTapEvents(int tableCount, int rows) {
			LinkedBlockingQueue<String> tableNames = new LinkedBlockingQueue<>();
			IntStream.range(0, tableCount).forEach(i -> tableNames.offer("table_" + (i + 1)));
			List<TapEvent> tapEvents = new ArrayList<>();
			IntStream.range(0, rows).forEach(i -> {
				TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
				String tableName;
				try {
					tableName = tableNames.take();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				tapInsertRecordEvent.table(tableName);
				when(hazelcastTargetPdkDataNode.getTgtTableNameFromTapEvent(tapInsertRecordEvent)).thenReturn(tableName);
				tableNames.offer(tableName);
				tapInsertRecordEvent.after(new HashMap<>());
				tapInsertRecordEvent.setReferenceTime(System.currentTimeMillis());
				tapEvents.add(tapInsertRecordEvent);
			});
			return tapEvents;
		}
	}
	@Nested
	class CreateTableTest{
		private TapTableMap tapTableMap;
		private TableInitFuncAspect funcAspect;
		private Node<?> node;
		private ExistsDataProcessEnum existsDataProcessEnum;
		private String tableId;
		@BeforeEach
		void beforeEach(){
			tapTableMap = mock(TapTableMap.class);
			funcAspect = mock(TableInitFuncAspect.class);
			node = mock(TableNode.class);
			existsDataProcessEnum = mock(ExistsDataProcessEnum.class);
			tableId = "test";
			doCallRealMethod().when(hazelcastTargetPdkDataNode).createTable(tapTableMap,funcAspect,node,existsDataProcessEnum,tableId,true);
		}
		@Test
		@DisplayName("test createTable method when tapTable is null")
		void testCreateTable1(){
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> hazelcastTargetPdkDataNode.createTable(tapTableMap, funcAspect, node, existsDataProcessEnum, tableId, true));
			assertEquals(tapCodeException.getCode(),TaskTargetProcessorExCode_15.INIT_TARGET_TABLE_TAP_TABLE_NULL);
		}
		@Test
		@DisplayName("test createTable method when tableId equals TAP_EXACTLY_ONCE_CACHE")
		void testCreateTable2(){
			tableId = "_TAP_EXACTLY_ONCE_CACHE";
			when(tapTableMap.get(tableId)).thenReturn(mock(TapTable.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).createTable(tapTableMap,funcAspect,node,existsDataProcessEnum,tableId,true);
			hazelcastTargetPdkDataNode.createTable(tapTableMap,funcAspect,node,existsDataProcessEnum,tableId,true);
			verify(hazelcastTargetPdkDataNode,new Times(0)).syncIndex(anyString(),any(TapTable.class),anyBoolean());
		}
		@Test
		@DisplayName("test createTable method normal")
		void testCreateTable3(){
			when(tapTableMap.get(tableId)).thenReturn(mock(TapTable.class));
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"writeStrategy","appendWrite");
			when(funcAspect.state(TableInitFuncAspect.STATE_PROCESS)).thenReturn(mock(TableInitFuncAspect.class));
			hazelcastTargetPdkDataNode.createTable(tapTableMap,funcAspect,node,existsDataProcessEnum,tableId,true);
			verify(hazelcastTargetPdkDataNode,new Times(1)).syncIndex(anyString(),any(TapTable.class),anyBoolean());
		}
	}
	@Nested
	class SyncIndexTest{
		private String tableId;
		private TapTable tapTable;
		private boolean autoCreateTable;
		private ObsLogger obsLogger;
		private ConnectorFunctions connectorFunctions;
		private CreateIndexFunction createIndexFunction;
		private GetTableInfoFunction getTableInfoFunction;
		private QueryIndexesFunction queryIndexesFunction;
		@BeforeEach
		@SneakyThrows
		void beforeEach(){
			tableId = "test";
			tapTable = mock(TapTable.class);
			obsLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"obsLogger",obsLogger);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			connectorFunctions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			when(connectorNode.getConnectorContext()).thenReturn(mock(TapConnectorContext.class));
			createIndexFunction = mock(CreateIndexFunction.class);
			getTableInfoFunction = mock(GetTableInfoFunction.class);
			queryIndexesFunction = mock(QueryIndexesFunction.class);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).queryExistsIndexes(any(), any());
		}
		@Test
		@DisplayName("test sync method when sync index switch is off")
		void testSyncIndex1(){
			when(hazelcastTargetPdkDataNode.checkSyncIndexOpen()).thenReturn(false);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).syncIndex(tableId, tapTable, autoCreateTable);
			hazelcastTargetPdkDataNode.syncIndex(tableId, tapTable, autoCreateTable);
			verify(hazelcastTargetPdkDataNode, new Times(0)).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
		}
		@Test
		@DisplayName("test sync method when autoCreateTable is false")
		void testSyncIndex2(){
			when(hazelcastTargetPdkDataNode.checkSyncIndexOpen()).thenReturn(true);
			autoCreateTable = false;
			doCallRealMethod().when(hazelcastTargetPdkDataNode).syncIndex(tableId, tapTable, autoCreateTable);
			hazelcastTargetPdkDataNode.syncIndex(tableId, tapTable, autoCreateTable);
			verify(obsLogger, new Times(1)).warn(anyString(),anyString());
		}
		@Test
		@DisplayName("test sync method when create index function is null")
		void testSyncIndex3(){
			autoCreateTable = true;
			when(hazelcastTargetPdkDataNode.checkSyncIndexOpen()).thenReturn(true);
			when(connectorFunctions.getCreateIndexFunction()).thenReturn(null);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).syncIndex(tableId, tapTable, autoCreateTable);
			hazelcastTargetPdkDataNode.syncIndex(tableId, tapTable, autoCreateTable);
			verify(hazelcastTargetPdkDataNode, new Times(0)).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
		}
		@Test
		@DisplayName("test sync method when get table info function is null")
		void testSyncIndex4(){
			autoCreateTable = true;
			when(hazelcastTargetPdkDataNode.checkSyncIndexOpen()).thenReturn(true);
			when(connectorFunctions.getCreateIndexFunction()).thenReturn(createIndexFunction);
			when(connectorFunctions.getGetTableInfoFunction()).thenReturn(null);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).syncIndex(tableId, tapTable, autoCreateTable);
			hazelcastTargetPdkDataNode.syncIndex(tableId, tapTable, autoCreateTable);
			verify(hazelcastTargetPdkDataNode, new Times(0)).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
		}
		@Test
		@DisplayName("test sync method when query index function is null")
		void testSyncIndex5(){
			autoCreateTable = true;
			when(hazelcastTargetPdkDataNode.checkSyncIndexOpen()).thenReturn(true);
			when(connectorFunctions.getCreateIndexFunction()).thenReturn(createIndexFunction);
			when(connectorFunctions.getGetTableInfoFunction()).thenReturn(getTableInfoFunction);
			when(connectorFunctions.getQueryIndexesFunction()).thenReturn(null);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).syncIndex(tableId, tapTable, autoCreateTable);
			hazelcastTargetPdkDataNode.syncIndex(tableId, tapTable, autoCreateTable);
			verify(hazelcastTargetPdkDataNode, new Times(0)).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
		}
		@Test
		@DisplayName("test sync method when table count more than threshold")
		@SneakyThrows
		void testSyncIndex6(){
			autoCreateTable = true;
			when(hazelcastTargetPdkDataNode.checkSyncIndexOpen()).thenReturn(true);
			when(connectorFunctions.getCreateIndexFunction()).thenReturn(createIndexFunction);
			when(connectorFunctions.getGetTableInfoFunction()).thenReturn(getTableInfoFunction);
			when(connectorFunctions.getQueryIndexesFunction()).thenReturn(queryIndexesFunction);
			TableInfo tableInfo = mock(TableInfo.class);
			when(tableInfo.getNumOfRows()).thenReturn(5000001L);
			when(getTableInfoFunction.getTableInfo(any(TapConnectorContext.class),anyString())).thenReturn(tableInfo);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).syncIndex(tableId, tapTable, autoCreateTable);
			hazelcastTargetPdkDataNode.syncIndex(tableId, tapTable, autoCreateTable);
			verify(hazelcastTargetPdkDataNode, new Times(0)).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
		}
		@Test
		@DisplayName("test sync method when table count is unknown")
		@SneakyThrows
		void testSyncIndex11(){
			autoCreateTable = true;
			when(hazelcastTargetPdkDataNode.checkSyncIndexOpen()).thenReturn(true);
			when(connectorFunctions.getCreateIndexFunction()).thenReturn(createIndexFunction);
			when(connectorFunctions.getGetTableInfoFunction()).thenReturn(getTableInfoFunction);
			when(connectorFunctions.getQueryIndexesFunction()).thenReturn(queryIndexesFunction);
			TableInfo tableInfo = mock(TableInfo.class);
			when(tableInfo.getNumOfRows()).thenReturn(null);
			when(getTableInfoFunction.getTableInfo(any(TapConnectorContext.class),anyString())).thenReturn(tableInfo);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).syncIndex(tableId, tapTable, autoCreateTable);
			hazelcastTargetPdkDataNode.syncIndex(tableId, tapTable, autoCreateTable);
			verify(obsLogger, new Times(1)).warn(anyString(),anyString());
		}
		@Test
		@DisplayName("test sync method when table info is null")
		@SneakyThrows
		void testSyncIndex12(){
			autoCreateTable = true;
			when(hazelcastTargetPdkDataNode.checkSyncIndexOpen()).thenReturn(true);
			when(connectorFunctions.getCreateIndexFunction()).thenReturn(createIndexFunction);
			when(connectorFunctions.getGetTableInfoFunction()).thenReturn(getTableInfoFunction);
			when(connectorFunctions.getQueryIndexesFunction()).thenReturn(queryIndexesFunction);
			when(getTableInfoFunction.getTableInfo(any(TapConnectorContext.class),anyString())).thenReturn(null);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).syncIndex(tableId, tapTable, autoCreateTable);
			hazelcastTargetPdkDataNode.syncIndex(tableId, tapTable, autoCreateTable);
			verify(obsLogger, new Times(1)).warn(anyString(),anyString());
		}
		@Test
		@DisplayName("test sync method when exists index with same name")
		@SneakyThrows
		void testSyncIndex7(){
			autoCreateTable = true;
			when(hazelcastTargetPdkDataNode.checkSyncIndexOpen()).thenReturn(true);
			when(connectorFunctions.getCreateIndexFunction()).thenReturn(createIndexFunction);
			when(connectorFunctions.getGetTableInfoFunction()).thenReturn(getTableInfoFunction);
			when(connectorFunctions.getQueryIndexesFunction()).thenReturn(queryIndexesFunction);
			TableInfo tableInfo = mock(TableInfo.class);
			when(tableInfo.getNumOfRows()).thenReturn(1L);
			when(getTableInfoFunction.getTableInfo(any(TapConnectorContext.class),anyString())).thenReturn(tableInfo);
			List<TapIndex> indices = new ArrayList<>();
			TapIndex tapIndex = mock(TapIndex.class);
			when(tapIndex.getUnique()).thenReturn(false);
			when(tapIndex.getPrimary()).thenReturn(false);
			when(tapIndex.getName()).thenReturn("index");
			indices.add(tapIndex);
			when(tapTable.getIndexList()).thenReturn(indices);
			doAnswer(invocationOnMock -> {
				Consumer consumer = invocationOnMock.getArgument(2);
				List<TapIndex> tapIndexList = new ArrayList<>();
				tapIndexList.add(tapIndex);
				consumer.accept(tapIndexList);
				return null;
			}).when(queryIndexesFunction).query(any(),any(),any());
			doCallRealMethod().when(hazelcastTargetPdkDataNode).syncIndex(tableId, tapTable, autoCreateTable);
			hazelcastTargetPdkDataNode.syncIndex(tableId, tapTable, autoCreateTable);
			verify(hazelcastTargetPdkDataNode, new Times(0)).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
		}
		@Test
		@DisplayName("test sync method when exists index with same field")
		@SneakyThrows
		void testSyncIndex8(){
			autoCreateTable = true;
			when(hazelcastTargetPdkDataNode.checkSyncIndexOpen()).thenReturn(true);
			when(connectorFunctions.getCreateIndexFunction()).thenReturn(createIndexFunction);
			when(connectorFunctions.getGetTableInfoFunction()).thenReturn(getTableInfoFunction);
			when(connectorFunctions.getQueryIndexesFunction()).thenReturn(queryIndexesFunction);
			TableInfo tableInfo = mock(TableInfo.class);
			when(tableInfo.getNumOfRows()).thenReturn(1L);
			when(getTableInfoFunction.getTableInfo(any(TapConnectorContext.class),anyString())).thenReturn(tableInfo);
			List<TapIndex> indices = new ArrayList<>();
			TapIndex tapIndex = mock(TapIndex.class);
			when(tapIndex.getUnique()).thenReturn(false);
			when(tapIndex.getPrimary()).thenReturn(false);
			when(tapIndex.getName()).thenReturn("index");
			List<TapIndexField> indexFields = new ArrayList<>();
			TapIndexField indexField = mock(TapIndexField.class);
			when(indexField.getName()).thenReturn("indexField");
			indexFields.add(indexField);
			when(tapIndex.getIndexFields()).thenReturn(indexFields);
			indices.add(tapIndex);
			when(tapTable.getIndexList()).thenReturn(indices);
			doAnswer(invocationOnMock -> {
				Consumer consumer = invocationOnMock.getArgument(2);
				List<TapIndex> tapIndexList = new ArrayList<>();
				TapIndex index = mock(TapIndex.class);
				when(index.getName()).thenReturn("index1");
				when(index.getIndexFields()).thenReturn(indexFields);
				tapIndexList.add(index);
				consumer.accept(tapIndexList);
				return null;
			}).when(queryIndexesFunction).query(any(),any(),any());
			doCallRealMethod().when(hazelcastTargetPdkDataNode).syncIndex(tableId, tapTable, autoCreateTable);
			hazelcastTargetPdkDataNode.syncIndex(tableId, tapTable, autoCreateTable);
			verify(hazelcastTargetPdkDataNode, new Times(0)).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
		}
		@Test
		@DisplayName("test sync method normally")
		@SneakyThrows
		void testSyncIndex9(){
			autoCreateTable = true;
			when(hazelcastTargetPdkDataNode.checkSyncIndexOpen()).thenReturn(true);
			when(connectorFunctions.getCreateIndexFunction()).thenReturn(createIndexFunction);
			when(connectorFunctions.getGetTableInfoFunction()).thenReturn(getTableInfoFunction);
			when(connectorFunctions.getQueryIndexesFunction()).thenReturn(queryIndexesFunction);
			TableInfo tableInfo = mock(TableInfo.class);
			when(tableInfo.getNumOfRows()).thenReturn(1L);
			when(getTableInfoFunction.getTableInfo(any(TapConnectorContext.class),anyString())).thenReturn(tableInfo);
			List<TapIndex> indices = new ArrayList<>();
			TapIndex tapIndex = mock(TapIndex.class);
			when(tapIndex.getUnique()).thenReturn(false);
			when(tapIndex.getPrimary()).thenReturn(false);
			when(tapIndex.getName()).thenReturn("index");
			List<TapIndexField> indexFields = new ArrayList<>();
			TapIndexField indexField = mock(TapIndexField.class);
			when(indexField.getName()).thenReturn("indexField");
			indexFields.add(indexField);
			when(tapIndex.getIndexFields()).thenReturn(indexFields);
			indices.add(tapIndex);
			when(tapTable.getIndexList()).thenReturn(indices);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).syncIndex(tableId, tapTable, autoCreateTable);
			hazelcastTargetPdkDataNode.syncIndex(tableId, tapTable, autoCreateTable);
			verify(hazelcastTargetPdkDataNode, new Times(1)).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
		}
		@Test
		@DisplayName("test sync method with exception")
		@SneakyThrows
		void testSyncIndex10(){
			autoCreateTable = true;
			when(hazelcastTargetPdkDataNode.checkSyncIndexOpen()).thenReturn(true);
			when(connectorFunctions.getCreateIndexFunction()).thenReturn(createIndexFunction);
			when(connectorFunctions.getGetTableInfoFunction()).thenReturn(getTableInfoFunction);
			when(connectorFunctions.getQueryIndexesFunction()).thenReturn(queryIndexesFunction);
			TableInfo tableInfo = mock(TableInfo.class);
			when(tableInfo.getNumOfRows()).thenReturn(1L);
			when(getTableInfoFunction.getTableInfo(any(TapConnectorContext.class),anyString())).thenReturn(tableInfo);
			List<TapIndex> indices = new ArrayList<>();
			TapIndex tapIndex = mock(TapIndex.class);
			when(tapIndex.getUnique()).thenReturn(false);
			when(tapIndex.getPrimary()).thenReturn(false);
			when(tapIndex.getName()).thenReturn("index");
			List<TapIndexField> indexFields = new ArrayList<>();
			TapIndexField indexField = mock(TapIndexField.class);
			indexFields.add(indexField);
			when(tapIndex.getIndexFields()).thenReturn(indexFields);
			indices.add(tapIndex);
			when(tapTable.getIndexList()).thenReturn(indices);
			doThrow(new TapCodeException("15003")).when(queryIndexesFunction).query(any(TapConnectorContext.class),any(TapTable.class),any(Consumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).syncIndex(tableId, tapTable, autoCreateTable);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).throwTapCodeException(any(),any());
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> hazelcastTargetPdkDataNode.syncIndex(tableId, tapTable, autoCreateTable));
			assertEquals(TaskTargetProcessorExCode_15.CREATE_INDEX_FAILED,tapCodeException.getCode());
		}
	}
	@Nested
	class CheckSyncIndexOpenTest{
		private Node node;
		private DataParentNode dataParentNode;
		@BeforeEach
		void beforeEach(){
			node = mock(DatabaseNode.class);
			dataParentNode = (DataParentNode) node;
			doCallRealMethod().when(hazelcastTargetPdkDataNode).checkSyncIndexOpen();
		}
		@Test
		@DisplayName("test checkSyncIndexOpen method when switch is on")
		void testCheckSyncIndexOpen1(){
			when(dataParentNode.getSyncIndexEnable()).thenReturn(true);
			when(hazelcastTargetPdkDataNode.getNode()).thenReturn(node);
			boolean actual = hazelcastTargetPdkDataNode.checkSyncIndexOpen();
			assertEquals(true, actual);
		}
		@Test
		@DisplayName("test checkSyncIndexOpen method when switch is off")
		void testCheckSyncIndexOpen2(){
			when(dataParentNode.getSyncIndexEnable()).thenReturn(false);
			when(hazelcastTargetPdkDataNode.getNode()).thenReturn(node);
			boolean actual = hazelcastTargetPdkDataNode.checkSyncIndexOpen();
			assertEquals(false, actual);
		}
		@Test
		@DisplayName("test checkSyncIndexOpen method when node is null")
		void testCheckSyncIndexOpen3(){
			when(hazelcastTargetPdkDataNode.getNode()).thenReturn(null);
			boolean actual = hazelcastTargetPdkDataNode.checkSyncIndexOpen();
			assertEquals(false, actual);
		}
		@Test
		@DisplayName("test checkSyncIndexOpen method when node is not database node or table node")
		void testCheckSyncIndexOpen4(){
			Node node1 = mock(Node.class);
			when(hazelcastTargetPdkDataNode.getNode()).thenReturn(node1);
			boolean actual = hazelcastTargetPdkDataNode.checkSyncIndexOpen();
			assertEquals(false, actual);
		}
	}

	@Nested
	class executeCreateIndexFunctionTest {

		private TapTableMap<String, TapTable> tapTableMap;

		@BeforeEach
		void setUp() {
			when(hazelcastTargetPdkDataNode.executeCreateIndexFunction(any())).thenCallRealMethod();
			tapTableMap = mock(TapTableMap.class);
			DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode, "dataProcessorContext", dataProcessorContext);
		}

		@Test
		@DisplayName("test main process")
		@SneakyThrows
		void test1() {
			List<TapIndex> tapIndices = new ArrayList<>();
			tapIndices.add(new TapIndex()
					.indexField(new TapIndexField().name("test_id").fieldAsc(true))
					.indexField(new TapIndexField().name("test_id1").fieldAsc(false)));
			tapIndices.add(new TapIndex()
					.name("dup_index")
					.indexField(new TapIndexField().name("dup_id").fieldAsc(true)));
			TapCreateIndexEvent tapCreateIndexEvent = new TapCreateIndexEvent()
					.indexList(tapIndices);
			tapCreateIndexEvent.setTableId("test");
			TapTable tapTable = new TapTable("test");
			when(tapTableMap.get("test")).thenReturn(tapTable);
			CreateIndexFunction createIndexFunction = mock(CreateIndexFunction.class);
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			when(connectorFunctions.getCreateIndexFunction()).thenReturn(createIndexFunction);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			List<TapIndex> existsIndexes = new ArrayList<>();
			existsIndexes.add(tapIndices.get(1));
			when(hazelcastTargetPdkDataNode.queryExistsIndexes(tapTable, tapIndices)).thenReturn(existsIndexes);
			TapConnectorContext connectorContext = mock(TapConnectorContext.class);
			when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
			doCallRealMethod().when(connectorNode).applyClassLoaderContext(any());
			doAnswer(invocationOnMock -> {
				Object argument3 = invocationOnMock.getArgument(2);
				((CommonUtils.AnyErrorConsumer<?>) argument3).accept(null);
				return null;
			}).when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(), any(), any());
			boolean result = hazelcastTargetPdkDataNode.executeCreateIndexFunction(tapCreateIndexEvent);
			assertTrue(result);
			assertEquals(1, tapCreateIndexEvent.getIndexList().size());
			verify(hazelcastTargetPdkDataNode).queryExistsIndexes(tapTable, tapIndices);
			verify(createIndexFunction).createIndex(connectorContext, tapTable, tapCreateIndexEvent);
		}

		@Test
		@DisplayName("test event's tableId is empty")
		void test2() {
			TapCreateIndexEvent tapCreateIndexEvent = new TapCreateIndexEvent();
			TapEventException tapEventException = assertThrows(TapEventException.class, () -> hazelcastTargetPdkDataNode.executeCreateIndexFunction(tapCreateIndexEvent));
			assertEquals(TaskTargetProcessorExCode_15.CREATE_INDEX_EVENT_TABLE_ID_EMPTY, tapEventException.getCode());
			assertNotNull(tapEventException.getEvents());
			assertEquals(1, tapEventException.getEvents().size());
			assertEquals(tapCreateIndexEvent, tapEventException.getEvents().get(0));
		}

		@Test
		@DisplayName("test cannot get TapTable by tableId")
		void test3() {
			TapCreateIndexEvent tapCreateIndexEvent = new TapCreateIndexEvent();
			tapCreateIndexEvent.setTableId("test");
			when(tapTableMap.get("test")).thenReturn(null);
			TapEventException tapEventException = assertThrows(TapEventException.class, () -> hazelcastTargetPdkDataNode.executeCreateIndexFunction(tapCreateIndexEvent));
			assertEquals(TaskTargetProcessorExCode_15.CREATE_INDEX_TABLE_NOT_FOUND, tapEventException.getCode());
			assertNotNull(tapEventException.getEvents());
			assertEquals(1, tapEventException.getEvents().size());
			assertEquals(tapCreateIndexEvent, tapEventException.getEvents().get(0));
		}

		@Test
		@DisplayName("test not support CreateIndexFunction")
		void test4() {
			TapCreateIndexEvent tapCreateIndexEvent = new TapCreateIndexEvent();
			tapCreateIndexEvent.setTableId("test");
			TapTable tapTable = new TapTable("test");
			when(tapTableMap.get("test")).thenReturn(tapTable);
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			when(connectorFunctions.getCreateIndexFunction()).thenReturn(null);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			assertFalse(hazelcastTargetPdkDataNode.executeCreateIndexFunction(tapCreateIndexEvent));
		}

		@Test
		@DisplayName("test queryExistsIndexes error")
		@SneakyThrows
		void test5() {
			List<TapIndex> tapIndices = new ArrayList<>();
			tapIndices.add(new TapIndex()
					.indexField(new TapIndexField().name("test_id").fieldAsc(true))
					.indexField(new TapIndexField().name("test_id1").fieldAsc(false)));
			tapIndices.add(new TapIndex()
					.name("dup_index")
					.indexField(new TapIndexField().name("dup_id").fieldAsc(true)));
			TapCreateIndexEvent tapCreateIndexEvent = new TapCreateIndexEvent()
					.indexList(tapIndices);
			tapCreateIndexEvent.setTableId("test");
			TapTable tapTable = new TapTable("test");
			when(tapTableMap.get("test")).thenReturn(tapTable);
			CreateIndexFunction createIndexFunction = mock(CreateIndexFunction.class);
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			when(connectorFunctions.getCreateIndexFunction()).thenReturn(createIndexFunction);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			when(hazelcastTargetPdkDataNode.queryExistsIndexes(tapTable, tapIndices)).thenThrow(new RuntimeException("test"));

			TapEventException tapEventException = assertThrows(TapEventException.class, () -> hazelcastTargetPdkDataNode.executeCreateIndexFunction(tapCreateIndexEvent));
			assertEquals(TaskTargetProcessorExCode_15.CREATE_INDEX_QUERY_EXISTS_INDEX_FAILED, tapEventException.getCode());
			assertNotNull(tapEventException.getEvents());
			assertEquals(1, tapEventException.getEvents().size());
			assertEquals(tapCreateIndexEvent, tapEventException.getEvents().get(0));
		}
	}
	@Nested
	class createTargetIndexTest{
		private List<String> updateConditionFields;
		private boolean createUnique;
		private String tableId;
		private TapTable tapTable;
		private boolean createdTable;
		@BeforeEach
		void beforeEach(){
			updateConditionFields = new ArrayList<>();
			updateConditionFields.add("field");
			tableId = "table";
			tapTable = mock(TapTable.class);
			ArrayList<String> pks = new ArrayList<>();
			when(tapTable.primaryKeys()).thenReturn(pks);
			when(hazelcastTargetPdkDataNode.usePkAsUpdateConditions(updateConditionFields,pks)).thenReturn(false);
			createUnique = true;
			createdTable = true;
			String writeStrategy = "updateOrInsert";
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"writeStrategy",writeStrategy);
			Boolean unwindProcess = false;
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"unwindProcess",unwindProcess);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"obsLogger",mockObsLogger);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getCreateIndexFunction()).thenReturn(mock(CreateIndexFunction.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).createTargetIndex(updateConditionFields,createUnique,tableId,tapTable,createdTable);
		}
		@Test
		@DisplayName("test createTargetIndex method for build error consumer")
		void test1(){
			hazelcastTargetPdkDataNode.createTargetIndex(updateConditionFields,createUnique,tableId,tapTable,createdTable);
			verify(hazelcastTargetPdkDataNode,new Times(1)).buildErrorConsumer(tableId);
		}
		@Test
		@SneakyThrows
		@DisplayName("test createTargetIndex method when index already exists")
		void test3(){
			List<TapIndex> existsIndexes = new ArrayList<>();
			existsIndexes.add(mock(TapIndex.class));
			when(hazelcastTargetPdkDataNode.queryExistsIndexes(any(TapTable.class),anyList())).thenReturn(existsIndexes);
			hazelcastTargetPdkDataNode.createTargetIndex(updateConditionFields,createUnique,tableId,tapTable,createdTable);
			verify(hazelcastTargetPdkDataNode,never()).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
		}
		@DisplayName("test createTargetIndex error exception")
		@Test
		void test4(){
			when(hazelcastTargetPdkDataNode.executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class))).thenThrow(new RuntimeException("mock error"));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).throwTapCodeException(any(),any());
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastTargetPdkDataNode.createTargetIndex(updateConditionFields, createUnique, tableId, tapTable, createdTable);
			});
			assertEquals(tapCodeException.getCode(),TaskTargetProcessorExCode_15.CREATE_INDEX_FAILED);
		}
	}
	@Nested
	class clearDataTest{
		private ExistsDataProcessEnum existsDataProcessEnum;
		private String tableId = "test";
		@Test
		@DisplayName("test clearData method for build error consumer")
		void test1(){
			SyncTypeEnum syncType = SyncTypeEnum.INITIAL_SYNC;
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"syncType",syncType);
			existsDataProcessEnum = ExistsDataProcessEnum.REMOVE_DATE;
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getClearTableFunction()).thenReturn(mock(ClearTableFunction.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).clearData(existsDataProcessEnum,tableId);
			hazelcastTargetPdkDataNode.clearData(existsDataProcessEnum,tableId);
			verify(hazelcastTargetPdkDataNode,new Times(1)).buildErrorConsumer(tableId);
		}
		@Test
		@DisplayName("test clearData method for exception")
		void test2(){
			SyncTypeEnum syncType = SyncTypeEnum.INITIAL_SYNC;
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"syncType",syncType);
			existsDataProcessEnum = ExistsDataProcessEnum.REMOVE_DATE;
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getClearTableFunction()).thenReturn(mock(ClearTableFunction.class));
			doThrow(new RuntimeException("clearData")).when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).clearData(existsDataProcessEnum,tableId);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).throwTapCodeException(any(),any());
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastTargetPdkDataNode.clearData(existsDataProcessEnum, tableId);
			});
			assertEquals(tapCodeException.getCode(),TaskTargetProcessorExCode_15.CLEAR_TABLE_FAILED);
		}
	}
	@Nested
	class dropTableTest{
		private ExistsDataProcessEnum existsDataProcessEnum;
		private String tableId = "test";
		@Test
		@DisplayName("test dropTable method for build error consumer")
		void test1(){
			TapTable mockTable = mock(TapTable.class);
			when(mockTable.getId()).thenReturn(tableId);
			SyncTypeEnum syncType = SyncTypeEnum.INITIAL_SYNC;
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"syncType",syncType);
			existsDataProcessEnum = ExistsDataProcessEnum.DROP_TABLE;
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getDropTableFunction()).thenReturn(mock(DropTableFunction.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).dropTable(existsDataProcessEnum,mockTable,true);
			hazelcastTargetPdkDataNode.dropTable(existsDataProcessEnum,mockTable,true);
			verify(hazelcastTargetPdkDataNode,new Times(1)).buildErrorConsumer(tableId);
		}
	}
	@Nested
	class executeNewFieldFunctionTest{
		private TapNewFieldEvent tapNewFieldEvent;
		private TapTable tapTable;
		@BeforeEach
		void beforeEach(){
			tapNewFieldEvent = new TapNewFieldEvent();
			tapNewFieldEvent.setTableId("test");
			List<TapField> fields = new ArrayList<>();
			TapField newField = new TapField();
			newField.setName("newField");
			fields.add(newField);
			tapNewFieldEvent.setNewFields(fields);
			dataProcessorContext = mock(DataProcessorContext.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"dataProcessorContext",dataProcessorContext);
			TapTableMap tapTableMap = mock(TapTableMap.class);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			tapTable = new TapTable();
			tapTable.setId("table");
			LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap<>();
			nameFieldMap.put("field1",mock(TapField.class));
			nameFieldMap.put("newField",newField);
			tapTable.setNameFieldMap(nameFieldMap);
			when(tapTableMap.get("test")).thenReturn(tapTable);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getNewFieldFunction()).thenReturn(mock(NewFieldFunction.class));
		}
		@Test
		@DisplayName("test executeNewFieldFunction method for build error consumer")
		void test1(){
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeNewFieldFunction(tapNewFieldEvent);
			hazelcastTargetPdkDataNode.executeNewFieldFunction(tapNewFieldEvent);
			verify(hazelcastTargetPdkDataNode,new Times(1)).buildErrorConsumer("table");
		}
		@Test
		@DisplayName("test executeNewFieldFunction method for ADD_NEW_FIELD_GET_TAP_TABLE_FAILED Exception")
		void test2(){
			TapNewFieldEvent tapNewFieldEvent1 = new TapNewFieldEvent();
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeNewFieldFunction(tapNewFieldEvent1);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastTargetPdkDataNode.executeNewFieldFunction(tapNewFieldEvent1);
			});
			assertEquals(tapCodeException.getCode(),TaskTargetProcessorExCode_15.ADD_NEW_FIELD_GET_TAP_TABLE_FAILED);
		}
		@Test
		@DisplayName("test executeNewFieldFunction method for ADD_NEW_FIELD_GET_TAP_TABLE_FAILED Exception")
		void test3(){
			List<TapField> fields = new ArrayList<>();
			TapField newField = new TapField();
			newField.setName("newField2");
			fields.add(newField);
			tapNewFieldEvent.setNewFields(fields);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeNewFieldFunction(tapNewFieldEvent);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastTargetPdkDataNode.executeNewFieldFunction(tapNewFieldEvent);
			});
			assertEquals(tapCodeException.getCode(),TaskTargetProcessorExCode_15.ADD_NEW_FIELD_IS_NULL);
		}
		@Test
		@DisplayName("test executeNewFieldFunction method for ADD_NEW_FIELD_EXECUTE_FAILED Exception")
		void test4(){

			doThrow(new RuntimeException("add new field failed")).when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).throwTapCodeException(any(),any());
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeNewFieldFunction(tapNewFieldEvent);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastTargetPdkDataNode.executeNewFieldFunction(tapNewFieldEvent);
			});
			assertEquals(tapCodeException.getCode(),TaskTargetProcessorExCode_15.ADD_NEW_FIELD_EXECUTE_FAILED);
		}
	}
	@Nested
	class executeAlterFieldNameFunctionTest{
		private TapAlterFieldNameEvent tapAlterFieldNameEvent;
		@BeforeEach
		void doInit(){
			tapAlterFieldNameEvent = mock(TapAlterFieldNameEvent.class);
			when(tapAlterFieldNameEvent.getTableId()).thenReturn("test");
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getAlterFieldNameFunction()).thenReturn(mock(AlterFieldNameFunction.class));
		}
		@Test
		@DisplayName("test executeAlterFieldNameFunction method for build error consumer")
		void test1(){
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeAlterFieldNameFunction(tapAlterFieldNameEvent);
			hazelcastTargetPdkDataNode.executeAlterFieldNameFunction(tapAlterFieldNameEvent);
			verify(hazelcastTargetPdkDataNode,new Times(1)).buildErrorConsumer("test");
		}
		@DisplayName("test executeAlterFieldNameFunction for exception ALTER_FIELD_NAME_EXECUTE_FAILED")
		@Test
		void test2(){
			doThrow(new RuntimeException("alter field name failed")).when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeAlterFieldNameFunction(tapAlterFieldNameEvent);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).throwTapCodeException(any(),any());
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastTargetPdkDataNode.executeAlterFieldNameFunction(tapAlterFieldNameEvent);
			});
			assertEquals(tapCodeException.getCode(),TaskTargetProcessorExCode_15.ALTER_FIELD_NAME_EXECUTE_FAILED);
		}
	}
	@Nested
	class executeAlterFieldAttrFunctionTest{
		private TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent;
		private TapTable tapTable;
		@BeforeEach
		void beforeEach(){
			tapAlterFieldAttributesEvent = mock(TapAlterFieldAttributesEvent.class);
			when(tapAlterFieldAttributesEvent.getTableId()).thenReturn("test");
			dataProcessorContext = mock(DataProcessorContext.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"dataProcessorContext",dataProcessorContext);
			TapTableMap tapTableMap = mock(TapTableMap.class);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			tapTable = new TapTable();
			tapTable.setId("table");
			LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap<>();
			nameFieldMap.put("field1",mock(TapField.class));
			tapTable.setNameFieldMap(nameFieldMap);
			when(tapTableMap.get("test")).thenReturn(tapTable);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getAlterFieldAttributesFunction()).thenReturn(mock(AlterFieldAttributesFunction.class));
		}
		@Test
		@DisplayName("test executeAlterFieldAttrFunction method for build error consumer")
		void test1(){
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeAlterFieldAttrFunction(tapAlterFieldAttributesEvent);
			hazelcastTargetPdkDataNode.executeAlterFieldAttrFunction(tapAlterFieldAttributesEvent);
			verify(hazelcastTargetPdkDataNode,new Times(1)).buildErrorConsumer("table");
		}
		@DisplayName("test executeAlterFieldAttrFunction exception for ALTER_FIELD_ATTR_CANNOT_GET_TAP_TABLE")
		@Test
		void test2(){
			TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = mock(TapAlterFieldAttributesEvent.class);
			when(tapAlterFieldAttributesEvent.getTableId()).thenReturn("test1");
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeAlterFieldAttrFunction(tapAlterFieldAttributesEvent);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastTargetPdkDataNode.executeAlterFieldAttrFunction(tapAlterFieldAttributesEvent);
			});
			assertEquals(tapCodeException.getCode(),TaskTargetProcessorExCode_15.ALTER_FIELD_ATTR_CANNOT_GET_TAP_TABLE);
		}
		@DisplayName("test executeAlterFieldAttrFunction exception for ALTER_FIELD_ATTR_EXECUTE_FAILED")
		@Test
		void test3(){
			doThrow(new RuntimeException("run alter field attr failed")).when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeAlterFieldAttrFunction(tapAlterFieldAttributesEvent);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).throwTapCodeException(any(),any());
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastTargetPdkDataNode.executeAlterFieldAttrFunction(tapAlterFieldAttributesEvent);
			});
			assertEquals(tapCodeException.getCode(),TaskTargetProcessorExCode_15.ALTER_FIELD_ATTR_EXECUTE_FAILED);

		}
	}
	@Nested
	class executeDropFieldFunctionTest{
		private TapDropFieldEvent tapDropFieldEvent;
		@BeforeEach
		void beforeEach(){
			tapDropFieldEvent = mock(TapDropFieldEvent.class);
			when(tapDropFieldEvent.getTableId()).thenReturn("test");
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getDropFieldFunction()).thenReturn(mock(DropFieldFunction.class));
		}
		@Test
		@DisplayName("test executeDropFieldFunction method for build error consumer")
		void test1(){
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeDropFieldFunction(tapDropFieldEvent);
			hazelcastTargetPdkDataNode.executeDropFieldFunction(tapDropFieldEvent);
			verify(hazelcastTargetPdkDataNode,new Times(1)).buildErrorConsumer("test");
		}
		@DisplayName("test executeDropFieldFunction method for DROP_FIELD_EXECUTE_FAILED exception")
		@Test
		void test2(){
			doThrow(new RuntimeException("drop field")).when(hazelcastTargetPdkDataNode).executeDataFuncAspect(any(Class.class),any(Callable.class),any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).executeDropFieldFunction(tapDropFieldEvent);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).throwTapCodeException(any(),any());
			TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
				hazelcastTargetPdkDataNode.executeDropFieldFunction(tapDropFieldEvent);
			});
			assertEquals(tapCodeException.getCode(),TaskTargetProcessorExCode_15.DROP_FIELD_EXECUTE_FAILED);
		}
	}
	@Nested
	class updateDagTest{
		@DisplayName("test update Dag when create table Event")
		@Test
		void test1(){
			TapCreateTableEvent tapCreateTableEvent = new TapCreateTableEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapCreateTableEvent);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).updateDAG(tapdataEvent);
			hazelcastTargetPdkDataNode.updateDAG(tapdataEvent);
			verify(hazelcastTargetPdkDataNode,new Times(1)).updateDAG(tapdataEvent);
		}
		@DisplayName("test update Dag when tapAlterFieldNameEvent Event when Data Transformation")
		@Test
		void test2(){
			allSetup();
			Map<String, List<String>> updateConditionFieldsMap = new HashMap<>();
			List<String> updateConditionFields = new ArrayList<>();
			updateConditionFields.add("id");
			updateConditionFieldsMap.put("dummy_test",updateConditionFields);
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"updateConditionFieldsMap",updateConditionFieldsMap);
			TapAlterFieldNameEvent tapAlterFieldNameEvent=new TapAlterFieldNameEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapAlterFieldNameEvent);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).updateDAG(tapdataEvent);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).getNode();
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode, "dataProcessorContext", dataProcessorContext);
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"processorBaseContext",processorBaseContext);
			hazelcastTargetPdkDataNode.updateDAG(tapdataEvent);
			TableNode node = (TableNode) processorBaseContext.getNode();
			List<String> updateConditionFields1 = node.getUpdateConditionFields();
			assertEquals(updateConditionFields,updateConditionFields1);
		}
		@DisplayName("test update Dag when tapAlterFieldNameEvent Event when Data Replications")
		@Test
		void test3(){
			setUpDatabaseNode();
			Map<String, List<String>> concurrentWritePartitionMap =new HashMap<>();
			List<String> list=new ArrayList<>();
			list.add("partition1");
			concurrentWritePartitionMap.put("dummyTest",list);
			TapAlterFieldNameEvent tapAlterFieldNameEvent=new TapAlterFieldNameEvent();
			tapAlterFieldNameEvent.setTableId("dummyTest");
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapAlterFieldNameEvent);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).updateDAG(tapdataEvent);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).getNode();
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode, "dataProcessorContext", dataProcessorContext);
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"processorBaseContext",processorBaseContext);
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"concurrentWritePartitionMap",concurrentWritePartitionMap);
			hazelcastTargetPdkDataNode.updateDAG(tapdataEvent);
			DatabaseNode node = (DatabaseNode) processorBaseContext.getNode();
			assertEquals(node.getConcurrentWritePartitionMap().size(),concurrentWritePartitionMap.size());
		}
	}
	@Nested
	class checkCreateUniqueIndexOpenTest{
		private Node node;
		private DataParentNode dataParentNode;
		@BeforeEach
		void beforeEach(){
			node = mock(DatabaseNode.class);
			dataParentNode = (DataParentNode) node;
			doCallRealMethod().when(hazelcastTargetPdkDataNode).checkCreateUniqueIndexOpen();
		}
		@Test
		@DisplayName("test checkCreateUniqueIndexOpen method when switch is on")
		void test1(){
			when(dataParentNode.getUniqueIndexEnable()).thenReturn(true);
			when(hazelcastTargetPdkDataNode.getNode()).thenReturn(node);
			boolean actual = hazelcastTargetPdkDataNode.checkCreateUniqueIndexOpen();
			assertEquals(true, actual);
		}
		@Test
		@DisplayName("test checkCreateUniqueIndexOpen method when switch is off")
		void test2(){
			when(dataParentNode.getUniqueIndexEnable()).thenReturn(false);
			when(hazelcastTargetPdkDataNode.getNode()).thenReturn(node);
			boolean actual = hazelcastTargetPdkDataNode.checkCreateUniqueIndexOpen();
			assertEquals(false, actual);
		}
		@Test
		@DisplayName("test checkCreateUniqueIndexOpen method when node is null")
		void test3(){
			when(hazelcastTargetPdkDataNode.getNode()).thenReturn(null);
			boolean actual = hazelcastTargetPdkDataNode.checkCreateUniqueIndexOpen();
			assertEquals(true, actual);
		}
		@Test
		@DisplayName("test checkCreateUniqueIndexOpen method when node is not database node or table node")
		void test4(){
			Node node1 = mock(Node.class);
			when(hazelcastTargetPdkDataNode.getNode()).thenReturn(node1);
			boolean actual = hazelcastTargetPdkDataNode.checkCreateUniqueIndexOpen();
			assertEquals(true, actual);
		}
	}

	@Nested
	class testFilterSubPartitionTableTableMap {
		@Test
		void testFilterSubPartitionTableTableMap() {

			DataProcessorContext context = mock(DataProcessorContext.class);
			TapTableMap<String, TapTable> tableMap = TapTableMap.create("test");

			TapTable table = new TapTable();
			table.setId("test");
			table.setName("test");
			table.setPartitionInfo(new TapPartition());
			table.setPartitionMasterTableId("test");
			tableMap.putNew("test", table, "test");

			table = new TapTable();
			table.setId("test1");
			table.setName("test1");
			table.setPartitionInfo(new TapPartition());
			table.setPartitionMasterTableId("test1_1");
			tableMap.putNew("test1", table, "test1");

			TaskDto taskDto = new TaskDto();
			taskDto.setSyncType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			when(context.getTaskDto()).thenReturn(taskDto);
			when(context.getTapTableMap()).thenReturn(tableMap);
			Node node = new DatabaseNode();
			node.setId("test");
			node.setName("test");
			when(context.getNode()).thenReturn(node);

			HazelcastTargetPdkDataNode targetPdkDataNode = new HazelcastTargetPdkDataNode(context);
			targetPdkDataNode.syncTargetPartitionTableEnable = Boolean.TRUE;

			Set<String> result = targetPdkDataNode.filterSubPartitionTableTableMap();

			Assertions.assertNotNull(result);
			Assertions.assertEquals(1, result.size());

			targetPdkDataNode.syncTargetPartitionTableEnable = Boolean.FALSE;
			result = targetPdkDataNode.filterSubPartitionTableTableMap();

			Assertions.assertNotNull(result);
			Assertions.assertEquals(2, result.size());

		}
	}

	@Test
	public void testDropTable() {
		DataProcessorContext context = mock(DataProcessorContext.class);
		TaskDto taskDto = new TaskDto();
		taskDto.setSyncType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		when(context.getTaskDto()).thenReturn(taskDto);

		Node node = new DatabaseNode();
		node.setId("test");
		node.setName("test");
		when(context.getNode()).thenReturn(node);

		HazelcastTargetPdkDataNode targetPdkDataNode = new HazelcastTargetPdkDataNode(context);
		targetPdkDataNode.syncTargetPartitionTableEnable = Boolean.TRUE;

		HazelcastTargetPdkDataNode spyTargetPdkDataNode = spy(targetPdkDataNode);

		ConnectorNode connectorNode = mock(ConnectorNode.class);
		ConnectorFunctions connectorFunction = mock(ConnectorFunctions.class);
		DropTableFunction dropTableFunction = new DropTableFunction() {
			@Override
			public void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) throws Throwable {

			}
		};
		DropPartitionTableFunction dropPartititonTableFunction = new DropPartitionTableFunction() {
			@Override
			public void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) throws Exception {

			}
		};
		when(connectorFunction.getDropTableFunction()).thenReturn(dropTableFunction);

		when(connectorFunction.getDropPartitionTableFunction()).thenReturn(dropPartititonTableFunction);
		when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunction);
		when(spyTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
		doAnswer(answer -> {
			Callable aspectCallable = answer.getArgument(1);
			aspectCallable.call();
			return null;
		}).when(spyTargetPdkDataNode).executeDataFuncAspect(any(), any(), any());

		doAnswer(answer -> {
			Callable aspectCallable = answer.getArgument(1);
			CommonUtils.AnyErrorConsumer anyErrorConsumer = answer.getArgument(2);
			aspectCallable.call();
			anyErrorConsumer.accept(null);
			return null;
		}).when(spyTargetPdkDataNode).executeDataFuncAspect(any(), any(), any());

		TapTable table = new TapTable();
		table.setId("test");
		table.setName("test");
		table.setPartitionInfo(new TapPartition());
		spyTargetPdkDataNode.dropTable(ExistsDataProcessEnum.DROP_TABLE, table, true);

		verify(spyTargetPdkDataNode, times(1)).executeDataFuncAspect(any(), any(), any());
	}

	@Test
	void testWriteDDL() {
		DataProcessorContext context = mock(DataProcessorContext.class);
		TaskDto taskDto = new TaskDto();
		taskDto.setSyncType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
		when(context.getTaskDto()).thenReturn(taskDto);

		Node node = new DatabaseNode();
		node.setId("test");
		node.setName("test");
		when(context.getNode()).thenReturn(node);

		HazelcastTargetPdkDataNode targetPdkDataNode = new HazelcastTargetPdkDataNode(context);
		targetPdkDataNode.syncTargetPartitionTableEnable = true;
		targetPdkDataNode.uploadDagService = new AtomicBoolean();
		ClassHandlers ddlEventHandlers = mock(ClassHandlers.class);
		ObsLogger obsLogger = mock(ObsLogger.class);
		ReflectionTestUtils.setField(targetPdkDataNode, "ddlEventHandlers", ddlEventHandlers);
		ReflectionTestUtils.setField(targetPdkDataNode, "obsLogger", obsLogger);


		List<TapEvent> events = new ArrayList<>();
		TapCreateTableEvent createEvent = new TapCreateTableEvent();
		createEvent.setTableId("test");
		createEvent.setTable(new TapTable());
		events.add(createEvent);
		createEvent = new TapCreateTableEvent();
		createEvent.setTableId("test1");
		TapTable partitionTable = new TapTable();
		partitionTable.setId("test_2");
		partitionTable.setName("test_2");
		partitionTable.setPartitionMasterTableId("test");
		partitionTable.setPartitionInfo(new TapPartition());
		createEvent.setTable(partitionTable);
		events.add(createEvent);
		TapDropTableEvent dropEvent = new TapDropTableEvent();
		dropEvent.setTableId("test_1");
		events.add(dropEvent);

		targetPdkDataNode.writeDDL(events);

		targetPdkDataNode.syncTargetPartitionTableEnable = false;
		targetPdkDataNode.writeDDL(events);

		verify(ddlEventHandlers,times(5)).handle(any());
	}

	@Nested
	class testWriteRecord {

		private HazelcastTargetPdkDataNode targetDataNode;
		private DataProcessorContext context;
		private TapTableMap<String, TapTable> tableMap;
		private ISyncMetricCollector syncMetricCollector;

		@BeforeEach
		void beforeEach() {
			context = mock(DataProcessorContext.class);
			TaskDto taskDto = new TaskDto();
			taskDto.setSyncType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			taskDto.setType(SyncTypeEnum.INITIAL_SYNC.getSyncType());
			when(context.getTaskDto()).thenReturn(taskDto);

			Node node = new DatabaseNode();
			node.setId("test");
			node.setName("test");
			when(context.getNode()).thenReturn(node);

			tableMap = TapTableMap.create("nodeId");
			when(context.getTapTableMap()).thenReturn(tableMap);

			targetDataNode = new HazelcastTargetPdkDataNode(context);
			ReflectionTestUtils.setField(targetDataNode, "partitionTapTables", new HashMap<>());
			syncMetricCollector = mock(ISyncMetricCollector.class);;
			targetDataNode.syncMetricCollector = syncMetricCollector;
		}

		@Test
		void testWriteRecord() {

			List<TapEvent> events = Stream.generate(() -> {
				TapInsertRecordEvent e = new TapInsertRecordEvent();
				e.setTableId("test");
				e.setPartitionMasterTableId("test_1");
				return e;
			}).limit(5).collect(Collectors.toList());

			TapTable table = new TapTable();
			table.setId("test");
			table.setName("test");
			table.setPartitionMasterTableId("test");
			table.setPartitionInfo(new TapPartition());
			tableMap.putNew("test", table, "node_id_test");

			HazelcastTargetPdkDataNode spyTargetDataNode = spy(targetDataNode);
			doNothing().when(spyTargetDataNode).handleTapTablePrimaryKeys(any());
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			WriteRecordFunction writeRecord = new WriteRecordFunction() {
				@Override
				public void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> recordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {

				}
			};
			when(connectorFunctions.getWriteRecordFunction()).thenReturn(writeRecord);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			when(connectorNode.getDagId()).thenReturn("test");
			when(connectorNode.getAssociateId()).thenReturn("test");
			TapNodeInfo tapNodeInfo = new TapNodeInfo();
			tapNodeInfo.setTapNodeSpecification(new TapNodeSpecification());
			tapNodeInfo.getTapNodeSpecification().setGroup("mysql");
			tapNodeInfo.getTapNodeSpecification().setVersion("v1.0.0");
			when(connectorNode.getTapNodeInfo()).thenReturn(tapNodeInfo);
			when(spyTargetDataNode.getConnectorNode()).thenReturn(connectorNode);
			PDKMethodInvoker pdkMethodInvoker = mock(PDKMethodInvoker.class);
			doReturn(pdkMethodInvoker).when(spyTargetDataNode).createPdkMethodInvoker();

			doAnswer(answer -> {
				Callable aspectCallable = answer.getArgument(1);
				aspectCallable.call();
				return null;
			}).when(spyTargetDataNode).executeDataFuncAspect(any(), any(), any());

			Assertions.assertDoesNotThrow(() -> {
				spyTargetDataNode.writeRecord(events);
			});
			verify(syncMetricCollector, times(1)).log(anyList());

		}
	}
}

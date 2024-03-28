package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.BaseTaskTest;
import com.tapdata.entity.task.ExistsDataProcessEnum;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import io.tapdata.aspect.TableInitFuncAspect;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.exception.TapCodeException;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.GetTableInfoFunction;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.target.CreateIndexFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryIndexesFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.IntStream;

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
			doCallRealMethod().when(hazelcastTargetPdkDataNode).createTable(tapTableMap,funcAspect,node,existsDataProcessEnum,tableId);
		}
		@Test
		@DisplayName("test createTable method when tapTable is null")
		void testCreateTable1(){
			assertThrows(TapCodeException.class, ()->hazelcastTargetPdkDataNode.createTable(tapTableMap,funcAspect,node,existsDataProcessEnum,tableId));
		}
		@Test
		@DisplayName("test createTable method when tableId equals TAP_EXACTLY_ONCE_CACHE")
		void testCreateTable2(){
			tableId = "_TAP_EXACTLY_ONCE_CACHE";
			when(tapTableMap.get(tableId)).thenReturn(mock(TapTable.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).createTable(tapTableMap,funcAspect,node,existsDataProcessEnum,tableId);
			hazelcastTargetPdkDataNode.createTable(tapTableMap,funcAspect,node,existsDataProcessEnum,tableId);
			verify(hazelcastTargetPdkDataNode,new Times(0)).syncIndex(anyString(),any(TapTable.class),anyBoolean());
		}
		@Test
		@DisplayName("test createTable method normal")
		void testCreateTable3(){
			when(tapTableMap.get(tableId)).thenReturn(mock(TapTable.class));
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode,"writeStrategy","appendWrite");
			when(funcAspect.state(TableInitFuncAspect.STATE_PROCESS)).thenReturn(mock(TableInitFuncAspect.class));
			hazelcastTargetPdkDataNode.createTable(tapTableMap,funcAspect,node,existsDataProcessEnum,tableId);
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
			doThrow(new TapCodeException("test error")).when(queryIndexesFunction).query(any(TapConnectorContext.class),any(TapTable.class),any(Consumer.class));
			doCallRealMethod().when(hazelcastTargetPdkDataNode).syncIndex(tableId, tapTable, autoCreateTable);
			assertThrows(TapCodeException.class, ()->hazelcastTargetPdkDataNode.syncIndex(tableId, tapTable, autoCreateTable));
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
}

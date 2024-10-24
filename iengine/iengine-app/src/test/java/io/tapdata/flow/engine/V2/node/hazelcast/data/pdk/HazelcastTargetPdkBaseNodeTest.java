package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import cn.hutool.core.collection.ConcurrentHashSet;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Processor;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.dataflow.TableBatchReadStatus;
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import com.tapdata.entity.task.ExistsDataProcessEnum;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DmlPolicy;
import com.tapdata.tm.commons.dag.DmlPolicyEnum;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.UnwindProcessNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import io.tapdata.aspect.CreateTableFuncAspect;
import io.tapdata.aspect.DropTableFuncAspect;
import io.tapdata.aspect.supervisor.DataNodeThreadGroupAspect;
import io.tapdata.aspect.taskmilestones.WriteErrorAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.error.TapdataEventException;
import io.tapdata.error.TaskTargetProcessorExCode_15;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.common.TapdataEventsRunner;
import io.tapdata.flow.engine.V2.exactlyonce.ExactlyOnceUtil;
import io.tapdata.flow.engine.V2.exactlyonce.write.CheckExactlyOnceWriteEnableResult;
import io.tapdata.flow.engine.V2.exactlyonce.write.ExactlyOnceWriteCleanerEntity;
import io.tapdata.flow.engine.V2.monitor.impl.JetJobStatusMonitor;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.PartitionConcurrentProcessor;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner.Partitioner;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.metric.collector.ISyncMetricCollector;
import io.tapdata.metric.collector.SyncMetricCollector;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateIndexFunction;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableV2Function;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.async.AsyncUtils;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.supervisor.TaskNodeInfo;
import io.tapdata.supervisor.TaskResourceSupervisorManager;
import io.tapdata.utils.UnitTestUtils;
import lombok.SneakyThrows;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.beans.BeanUtils;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-01-22 18:46
 **/
@DisplayName("HazelcastTargetPdkBaseNode Class Test")
class HazelcastTargetPdkBaseNodeTest extends BaseHazelcastNodeTest {
	private HazelcastTargetPdkBaseNode hazelcastTargetPdkBaseNode;

	@BeforeEach
	void setUp() {
		hazelcastTargetPdkBaseNode = mock(HazelcastTargetPdkBaseNode.class);
		when(hazelcastTargetPdkBaseNode.getDataProcessorContext()).thenReturn(dataProcessorContext);
	}

	@Nested
	@DisplayName("fromTapValueMergeInfo method test")
	class fromTapValueMergeInfoTest {
		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).fromTapValueMergeInfo(any());
		}

		@Test
		@DisplayName("main process test")
		void testMainProcess() {
			MergeInfo mergeInfo = new MergeInfo();
			TapdataEvent tapdataEvent = new TapdataEvent();
			TapEvent tapEvent = mock(TapEvent.class);
			when(tapEvent.getInfo(MergeInfo.EVENT_INFO_KEY)).thenReturn(mergeInfo);
			tapdataEvent.setTapEvent(tapEvent);

			hazelcastTargetPdkBaseNode.fromTapValueMergeInfo(tapdataEvent);
			verify(hazelcastTargetPdkBaseNode, times(1)).recursiveMergeInfoTransformFromTapValue(any());
		}

		@Test
		@DisplayName("when tapEvent not have mergeInfo")
		void notHaveMergeInfo() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			TapEvent tapEvent = mock(TapEvent.class);
			when(tapEvent.getInfo(MergeInfo.EVENT_INFO_KEY)).thenReturn(null);
			tapdataEvent.setTapEvent(tapEvent);

			hazelcastTargetPdkBaseNode.fromTapValueMergeInfo(tapdataEvent);
			verify(hazelcastTargetPdkBaseNode, times(0)).recursiveMergeInfoTransformFromTapValue(any());
		}
	}

	@Nested
	@DisplayName("recursiveMergeInfoTransformFromTapValue method test")
	class recursiveMergeInfoTransformFromTapValueTest {
		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).recursiveMergeInfoTransformFromTapValue(any());
		}

		@Test
		@DisplayName("main process test")
		void testMainProcess() {
			List<MergeLookupResult> mergeLookupResults = new ArrayList<>();
			MergeLookupResult mergeLookupResult = new MergeLookupResult();
			Map<String, Object> data = new HashMap<>();
			data.put("_id", 1);
			mergeLookupResult.setData(data);
			mergeLookupResults.add(mergeLookupResult);
			TapTable tapTable = new TapTable();
			mergeLookupResult.setTapTable(tapTable);
			List<MergeLookupResult> childMergeLookupResults = new ArrayList<>();
			MergeLookupResult childMergeLookupResult = new MergeLookupResult();
			Map<String, Object> childData = new HashMap<>(data);
			childMergeLookupResult.setData(childData);
			childMergeLookupResults.add(childMergeLookupResult);
			childMergeLookupResult.setTapTable(tapTable);
			TapCodecsFilterManager tapCodecsFilterManager = mock(TapCodecsFilterManager.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "codecsFilterManager", tapCodecsFilterManager);
			mergeLookupResult.setMergeLookupResults(childMergeLookupResults);

			hazelcastTargetPdkBaseNode.recursiveMergeInfoTransformFromTapValue(mergeLookupResults);
			verify(hazelcastTargetPdkBaseNode, times(2)).fromTapValue(data, tapCodecsFilterManager, tapTable);
		}

		@Test
		@DisplayName("when mergeLookupResults is empty")
		void mergeLookupResultsIsEmpty() {
			List<MergeLookupResult> mergeLookupResults = new ArrayList<>();

			hazelcastTargetPdkBaseNode.recursiveMergeInfoTransformFromTapValue(mergeLookupResults);
			verify(hazelcastTargetPdkBaseNode, times(0)).fromTapValue(any(Map.class), any(TapCodecsFilterManager.class), any(TapTable.class));
		}
	}

	@DisplayName("test ignorePksAndIndices normal")
	@Test
	void ignorePksAndIndicesTest1() {
		TapTable tapTable = new TapTable();
		TapField field = getField("_id");
		TapField index = getField("index");
		tapTable.add(field);
		tapTable.add(index);
		List<String> list = Arrays.asList("_id", "index");

		HazelcastTargetPdkBaseNode.ignorePksAndIndices(tapTable, list);
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		TapField idField = nameFieldMap.get("_id");
		TapField indexField = nameFieldMap.get("index");
		assertEquals(2, nameFieldMap.size());
		assertEquals(0, idField.getPrimaryKeyPos());
		assertEquals(0, indexField.getPrimaryKeyPos());
		assertEquals(false, indexField.getPrimaryKey());
		assertEquals(false, idField.getPrimaryKey());
	}

	@DisplayName("test ignorePksAndIndices logic primary key is null")
	@Test
	void ignorePksAndIndicesTest2() {
		TapTable tapTable = new TapTable();
		TapField field = getField("_id");
		TapField index = getField("index");
		tapTable.add(field);
		tapTable.add(index);
		HazelcastTargetPdkBaseNode.ignorePksAndIndices(tapTable, null);
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		TapField idField = nameFieldMap.get("_id");
		TapField indexField = nameFieldMap.get("index");
		assertEquals(2, nameFieldMap.size());
		assertEquals(0, idField.getPrimaryKeyPos());
		assertEquals(0, indexField.getPrimaryKeyPos());
		assertEquals(false, indexField.getPrimaryKey());
		assertEquals(false, idField.getPrimaryKey());
	}

	public TapField getField(String name) {
		TapField field = new TapField();
		field.setName(name);
		return field;
	}

	@Nested
	class usePkAsUpdateConditionsTest {
		@BeforeEach
		void setUp() {
			when(hazelcastTargetPdkBaseNode.usePkAsUpdateConditions(any(), any())).thenCallRealMethod();
		}

		@DisplayName("test pk as update conditions same1")
		@Test
		void usePkAsUpdateConditionsTest1() {
			assertTrue(hazelcastTargetPdkBaseNode.usePkAsUpdateConditions(Collections.emptyList(), null));
		}

		@DisplayName("test pk as update conditions same2")
		@Test
		void usePkAsUpdateConditionsTest2() {
			assertTrue(hazelcastTargetPdkBaseNode.usePkAsUpdateConditions(Lists.newArrayList("A", "B"), Lists.newArrayList("A", "B")));
		}

		@DisplayName("test pk as update conditions different1")
		@Test
		void usePkAsUpdateConditionsTest3() {
			assertFalse(hazelcastTargetPdkBaseNode.usePkAsUpdateConditions(Lists.newArrayList("A", "B"), Lists.newArrayList("C")));
		}

		@DisplayName("test pk as update conditions different2")
		@Test
		void usePkAsUpdateConditionsTest4() {
			assertFalse(hazelcastTargetPdkBaseNode.usePkAsUpdateConditions(Lists.newArrayList("A", "B", "C"), Lists.newArrayList("A", "B")));
		}
	}

	@Nested
	class createTableTest {

		DataProcessorContext dataProcessorContext;

		@BeforeEach
		void setUp() {
			dataProcessorContext = mock(DataProcessorContext.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "dataProcessorContext", dataProcessorContext);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "clientMongoOperator", mockClientMongoOperator);
		}

		@Test
		void testIsUnwindProcess() {
			hazelcastTargetPdkBaseNode.unwindProcess = true;
			TapTable tapTable = mock(TapTable.class);
			AtomicBoolean atomicBoolean = new AtomicBoolean(false);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).createTable(tapTable, atomicBoolean, true);
			TableNode node = new TableNode();
			node.setDisabled(false);
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn((Node) node);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkBaseNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			when(connectorFunctions.getCreateTableFunction()).thenReturn(null);
			when(connectorFunctions.getCreateTableV2Function()).thenReturn(mock(CreateTableV2Function.class));
			Connections connections = new Connections();
			connections.setId("test");
			when(dataProcessorContext.getTargetConn()).thenReturn(connections);
			boolean result = hazelcastTargetPdkBaseNode.createTable(tapTable, atomicBoolean, true);
			Assertions.assertTrue(result);
		}

		@Test
		void testIsNotUnwindProcess() {
			hazelcastTargetPdkBaseNode.unwindProcess = false;
			TapTable tapTable = mock(TapTable.class);
			AtomicBoolean atomicBoolean = new AtomicBoolean(false);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).createTable(tapTable, atomicBoolean, true);
			TableNode node = new TableNode();
			node.setDisabled(false);
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn((Node) node);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkBaseNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			when(connectorFunctions.getCreateTableFunction()).thenReturn(null);
			when(connectorFunctions.getCreateTableV2Function()).thenReturn(mock(CreateTableV2Function.class));
			Connections connections = new Connections();
			connections.setId("test");
			when(dataProcessorContext.getTargetConn()).thenReturn(connections);
			boolean result = hazelcastTargetPdkBaseNode.createTable(tapTable, atomicBoolean, true);
			Assertions.assertTrue(result);
		}

		@Test
		void testCreateTableIsNull() {
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "unwindProcess", false);
			TapTable tapTable = new TapTable();
			tapTable.setId("test");
			AtomicBoolean succeed = new AtomicBoolean(true);
			Node node = mock(Node.class);
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn(node);
			when(node.disabledNode()).thenReturn(false);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkBaseNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getCreateTableFunction()).thenReturn(null);
			when(functions.getCreateTableV2Function()).thenReturn(null);
			when(dataProcessorContext.getTargetConn()).thenReturn(mock(Connections.class));
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).createTable(tapTable, succeed, true);
			hazelcastTargetPdkBaseNode.createTable(tapTable, succeed, true);
			verify(hazelcastTargetPdkBaseNode, new Times(0)).buildErrorConsumer("test");
		}

		@Test
		void testCreateTableForBuildErrorConsumer() {
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "unwindProcess", false);
			TapTable tapTable = new TapTable();
			tapTable.setId("test");
			AtomicBoolean succeed = new AtomicBoolean(true);
			Node node = mock(Node.class);
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn(node);
			when(node.disabledNode()).thenReturn(false);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkBaseNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getCreateTableFunction()).thenReturn(mock(CreateTableFunction.class));
			when(dataProcessorContext.getTargetConn()).thenReturn(mock(Connections.class));
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).executeDataFuncAspect(any(Class.class), any(Callable.class), any(CommonUtils.AnyErrorConsumer.class));
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).createTable(tapTable, succeed, true);
			hazelcastTargetPdkBaseNode.createTable(tapTable, succeed, true);
			verify(hazelcastTargetPdkBaseNode, new Times(1)).buildErrorConsumer("test");
		}

		@Test
		void createTableTestForInit() {
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "unwindProcess", false);
			TapTable tapTable = new TapTable();
			tapTable.setId("test");
			AtomicBoolean succeed = new AtomicBoolean(true);
			Node node = mock(Node.class);
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn(node);
			when(node.disabledNode()).thenReturn(false);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkBaseNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getCreateTableFunction()).thenReturn(mock(CreateTableFunction.class));
			when(dataProcessorContext.getTargetConn()).thenReturn(mock(Connections.class));
			when(hazelcastTargetPdkBaseNode.executeDataFuncAspect(any(Class.class), any(Callable.class), any(CommonUtils.AnyErrorConsumer.class))).thenAnswer(a -> {
				Callable<?> callable = a.getArgument(1, Callable.class);
				CommonUtils.AnyErrorConsumer<CreateTableFuncAspect> errorConsumer = a.getArgument(2, CommonUtils.AnyErrorConsumer.class);
				Object call = callable.call();

				Assertions.assertNotNull(call);
				Assertions.assertTrue(((CreateTableFuncAspect) call).isInit());
				return null;
			});
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).createTable(tapTable, succeed, true);
			hazelcastTargetPdkBaseNode.createTable(tapTable, succeed, true);
		}

		@Test
		void createTableFalseTestForInit() {
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "unwindProcess", false);
			TapTable tapTable = new TapTable();
			tapTable.setId("test");
			AtomicBoolean succeed = new AtomicBoolean(true);
			Node node = mock(Node.class);
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn(node);
			when(node.disabledNode()).thenReturn(false);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkBaseNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getCreateTableFunction()).thenReturn(null);
			when(dataProcessorContext.getTargetConn()).thenReturn(mock(Connections.class));
			try (MockedStatic<AspectUtils> aspectUtilsMockedStatic = Mockito.mockStatic(AspectUtils.class)) {
				aspectUtilsMockedStatic.when(() -> AspectUtils.executeAspect(any())).then(a -> {
					CreateTableFuncAspect createTableFuncAspect = a.getArgument(0);
					Assertions.assertTrue(createTableFuncAspect.isInit());
					return null;
				});

				doCallRealMethod().when(hazelcastTargetPdkBaseNode).createTable(tapTable, succeed, true);
				hazelcastTargetPdkBaseNode.createTable(tapTable, succeed, true);

			}
		}

		@Test
		void dropTableTestForInit() {
			TaskDto taskDto = new TaskDto();
			taskDto.setType("initial_sync");
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
			HazelcastTargetPdkDataNode hazelcastTargetPdkDataNode = mock(HazelcastTargetPdkDataNode.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode, "dataProcessorContext", dataProcessorContext);
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode, "clientMongoOperator", mockClientMongoOperator);
			TapTable tapTable = new TapTable();
			tapTable.setId("test");
			Node node = mock(Node.class);
			when(hazelcastTargetPdkDataNode.getNode()).thenReturn(node);
			when(node.disabledNode()).thenReturn(false);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getDropTableFunction()).thenReturn(mock(DropTableFunction.class));
			when(dataProcessorContext.getTargetConn()).thenReturn(mock(Connections.class));


			when(hazelcastTargetPdkDataNode.executeDataFuncAspect(any(Class.class), any(Callable.class), any(CommonUtils.AnyErrorConsumer.class))).thenAnswer(a -> {
				Callable<?> callable = a.getArgument(1, Callable.class);
				CommonUtils.AnyErrorConsumer<DropTableFuncAspect> errorConsumer = a.getArgument(2, CommonUtils.AnyErrorConsumer.class);
				Object call = callable.call();

				Assertions.assertNotNull(call);
				Assertions.assertTrue(((DropTableFuncAspect) call).isInit());
				return null;
			});
			ExistsDataProcessEnum existsDataProcessEnum = ExistsDataProcessEnum.DROP_TABLE;
			doCallRealMethod().when(hazelcastTargetPdkDataNode).dropTable(existsDataProcessEnum, "test", true);
			hazelcastTargetPdkDataNode.dropTable(existsDataProcessEnum, "test", true);
		}

		@Test
		void dropTableFalseTestForInit() {
			TaskDto taskDto = new TaskDto();
			taskDto.setType("initial_sync");
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
			HazelcastTargetPdkDataNode hazelcastTargetPdkDataNode = mock(HazelcastTargetPdkDataNode.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode, "dataProcessorContext", dataProcessorContext);
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode, "clientMongoOperator", mockClientMongoOperator);
			TapTable tapTable = new TapTable();
			tapTable.setId("test");
			Node node = mock(Node.class);
			when(hazelcastTargetPdkDataNode.getNode()).thenReturn(node);
			when(node.disabledNode()).thenReturn(false);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			when(hazelcastTargetPdkDataNode.getConnectorNode()).thenReturn(connectorNode);
			ConnectorFunctions functions = mock(ConnectorFunctions.class);
			when(connectorNode.getConnectorFunctions()).thenReturn(functions);
			when(functions.getDropTableFunction()).thenReturn(null);
			when(dataProcessorContext.getTargetConn()).thenReturn(mock(Connections.class));


			try (MockedStatic<AspectUtils> aspectUtilsMockedStatic = Mockito.mockStatic(AspectUtils.class)) {
				aspectUtilsMockedStatic.when(() -> AspectUtils.executeAspect(any())).then(a -> {
					DropTableFuncAspect actualData = a.getArgument(0);
					Assertions.assertTrue(actualData.isInit());
					return null;
				});

				ExistsDataProcessEnum existsDataProcessEnum = ExistsDataProcessEnum.DROP_TABLE;
				doCallRealMethod().when(hazelcastTargetPdkDataNode).dropTable(existsDataProcessEnum, "test", true);
				hazelcastTargetPdkDataNode.dropTable(existsDataProcessEnum, "test", true);

			}

		}
	}

	@Nested
	class checkUnwindConfigurationTest {
		DataProcessorContext dataProcessorContext;
		ObsLogger obsLogger;

		@BeforeEach
		void setUp() {
			dataProcessorContext = mock(DataProcessorContext.class);
			obsLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "dataProcessorContext", dataProcessorContext);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "obsLogger", obsLogger);

		}

		@Test
		void test() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).checkUnwindConfiguration();
			TaskDto taskDto1 = mock(TaskDto.class);
			DAG dag = mock(DAG.class);
			taskDto1.setDag(dag);
			List<Node> nodes = new ArrayList<>();
			nodes.add(mock(UnwindProcessNode.class));
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto1);
			when(taskDto1.getDag()).thenReturn(dag);
			when(dag.getNodes()).thenReturn(nodes);
			TableNode node = new TableNode();
			DmlPolicy dmlPolicy = new DmlPolicy();
			dmlPolicy.setInsertPolicy(DmlPolicyEnum.just_insert);
			node.setDmlPolicy(dmlPolicy);
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn((Node) node);
			hazelcastTargetPdkBaseNode.checkUnwindConfiguration();
			Assertions.assertTrue(hazelcastTargetPdkBaseNode.unwindProcess);
		}

		@Test
		void testGetNodeIsNotTableNode() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).checkUnwindConfiguration();
			TaskDto taskDto1 = mock(TaskDto.class);
			DAG dag = mock(DAG.class);
			taskDto1.setDag(dag);
			List<Node> nodes = new ArrayList<>();
			nodes.add(mock(UnwindProcessNode.class));
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto1);
			when(taskDto1.getDag()).thenReturn(dag);
			when(dag.getNodes()).thenReturn(nodes);
			DatabaseNode node = new DatabaseNode();
			DmlPolicy dmlPolicy = new DmlPolicy();
			dmlPolicy.setInsertPolicy(DmlPolicyEnum.insert_on_nonexists);
			node.setDmlPolicy(dmlPolicy);
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn((Node) node);
			hazelcastTargetPdkBaseNode.checkUnwindConfiguration();
			verify(obsLogger, times(0)).warn(any());
		}

		@Test
		void testInsertPolicyIsNullJustInsert() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).checkUnwindConfiguration();
			TaskDto taskDto1 = mock(TaskDto.class);
			DAG dag = mock(DAG.class);
			taskDto1.setDag(dag);
			List<Node> nodes = new ArrayList<>();
			nodes.add(mock(UnwindProcessNode.class));
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto1);
			when(taskDto1.getDag()).thenReturn(dag);
			when(dag.getNodes()).thenReturn(nodes);
			TableNode node = new TableNode();
			DmlPolicy dmlPolicy = new DmlPolicy();
			dmlPolicy.setInsertPolicy(DmlPolicyEnum.insert_on_nonexists);
			node.setDmlPolicy(dmlPolicy);
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn((Node) node);
			hazelcastTargetPdkBaseNode.checkUnwindConfiguration();
			verify(obsLogger, times(1)).warn(any());
		}

		@Test
		void testDmlPolicyIsNull() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).checkUnwindConfiguration();
			TaskDto taskDto1 = mock(TaskDto.class);
			DAG dag = mock(DAG.class);
			taskDto1.setDag(dag);
			List<Node> nodes = new ArrayList<>();
			nodes.add(mock(UnwindProcessNode.class));
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto1);
			when(taskDto1.getDag()).thenReturn(dag);
			when(dag.getNodes()).thenReturn(nodes);
			TableNode node = new TableNode();
			node.setDmlPolicy(null);
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn((Node) node);
			hazelcastTargetPdkBaseNode.checkUnwindConfiguration();
			verify(obsLogger, times(1)).warn(any());
		}
	}

	@Nested
	class IsCDCConcurrentTest {
		ObsLogger obsLogger;

		@BeforeEach
		void setUp() {
			obsLogger = mock(ObsLogger.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "cdcConcurrentWriteNum", 8);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "obsLogger", obsLogger);
		}

		@Test
		void testUnwindProcess() {
			TableNode node = mock(TableNode.class);
			List predecessors = new ArrayList<>();
			UnwindProcessNode unwindProcessNode = new UnwindProcessNode();
			predecessors.add(unwindProcessNode);
			when(node.predecessors()).thenReturn(predecessors);
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn((Node) node);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).isCDCConcurrent(true);
			boolean result = hazelcastTargetPdkBaseNode.isCDCConcurrent(true);
			Assertions.assertFalse(result);
			verify(obsLogger, times(1)).info(any());
		}

		@Test
		void testMergeTableNode() {
			TableNode node = mock(TableNode.class);
			List predecessors = new ArrayList<>();
			MergeTableNode mergeTableNode = new MergeTableNode();
			predecessors.add(mergeTableNode);
			when(node.predecessors()).thenReturn(predecessors);
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn((Node) node);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).isCDCConcurrent(true);
			boolean result = hazelcastTargetPdkBaseNode.isCDCConcurrent(true);
			Assertions.assertFalse(result);
			verify(obsLogger, times(1)).info(any());
		}
	}

	@Nested
	class HandleTapdataShareLogEventTest {
		List<TapdataShareLogEvent> tapdataShareLogEvents;
		TapdataEvent tapdataEvent;
		Consumer<TapdataEvent> consumer;

		@Test
		@DisplayName("test handleTapdataShareLogEvent method for TapRecordEvent")
		void test1() {
			tapdataShareLogEvents = new ArrayList<>();
			tapdataEvent = new TapdataShareLogEvent();
			TapRecordEvent event = new TapInsertRecordEvent();
			tapdataEvent.setTapEvent(event);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).handleTapdataShareLogEvent(tapdataShareLogEvents, tapdataEvent, consumer);
			hazelcastTargetPdkBaseNode.handleTapdataShareLogEvent(tapdataShareLogEvents, tapdataEvent, consumer);
			verify(hazelcastTargetPdkBaseNode).replaceIllegalDateWithNullIfNeed(any(TapRecordEvent.class));
		}
	}

	@Nested
	class HandleTapdataRecordEventTest {
		private TapdataEvent tapdataEvent = new TapdataEvent();

		@BeforeEach
		void beforeEach() {
		}

		@Test
		@DisplayName("test handleTapdataRecordEvent method for TapUpdateRecordEvent")
		void test1() {
			String writeStrategy = "appendWrite";
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "writeStrategy", writeStrategy);
			TapRecordEvent tapRecordEvent = new TapUpdateRecordEvent();
			Map<String, Object> after = new HashMap<>();
			((TapUpdateRecordEvent) tapRecordEvent).setAfter(after);
			tapdataEvent.setTapEvent(tapRecordEvent);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).handleTapdataRecordEvent(tapdataEvent);
			TapRecordEvent result = hazelcastTargetPdkBaseNode.handleTapdataRecordEvent(tapdataEvent);
			assertInstanceOf(TapInsertRecordEvent.class, result);
		}

		@Test
		@DisplayName("test handleTapdataRecordEvent method for TapDeleteRecordEvent")
		void test2() {
			String writeStrategy = "appendWrite";
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "writeStrategy", writeStrategy);
			TapRecordEvent tapRecordEvent = new TapDeleteRecordEvent();
			Map<String, Object> after = new HashMap<>();
			((TapDeleteRecordEvent) tapRecordEvent).setBefore(after);
			tapdataEvent.setTapEvent(tapRecordEvent);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).handleTapdataRecordEvent(tapdataEvent);
			TapRecordEvent result = hazelcastTargetPdkBaseNode.handleTapdataRecordEvent(tapdataEvent);
			assertInstanceOf(TapInsertRecordEvent.class, result);
		}
	}

	@Nested
	class ReplaceIllegalDateWithNullIfNeedTest {
		private TapRecordEvent event;
		private List<Capability> capabilities;
		private Connections connections;

		@BeforeEach
		void beforeEach() {
			dataProcessorContext = mock(DataProcessorContext.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "dataProcessorContext", dataProcessorContext);
			connections = mock(Connections.class);
			when(dataProcessorContext.getConnections()).thenReturn(connections);
			capabilities = new ArrayList<>();
			Capability capability = mock(Capability.class);
			capabilities.add(capability);
			when(connections.getCapabilities()).thenReturn(capabilities);
			when(capability.getId()).thenReturn(ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS);
		}

		@Test
		@DisplayName("test replaceIllegalDateWithNullIfNeed method when containsIllegalDate is false")
		void test1() {
			event = new TapInsertRecordEvent();
			event.setContainsIllegalDate(true);
			Map<String, Object> after = new HashMap<>();
			after.put("id", "1");
			after.put("name", "test");
			((TapInsertRecordEvent) event).setAfter(after);
			event.setContainsIllegalDate(false);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).replaceIllegalDateWithNullIfNeed(event);
			hazelcastTargetPdkBaseNode.replaceIllegalDateWithNullIfNeed(event);
			assertEquals(after, ((TapInsertRecordEvent) event).getAfter());
		}

		@Test
		@DisplayName("test replaceIllegalDateWithNullIfNeed method when containsIllegalDate is true for TapInsertRecordEvent")
		void test2() {
			event = new TapInsertRecordEvent();
			event.setContainsIllegalDate(true);
			Map<String, Object> after = new HashMap<>();
			TapDateTimeValue tapDateTimeValue = new TapDateTimeValue();
			DateTime dateTime = new DateTime("0000-00-00 00:00:00", DateTime.DATETIME_TYPE);
			tapDateTimeValue.setValue(dateTime);
			after.put("id", "1");
			after.put("name", "test");
			after.put("date", tapDateTimeValue);
			after.put("last_date", tapDateTimeValue);
			((TapInsertRecordEvent) event).setAfter(after);
			event.setContainsIllegalDate(true);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).replaceIllegalDateWithNullIfNeed(event);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).replaceIllegalDate(any());
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).replaceIllegalDateTime2Null(any(),any());
			hazelcastTargetPdkBaseNode.replaceIllegalDateWithNullIfNeed(event);
			assertNull(((TapInsertRecordEvent) event).getAfter().get("date"));
			assertNull(((TapInsertRecordEvent) event).getAfter().get("last_date"));
		}

		@Test
		@DisplayName("test replaceIllegalDateWithNullIfNeed method when containsIllegalDate is true for TapUpdateRecordEvent")
		void test3() {
			event = new TapUpdateRecordEvent();
			event.setContainsIllegalDate(true);
			Map<String, Object> after = new HashMap<>();
			TapDateTimeValue tapDateTimeValue = new TapDateTimeValue();
			DateTime dateTime = new DateTime("0000-00-00 00:00:00", DateTime.DATETIME_TYPE);
			tapDateTimeValue.setValue(dateTime);
			after.put("id", "1");
			after.put("name", "test");
			after.put("date", tapDateTimeValue);
			((TapUpdateRecordEvent) event).setBefore(after);
			((TapUpdateRecordEvent) event).setAfter(after);
			event.setContainsIllegalDate(true);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).replaceIllegalDateWithNullIfNeed(event);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).replaceIllegalDate(any());
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).replaceIllegalDateTime2Null(any(),any());
			hazelcastTargetPdkBaseNode.replaceIllegalDateWithNullIfNeed(event);
			assertNull(((TapUpdateRecordEvent) event).getBefore().get("date"));
			assertNull(((TapUpdateRecordEvent) event).getAfter().get("date"));
		}

		@Test
		@DisplayName("test replaceIllegalDateWithNullIfNeed method when containsIllegalDate is true for TapDeleteRecordEvent")
		void test4() {
			event = new TapDeleteRecordEvent();
			event.setContainsIllegalDate(true);
			Map<String, Object> before = new HashMap<>();
			((TapDeleteRecordEvent) event).setBefore(before);
			event.setContainsIllegalDate(true);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).replaceIllegalDateWithNullIfNeed(event);
			hazelcastTargetPdkBaseNode.replaceIllegalDateWithNullIfNeed(event);
			assertEquals(before, ((TapDeleteRecordEvent) event).getBefore());
		}

		@Test
		@DisplayName("test replaceIllegalDateWithNullIfNeed method when illegalDateAcceptable is true")
		void test5() {
			hazelcastTargetPdkBaseNode.illegalDateAcceptable = true;
			event = new TapInsertRecordEvent();
			event.setContainsIllegalDate(true);
			Map<String, Object> after = new HashMap<>();
			after.put("date", new Date());
			after.put("last_date", new Date());
			((TapInsertRecordEvent) event).setAfter(after);
			event.setContainsIllegalDate(true);
			List<String> illegalDateFiledName = new ArrayList<>();
			illegalDateFiledName.add("date");
			illegalDateFiledName.add("last_date");
			((TapInsertRecordEvent) event).setAfterIllegalDateFieldName(illegalDateFiledName);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).replaceIllegalDateWithNullIfNeed(event);
			hazelcastTargetPdkBaseNode.replaceIllegalDateWithNullIfNeed(event);
			assertNotNull(((TapInsertRecordEvent) event).getAfter().get("date"));
			assertNotNull(((TapInsertRecordEvent) event).getAfter().get("last_date"));
		}
	}

	@Nested
	@DisplayName("Method initIllegalDateAcceptable test")
	class initIllegalDateAcceptableTest {

		private Connections connections;

		@BeforeEach
		void setUp() {
			connections = new Connections();
			dataProcessorContext = new DataProcessorContext.DataProcessorContextBuilder()
					.withConnections(connections).build();
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "dataProcessorContext", dataProcessorContext);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).initIllegalDateAcceptable();
		}

		@Test
		@DisplayName("test connector can accept illegal date")
		void testIllegalDateAcceptable() {
			List<Capability> capabilities = new ArrayList<>();
			capabilities.add(new Capability(ConnectionOptions.DML_ILLEGAL_DATE_ACCEPTABLE));
			connections.setCapabilities(capabilities);
			hazelcastTargetPdkBaseNode.initIllegalDateAcceptable();
			assertTrue(hazelcastTargetPdkBaseNode.illegalDateAcceptable);
		}

		@Test
		@DisplayName("test connector cannot accept illegal date")
		void testNotIllegalDateAcceptable() {
			hazelcastTargetPdkBaseNode.initIllegalDateAcceptable();
			assertFalse(hazelcastTargetPdkBaseNode.illegalDateAcceptable);

			List<Capability> capabilities = new ArrayList<>();
			connections.setCapabilities(capabilities);
			hazelcastTargetPdkBaseNode.initIllegalDateAcceptable();
			assertFalse(hazelcastTargetPdkBaseNode.illegalDateAcceptable);
		}
	}

	@Nested
	@DisplayName("Method doInit test")
	class doInitTest {

		private ScheduledExecutorService flushOffsetExecutor;

		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).doInit(any(Processor.Context.class));
			ThreadPoolExecutorEx queueConsumerThreadPool = AsyncUtils.createThreadPoolExecutor("test", 1, new ThreadGroup("test"), HazelcastTargetPdkBaseNodeTest.class.getSimpleName());
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "queueConsumerThreadPool", queueConsumerThreadPool);
			flushOffsetExecutor = new ScheduledThreadPoolExecutor(1, r -> {
				Thread thread = new Thread(r);
				thread.setName("test");
				return thread;
			});
			flushOffsetExecutor = spy(flushOffsetExecutor);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "flushOffsetExecutor", flushOffsetExecutor);
			Node node = new TableNode();
			node.setId("1");
			node.setName("test");
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn(node);
			dataProcessorContext = new DataProcessorContext.DataProcessorContextBuilder().build();
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "dataProcessorContext", dataProcessorContext);
		}

		@Test
		void testMainProcess() {
			try (
					MockedStatic<ISyncMetricCollector> iSyncMetricCollectorMockedStatic = mockStatic(ISyncMetricCollector.class)
			) {
				hazelcastTargetPdkBaseNode.doInit(mock(Processor.Context.class));
				iSyncMetricCollectorMockedStatic.verify(() -> ISyncMetricCollector.init(dataProcessorContext));
				verify(hazelcastTargetPdkBaseNode).doInit(any());
				verify(hazelcastTargetPdkBaseNode).createPdkAndInit(any(Processor.Context.class));
				verify(hazelcastTargetPdkBaseNode).initExactlyOnceWriteIfNeed();
				verify(hazelcastTargetPdkBaseNode).initTargetVariable();
				verify(hazelcastTargetPdkBaseNode).initTargetQueueConsumer();
				verify(hazelcastTargetPdkBaseNode).initTargetConcurrentProcessorIfNeed();
				verify(hazelcastTargetPdkBaseNode).initTapEventFilter();
				verify(hazelcastTargetPdkBaseNode).initIllegalDateAcceptable();
				verify(hazelcastTargetPdkBaseNode).initSyncProgressMap();
				verify(flushOffsetExecutor).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
				assertTrue(Thread.currentThread().getName().startsWith("Target-Process"));
				verify(hazelcastTargetPdkBaseNode).checkUnwindConfiguration();
				verify(hazelcastTargetPdkBaseNode).initCodecsFilterManager();
			}
		}
	}

	@Nested
	@DisplayName("Method initTargetConcurrentProcessorIfNeed test")
	class initTargetConcurrentProcessorIfNeedTest {
		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).initTargetConcurrentProcessorIfNeed();
			TableNode node = new TableNode();
			node.setId("1");
			node.setName("node");
			node.setInitialConcurrent(true);
			node.setCdcConcurrent(true);
			Map<String, List<String>> concurrentWritePartitionMap = new HashMap<>();
			String tableName = "test";
			concurrentWritePartitionMap.put(tableName, new ArrayList<String>() {
				{
					add("id");
				}
			});
			node.setConcurrentWritePartitionMap(concurrentWritePartitionMap);
			when(hazelcastTargetPdkBaseNode.getTgtTableNameFromTapEvent(any(TapEvent.class))).thenReturn(tableName);
			when(hazelcastTargetPdkBaseNode.getNode()).thenReturn((Node) node);
			PartitionConcurrentProcessor initialPartitionConcurrentProcessor = mock(PartitionConcurrentProcessor.class);
			PartitionConcurrentProcessor cdcPartitionConcurrentProcessor = mock(PartitionConcurrentProcessor.class);
			when(hazelcastTargetPdkBaseNode.initInitialConcurrentProcessor(anyInt(), any(Partitioner.class))).thenReturn(initialPartitionConcurrentProcessor);
			when(hazelcastTargetPdkBaseNode.initCDCConcurrentProcessor(anyInt(), any(Function.class))).thenReturn(cdcPartitionConcurrentProcessor);
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			when(hazelcastTargetPdkBaseNode.isCDCConcurrent(true)).thenReturn(true);
			hazelcastTargetPdkBaseNode.initTargetConcurrentProcessorIfNeed();

			assertNotNull(ReflectionTestUtils.getField(hazelcastTargetPdkBaseNode, "initialPartitionConcurrentProcessor"));
			assertNotNull(ReflectionTestUtils.getField(hazelcastTargetPdkBaseNode, "cdcPartitionConcurrentProcessor"));
			assertTrue((Boolean) ReflectionTestUtils.getField(hazelcastTargetPdkBaseNode, "initialConcurrent"));
			assertTrue((Boolean) ReflectionTestUtils.getField(hazelcastTargetPdkBaseNode, "cdcConcurrent"));
		}
	}

	@Nested
	class handleTapTablePrimaryKeysTest {
		private TapTable tapTable;
		private ConcurrentHashMap<String, Boolean> everHandleTapTablePrimaryKeysMap;
		private String writeStrategy = "updateOrInsert";
		protected Map<String, List<String>> updateConditionFieldsMap;
		private List<String> updateConditionFields;

		@BeforeEach
		void beforeEach() {
			everHandleTapTablePrimaryKeysMap = new ConcurrentHashMap<>();
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "everHandleTapTablePrimaryKeysMap", everHandleTapTablePrimaryKeysMap);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "writeStrategy", writeStrategy);
			updateConditionFieldsMap = new HashMap<>();
			tapTable = new TapTable();
			tapTable.setId("test");
			LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap<>();
			TapField primary = new TapField();
			primary.setPrimaryKey(true);
			nameFieldMap.put("primary", primary);
			tapTable.setNameFieldMap(nameFieldMap);
			updateConditionFields = new ArrayList<>();
			updateConditionFields.add("field");
			updateConditionFieldsMap.put("test", updateConditionFields);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "updateConditionFieldsMap", updateConditionFieldsMap);

		}

		@Test
		@DisplayName("test handleTapTablePrimaryKeys method when everHandleTapTablePrimaryKeysMap not contains tapTable")
		void test1() {
			try (MockedStatic<HazelcastTargetPdkBaseNode> mb = Mockito
					.mockStatic(HazelcastTargetPdkBaseNode.class)) {
				mb.when(() -> HazelcastTargetPdkBaseNode.ignorePksAndIndices(tapTable, updateConditionFields)).thenAnswer(invocationOnMock -> {
					return null;
				});
				doCallRealMethod().when(hazelcastTargetPdkBaseNode).handleTapTablePrimaryKeys(tapTable);
				hazelcastTargetPdkBaseNode.handleTapTablePrimaryKeys(tapTable);
				assertTrue(everHandleTapTablePrimaryKeysMap.containsKey("test"));
				assertTrue(everHandleTapTablePrimaryKeysMap.get("test"));
				mb.verify(() -> HazelcastTargetPdkBaseNode.ignorePksAndIndices(tapTable, updateConditionFields), new Times(1));
			}
		}

		@Test
		@DisplayName("test handleTapTablePrimaryKeys method when everHandleTapTablePrimaryKeysMap contains tapTable")
		void test2() {
			try (MockedStatic<HazelcastTargetPdkBaseNode> mb = Mockito
					.mockStatic(HazelcastTargetPdkBaseNode.class)) {
				mb.when(() -> HazelcastTargetPdkBaseNode.ignorePksAndIndices(tapTable, updateConditionFields)).thenAnswer(invocationOnMock -> {
					return null;
				});
				everHandleTapTablePrimaryKeysMap.put("test", true);
				doCallRealMethod().when(hazelcastTargetPdkBaseNode).handleTapTablePrimaryKeys(tapTable);
				hazelcastTargetPdkBaseNode.handleTapTablePrimaryKeys(tapTable);
				assertTrue(everHandleTapTablePrimaryKeysMap.containsKey("test"));
				assertTrue(everHandleTapTablePrimaryKeysMap.get("test"));
				mb.verify(() -> HazelcastTargetPdkBaseNode.ignorePksAndIndices(tapTable, updateConditionFields), new Times(0));
			}
		}
	}

	@Nested
	class InitExactlyOnceWriteIfNeedTest {
		@BeforeEach
		void beforeEach() {
			List<String> exactlyOnceWriteTables = new ArrayList<>();
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "exactlyOnceWriteTables", exactlyOnceWriteTables);
			List<ExactlyOnceWriteCleanerEntity> exactlyOnceWriteCleanerEntities = new ArrayList<>();
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "exactlyOnceWriteCleanerEntities", exactlyOnceWriteCleanerEntities);
			dataProcessorContext = mock(DataProcessorContext.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "dataProcessorContext", dataProcessorContext);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "obsLogger", mockObsLogger);
		}

		@Test
		@DisplayName("test initExactlyOnceWriteIfNeed method for createTable is true")
		void test1() {
			try (MockedStatic<ExactlyOnceUtil> mb = Mockito
					.mockStatic(ExactlyOnceUtil.class)) {
				TapTable exactlyOnceTable = mock(TapTable.class);
				when(exactlyOnceTable.getId()).thenReturn("test");
				ConnectorNode connectorNode = mock(ConnectorNode.class);
				when(hazelcastTargetPdkBaseNode.getConnectorNode()).thenReturn(connectorNode);
				mb.when(() -> ExactlyOnceUtil.generateExactlyOnceTable(connectorNode)).thenReturn(exactlyOnceTable);
				CheckExactlyOnceWriteEnableResult checkExactlyOnceWriteEnableResult = mock(CheckExactlyOnceWriteEnableResult.class);
				when(hazelcastTargetPdkBaseNode.enableExactlyOnceWrite()).thenReturn(checkExactlyOnceWriteEnableResult);
				when(checkExactlyOnceWriteEnableResult.getEnable()).thenReturn(true);
				ConnectorFunctions functions = mock(ConnectorFunctions.class);
				when(connectorNode.getConnectorFunctions()).thenReturn(functions);
				when(hazelcastTargetPdkBaseNode.createTable(any(TapTable.class), any(AtomicBoolean.class), any(Boolean.class))).thenReturn(true);
				when(functions.getCreateIndexFunction()).thenReturn(mock(CreateIndexFunction.class));
				when(hazelcastTargetPdkBaseNode.getNode()).thenReturn((Node) mock(TableNode.class));
				doCallRealMethod().when(hazelcastTargetPdkBaseNode).initExactlyOnceWriteIfNeed();
				hazelcastTargetPdkBaseNode.initExactlyOnceWriteIfNeed();
				verify(hazelcastTargetPdkBaseNode, new Times(1)).buildErrorConsumer("test");
			}
		}
	}

	@Nested
	class HandleTapdataEventsTest {
		List<TapdataEvent> tapdataEvents;
		JetJobStatusMonitor jobStatusMonitor = mock(JetJobStatusMonitor.class);

		@BeforeEach
		void setUp() {
			tapdataEvents = new ArrayList<>();
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).handleTapdataEvents(any());
			when(jobStatusMonitor.get()).thenReturn(JobStatus.RUNNING);

			UnitTestUtils.injectField(HazelcastBaseNode.class, hazelcastTargetPdkBaseNode, "running", new AtomicBoolean(true));
			UnitTestUtils.injectField(HazelcastBaseNode.class, hazelcastTargetPdkBaseNode, "jetJobStatusMonitor", jobStatusMonitor);
			UnitTestUtils.injectField(HazelcastTargetPdkBaseNode.class, hazelcastTargetPdkBaseNode, "firstStreamEvent", new AtomicBoolean(false));
			UnitTestUtils.injectField(HazelcastTargetPdkBaseNode.class, hazelcastTargetPdkBaseNode, "exactlyOnceWriteNeedLookupTables", new ConcurrentHashMap<>());
			when(hazelcastTargetPdkBaseNode.getConnectorNode()).thenReturn(mock(ConnectorNode.class));
		}

		@Test
		void testEmptyEvents() {
			hazelcastTargetPdkBaseNode.handleTapdataEvents(tapdataEvents);
			verify(hazelcastTargetPdkBaseNode, times(0)).handleTapdataEvent(any(), any(), any(), any(), any(), any());
			verify(hazelcastTargetPdkBaseNode, times(1)).processTapEvents(any(), any(), any());
		}

		@Test
		void testRunning() {
			// running false
			String testTableName = "test-table";
			Map<String, Object> testAfterData = new HashMap<>();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(TapInsertRecordEvent.create().table(testTableName).after(testAfterData));
			tapdataEvents.add(tapdataEvent);
			hazelcastTargetPdkBaseNode.handleTapdataEvents(tapdataEvents);

			// running true
			when(hazelcastTargetPdkBaseNode.isRunning()).thenReturn(true);
			hazelcastTargetPdkBaseNode.handleTapdataEvents(tapdataEvents);
			verify(hazelcastTargetPdkBaseNode, times(1)).handleTapdataEvent(any(), any(), any(), any(), any(), any());
			verify(hazelcastTargetPdkBaseNode, times(1)).processTapEvents(any(), any(), any());
		}

		@Test
		void testShareLog() {
			String testTableName = "test-table";
			Map<String, Object> testAfterData = new HashMap<>();
			TapdataShareLogEvent shareLogEvent = new TapdataShareLogEvent();
			shareLogEvent.setSyncStage(SyncStage.CDC);
			shareLogEvent.setTapEvent(TapInsertRecordEvent.create().table(testTableName).after(testAfterData));
			tapdataEvents.add(shareLogEvent);

			hazelcastTargetPdkBaseNode.syncMetricCollector = mock(SyncMetricCollector.class);
			when(hazelcastTargetPdkBaseNode.isRunning()).thenReturn(true);
			doAnswer(invocationOnMock -> {
				invocationOnMock.getArgument(1, List.class).add(invocationOnMock.getArgument(5));
				return null;
			}).when(hazelcastTargetPdkBaseNode).handleTapdataEvent(any(), any(), any(), any(), any(), any());

			hazelcastTargetPdkBaseNode.handleTapdataEvents(tapdataEvents);
			verify(hazelcastTargetPdkBaseNode, times(1)).handleTapdataEvent(any(), any(), any(), any(), any(), any());
			verify(hazelcastTargetPdkBaseNode, times(1)).processShareLog(any());
		}

		@Test
		void testProcessError() {
			tapdataEvents.add(TapdataStartedCdcEvent.create());

			doAnswer(invocationOnMock -> {
				throw new RuntimeException("test");
			}).when(hazelcastTargetPdkBaseNode).processTapEvents(any(), any(), any());

			when(hazelcastTargetPdkBaseNode.isRunning()).thenReturn(true);
			Assertions.assertThrows(TapdataEventException.class, () -> hazelcastTargetPdkBaseNode.handleTapdataEvents(tapdataEvents));
		}

		@Test
		void testCountDownLatchEvent() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).handleTapdataEvent(any(List.class), any(List.class), any(AtomicReference.class), any(AtomicBoolean.class), any(List.class), any(TapdataEvent.class));
			TapdataCountDownLatchEvent tapdataCountDownLatchEvent = TapdataCountDownLatchEvent.create(1);
			tapdataEvents.add(tapdataCountDownLatchEvent);
			when(hazelcastTargetPdkBaseNode.isRunning()).thenReturn(true);

			hazelcastTargetPdkBaseNode.handleTapdataEvents(tapdataEvents);

			assertEquals(0, tapdataCountDownLatchEvent.getCountDownLatch().getCount());
		}

		@Test
		void testInvalidCountDownLatchEvent() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).handleTapdataEvent(any(List.class), any(List.class), any(AtomicReference.class), any(AtomicBoolean.class), any(List.class), any(TapdataEvent.class));
			TapdataCountDownLatchEvent tapdataCountDownLatchEvent = TapdataCountDownLatchEvent.create(1);
			ReflectionTestUtils.setField(tapdataCountDownLatchEvent, "countDownLatch", null);
			tapdataEvents.add(tapdataCountDownLatchEvent);
			when(hazelcastTargetPdkBaseNode.isRunning()).thenReturn(true);

			assertDoesNotThrow(() -> hazelcastTargetPdkBaseNode.handleTapdataEvents(tapdataEvents));
		}
	}

	@Nested
	class HandleTapdataEventTest {
		List<TapEvent> tapEvents = new ArrayList<>();
		AtomicBoolean hasExactlyOnceWriteCache = new AtomicBoolean(false);
		List<TapRecordEvent> exactlyOnceWriteCache = new ArrayList<>();
		AtomicReference<TapdataEvent> lastTapdataEvent = new AtomicReference<>();

		@BeforeEach
		void setUp() throws Exception {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).handleTapdataEvent(any(), any(), any(), any(), any());
		}

		@Test
		void testDDL() {
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(new TapCreateTableEvent());

			assertDoesNotThrow(() -> hazelcastTargetPdkBaseNode.handleTapdataEvent(tapEvents, hasExactlyOnceWriteCache, exactlyOnceWriteCache, lastTapdataEvent, tapdataEvent));
		}

		@Test
		void testDML() {
			String testTableName = "test-table";
			Map<String, Object> testAfterData = new HashMap<>();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(TapInsertRecordEvent.create().table(testTableName).after(testAfterData));

			assertDoesNotThrow(() -> hazelcastTargetPdkBaseNode.handleTapdataEvent(tapEvents, hasExactlyOnceWriteCache, exactlyOnceWriteCache, lastTapdataEvent, tapdataEvent));
		}

		@Test
		void testOther() {
			TapdataEvent tapdataEvent = new TapdataEvent();

			assertDoesNotThrow(() -> hazelcastTargetPdkBaseNode.handleTapdataEvent(tapEvents, hasExactlyOnceWriteCache, exactlyOnceWriteCache, lastTapdataEvent, tapdataEvent));
		}
	}

	@Nested
	class ProcessTapEventsTest {
		List<TapdataEvent> tapdataEvents;
		List<TapEvent> tapEvents;
		CheckExactlyOnceWriteEnableResult checkExactlyOnceWriteEnableResult;
		AtomicBoolean hasExactlyOnceWriteCache;

		@BeforeEach
		void setUp() {
			tapdataEvents = new ArrayList<>();
			tapEvents = new ArrayList<>();
			checkExactlyOnceWriteEnableResult = CheckExactlyOnceWriteEnableResult.createEnable();
			hasExactlyOnceWriteCache = new AtomicBoolean(false);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).processTapEvents(any(), any(), any());
			UnitTestUtils.injectField(HazelcastTargetPdkBaseNode.class, hazelcastTargetPdkBaseNode, "checkExactlyOnceWriteEnableResult", checkExactlyOnceWriteEnableResult);
		}

		@Test
		void testEmpty() {
			hazelcastTargetPdkBaseNode.processTapEvents(tapdataEvents, tapEvents, hasExactlyOnceWriteCache);
		}

		@Test
		void testExactlyOnceWriteCacheFalse() {
			String testTableName = "test-table";
			Map<String, Object> testAfterData = new HashMap<>();
			tapEvents.add(TapInsertRecordEvent.create().table(testTableName).after(testAfterData));
			hazelcastTargetPdkBaseNode.processTapEvents(tapdataEvents, tapEvents, hasExactlyOnceWriteCache);
			verify(hazelcastTargetPdkBaseNode, times(1)).processEvents(any());
		}

		@Test
		void testExactlyOnceWriteCacheTrue() {
			hasExactlyOnceWriteCache.set(true);
			String testTableName = "test-table";
			Map<String, Object> testAfterData = new HashMap<>();
			tapEvents.add(TapInsertRecordEvent.create().table(testTableName).after(testAfterData));
			hazelcastTargetPdkBaseNode.processTapEvents(tapdataEvents, tapEvents, hasExactlyOnceWriteCache);
			verify(hazelcastTargetPdkBaseNode, times(1)).transactionBegin();
			verify(hazelcastTargetPdkBaseNode, times(1)).processEvents(any());
			verify(hazelcastTargetPdkBaseNode, times(1)).processExactlyOnceWriteCache(any());
			verify(hazelcastTargetPdkBaseNode, times(1)).transactionCommit();
		}

		@Test
		void testRollback() {
			hasExactlyOnceWriteCache.set(true);
			String testTableName = "test-table";
			Map<String, Object> testAfterData = new HashMap<>();
			tapEvents.add(TapInsertRecordEvent.create().table(testTableName).after(testAfterData));

			doAnswer(invocationOnMock -> {
				throw new RuntimeException("test");
			}).when(hazelcastTargetPdkBaseNode).processEvents(any());

			assertThrows(RuntimeException.class, () -> {
				hazelcastTargetPdkBaseNode.processTapEvents(tapdataEvents, tapEvents, hasExactlyOnceWriteCache);
			});
			verify(hazelcastTargetPdkBaseNode, times(1)).transactionBegin();
			verify(hazelcastTargetPdkBaseNode, times(1)).processEvents(any());
			verify(hazelcastTargetPdkBaseNode, times(1)).transactionRollback();
		}
	}

	@Nested
	class HandleAspectWithSyncStageTest {

		AtomicBoolean firstBatchEvent;
		AtomicBoolean firstStreamEvent;

		@BeforeEach
		void setUp() {
			firstBatchEvent = new AtomicBoolean(false);
			firstStreamEvent = new AtomicBoolean(false);
			UnitTestUtils.injectField(HazelcastTargetPdkBaseNode.class, hazelcastTargetPdkBaseNode, "firstBatchEvent", firstBatchEvent);
			UnitTestUtils.injectField(HazelcastTargetPdkBaseNode.class, hazelcastTargetPdkBaseNode, "firstStreamEvent", firstStreamEvent);
			hazelcastTargetPdkBaseNode.syncMetricCollector = mock(SyncMetricCollector.class);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).handleAspectWithSyncStage(any());
		}

		@Test
		void testInitialSync() {
			hazelcastTargetPdkBaseNode.handleAspectWithSyncStage(SyncStage.INITIAL_SYNC);
			hazelcastTargetPdkBaseNode.handleAspectWithSyncStage(SyncStage.INITIAL_SYNC);
			assertTrue(firstBatchEvent.get());
			assertFalse(firstStreamEvent.get());
		}

		@Test
		void testCdc() {
			hazelcastTargetPdkBaseNode.handleAspectWithSyncStage(SyncStage.CDC);
			hazelcastTargetPdkBaseNode.handleAspectWithSyncStage(SyncStage.CDC);
			assertFalse(firstBatchEvent.get());
			assertTrue(firstStreamEvent.get());
		}
	}

	@Nested
	@DisplayName("Method processTargetEvents test")
	class processTargetEventsTest {
		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).processTargetEvents(any(List.class));
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).enqueue(any(LinkedBlockingQueue.class), any(TapdataEvent.class));
			String tableName = "test";
			when(hazelcastTargetPdkBaseNode.getTgtTableNameFromTapEvent(any(TapEvent.class))).thenReturn(tableName);
			when(hazelcastTargetPdkBaseNode.isRunning()).thenReturn(true);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "obsLogger", mockObsLogger);
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().init()
					.after(new Document("id", 1).append("name", "test").append("created", new TapDateTimeValue(new DateTime(Instant.now()))));
			TapUpdateRecordEvent tapUpdateRecordEvent1 = TapUpdateRecordEvent.create().init()
					.before(new Document("id", 1).append("name", "test").append("created", new TapDateTimeValue(new DateTime(Instant.now()))))
					.after(new Document("id", 1).append("name", "test1").append("created", new TapDateTimeValue(new DateTime(Instant.now()))));
			TapUpdateRecordEvent tapUpdateRecordEvent2 = TapUpdateRecordEvent.create().init()
					.before(new Document("id", 1).append("name", "test1").append("created", new TapDateTimeValue(new DateTime(Instant.now()))))
					.after(new Document("id", 1).append("name", "test2").append("created", new TapDateTimeValue(new DateTime(Instant.now()))));
			TapDeleteRecordEvent tapDeleteRecordEvent = TapDeleteRecordEvent.create().init().before(new Document("id", 1));

			TapdataEvent tapdataEvent1 = new TapdataEvent();
			tapdataEvent1.setTapEvent(tapInsertRecordEvent);
			TapdataEvent tapdataEvent2 = new TapdataEvent();
			tapdataEvent2.setTapEvent(tapUpdateRecordEvent1);
			TapdataEvent tapdataEvent3 = new TapdataEvent();
			tapdataEvent3.setTapEvent(tapUpdateRecordEvent2);
			TransformToTapValueResult transformToTapValueResult = TransformToTapValueResult.create()
					.beforeTransformedToTapValueFieldNames(new HashSet<String>() {{
						add("created");
					}})
					.afterTransformedToTapValueFieldNames(new HashSet<String>() {{
						add("created");
					}});
			tapdataEvent3.setTransformToTapValueResult(transformToTapValueResult);
			TapdataEvent tapdataEvent4 = new TapdataEvent();
			tapdataEvent4.setTapEvent(tapDeleteRecordEvent);

			List<TapdataEvent> tapdataEvents = new ArrayList<>();
			tapdataEvents.add(tapdataEvent1);
			tapdataEvents.add(tapdataEvent2);
			tapdataEvents.add(tapdataEvent3);
			tapdataEvents.add(tapdataEvent4);
			BlockingQueue<TapdataEvent> tapEventProcessQueue = new LinkedBlockingQueue<>();
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "tapEventProcessQueue", tapEventProcessQueue);
			TapCodecsFilterManager codecsFilterManager = mock(TapCodecsFilterManager.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "codecsFilterManager", codecsFilterManager);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "codecsFilterManagerForBatchRead", codecsFilterManager);

			hazelcastTargetPdkBaseNode.processTargetEvents(tapdataEvents);
			assertEquals(tapdataEvents.size(), tapEventProcessQueue.size());
			verify(hazelcastTargetPdkBaseNode, times(tapdataEvents.size())).replaceIllegalDateWithNullIfNeed(any(TapRecordEvent.class));
			verify(hazelcastTargetPdkBaseNode, times(tapdataEvents.size())).fromTapValueMergeInfo(any(TapdataEvent.class));
			verify(hazelcastTargetPdkBaseNode, times(2)).fromTapValue(any(Map.class), any(TapCodecsFilterManager.class), anyString(), any(Set.class));
			verify(hazelcastTargetPdkBaseNode, times(4)).fromTapValue(any(Map.class), any(TapCodecsFilterManager.class), anyString());
		}

		@Test
		@DisplayName("test when is not running")
		void test2() {
			when(hazelcastTargetPdkBaseNode.isRunning()).thenReturn(false);
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().init().after(new Document("id", 1));
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			List<TapdataEvent> tapdataEvents = new ArrayList<>();
			tapdataEvents.add(tapdataEvent);
			BlockingQueue<TapdataEvent> tapEventProcessQueue = new LinkedBlockingQueue<>();
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "tapEventProcessQueue", tapEventProcessQueue);

			hazelcastTargetPdkBaseNode.processTargetEvents(tapdataEvents);
			assertEquals(0, tapEventProcessQueue.size());
		}
		@Test
		@DisplayName("test processTargetEvents method for ddl event")
		void test3() {
			TapCreateTableEvent tapCreateTableEvent = new TapCreateTableEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapCreateTableEvent);
			List<TapdataEvent> tapdataEvents = new ArrayList<>();
			tapdataEvents.add(tapdataEvent);
			BlockingQueue<TapdataEvent> tapEventProcessQueue = new LinkedBlockingQueue<>();
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "tapEventProcessQueue", tapEventProcessQueue);
			new Thread(() -> assertDoesNotThrow(() -> {
				TapdataEvent pollEvent = tapEventProcessQueue.take();
				assertInstanceOf(TapdataCountDownLatchEvent.class, pollEvent);
				assertNotNull(((TapdataCountDownLatchEvent) pollEvent).getCountDownLatch());
				assertEquals(1, ((TapdataCountDownLatchEvent) pollEvent).getCountDownLatch().getCount());
				((TapdataCountDownLatchEvent) pollEvent).getCountDownLatch().countDown();
				pollEvent = tapEventProcessQueue.take();
				assertEquals(tapdataEvent, pollEvent);
			})).start();
			hazelcastTargetPdkBaseNode.processTargetEvents(tapdataEvents);
			verify(hazelcastTargetPdkBaseNode, never()).fromTapValueMergeInfo(any(TapdataEvent.class));
			verify(hazelcastTargetPdkBaseNode).updateMemoryFromDDLInfoMap(tapdataEvent);
		}

		@Test
		@DisplayName("test when ddl event, thread interrupt when await count down latch")
		void test4() {
			TapCreateTableEvent tapCreateTableEvent = new TapCreateTableEvent();
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapCreateTableEvent);
			List<TapdataEvent> tapdataEvents = new ArrayList<>();
			tapdataEvents.add(tapdataEvent);
			BlockingQueue<TapdataEvent> tapEventProcessQueue = new LinkedBlockingQueue<>();
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "tapEventProcessQueue", tapEventProcessQueue);
			Thread thread = new Thread(() -> hazelcastTargetPdkBaseNode.processTargetEvents(tapdataEvents));
			thread.start();
			assertDoesNotThrow(() -> TimeUnit.MILLISECONDS.sleep(300L));
			thread.interrupt();
			verify(hazelcastTargetPdkBaseNode, never()).updateMemoryFromDDLInfoMap(tapdataEvent);
		}
	}

	@Nested
	@DisplayName("Method queueConsume test")
	class queueConsumeTest {

		private MockedStatic<AspectUtils> aspectUtilsMockedStatic;
		private LinkedBlockingQueue<TapdataEvent> tapEventQueue;

		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).queueConsume();
			aspectUtilsMockedStatic = mockStatic(AspectUtils.class);
			aspectUtilsMockedStatic.when(() -> AspectUtils.executeAspect(eq(DataNodeThreadGroupAspect.class), any())).thenAnswer(invocationOnMock -> null);
			tapEventQueue = new LinkedBlockingQueue<>();
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "tapEventQueue", tapEventQueue);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "targetBatch", 100);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "targetBatchIntervalMs", 500L);
			doAnswer(invocationOnMock -> null).when(hazelcastTargetPdkBaseNode).executeAspect(eq(WriteErrorAspect.class), any(Callable.class));
		}

		@AfterEach
		void tearDown() {
			aspectUtilsMockedStatic.close();
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			hazelcastTargetPdkBaseNode.queueConsume();

			verify(hazelcastTargetPdkBaseNode).drainAndRun(eq(tapEventQueue), eq(100), eq(500L), eq(TimeUnit.MILLISECONDS), any(TapdataEventsRunner.class));
		}

		@Test
		@DisplayName("test throw TapCodeException")
		void test2() {
			TapCodeException tapCodeException = new TapCodeException("test", "test error");
			doThrow(tapCodeException).when(hazelcastTargetPdkBaseNode).drainAndRun(eq(tapEventQueue), eq(100), eq(500L), eq(TimeUnit.MILLISECONDS), any(TapdataEventsRunner.class));
			doAnswer(invocationOnMock -> {
				Object argument = invocationOnMock.getArgument(0);
				assertSame(argument, tapCodeException);
				return argument;
			}).when(hazelcastTargetPdkBaseNode).errorHandle(any(Throwable.class));

			hazelcastTargetPdkBaseNode.queueConsume();
		}

		@Test
		@DisplayName("test throw not TapCodeException")
		void test3() {
			RuntimeException runtimeException = new RuntimeException("test");
			doThrow(runtimeException).when(hazelcastTargetPdkBaseNode).drainAndRun(eq(tapEventQueue), eq(100), eq(500L), eq(TimeUnit.MILLISECONDS), any(TapdataEventsRunner.class));
			doAnswer(invocationOnMock -> {
				Object argument = invocationOnMock.getArgument(0);
				assertInstanceOf(TapCodeException.class, argument);
				assertEquals(TaskTargetProcessorExCode_15.UNKNOWN_ERROR, ((TapCodeException) argument).getCode());
				assertSame(runtimeException, ((TapCodeException) argument).getCause());
				return argument;
			}).when(hazelcastTargetPdkBaseNode).errorHandle(any(Throwable.class));

			hazelcastTargetPdkBaseNode.queueConsume();
		}
	}

	@Nested
	@DisplayName("Method drainAndRun test")
	class drainAndRunTest {
		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).drainAndRun(any(BlockingQueue.class), anyInt(), anyLong(), any(TimeUnit.class), any(TapdataEventsRunner.class));
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			when(hazelcastTargetPdkBaseNode.isRunning()).thenReturn(true);
			BlockingQueue<TapdataEvent> queue = new LinkedBlockingQueue<>();
			for (int i = 0; i < 10; i++) {
				assertDoesNotThrow(() -> queue.put(new TapdataEvent()));
			}
			CountDownLatch countDownLatch = new CountDownLatch(queue.size());

			new Thread(() -> hazelcastTargetPdkBaseNode.drainAndRun(queue, 5, 5L, TimeUnit.SECONDS, tapdataEventList -> {
				assertEquals(5, tapdataEventList.size());
				for (TapdataEvent ignored : tapdataEventList) {
					countDownLatch.countDown();
				}
			})).start();

			assertDoesNotThrow(() -> countDownLatch.await(2L, TimeUnit.SECONDS));
			assertEquals(0, countDownLatch.getCount());
		}
	}

	@Nested
	@DisplayName("Method initInitialConcurrentProcessor test")
	class initInitialConcurrentProcessorTest {
		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).initInitialConcurrentProcessor(anyInt(), any(Partitioner.class));
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			taskDto = new TaskDto();
			taskDto.setId(new ObjectId());
			taskDto.setName("task 1");
			dataProcessorContext = mock(DataProcessorContext.class);
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "dataProcessorContext", dataProcessorContext);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "targetBatch", 100);

			PartitionConcurrentProcessor partitionConcurrentProcessor = hazelcastTargetPdkBaseNode.initInitialConcurrentProcessor(4, mock(Partitioner.class));

			assertNotNull(partitionConcurrentProcessor);
		}
	}

	@Nested
	@DisplayName("Method handleTapdataAdjustMemoryEventTest test")
	class handleTapdataAdjustMemoryEventTest {
		ThreadPoolExecutorEx queueExecutorEx;
		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).handleTapdataAdjustMemoryEvent(any());
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).initQueueConsumerThreadPool();
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).initTargetQueueConsumer();
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode,"originalWriteQueueCapacity", 100);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode,"writeQueueCapacity", 200);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode,"tapEventQueue", new LinkedBlockingQueue<>(100));
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode,"tapEventProcessQueue", new LinkedBlockingQueue<>(100));
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode,"tapEventProcessQueue", new LinkedBlockingQueue<>(100));
			queueExecutorEx = mock(ThreadPoolExecutorEx.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode,"queueConsumerThreadPool",queueExecutorEx );
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode,"obsLogger",mock(ObsLogger.class));
			TaskResourceSupervisorManager taskResourceSupervisorManager = mock(TaskResourceSupervisorManager.class);
			when(taskResourceSupervisorManager.getTaskNodeInfos()).thenReturn(new ConcurrentHashSet<>());
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode,"taskResourceSupervisorManager",taskResourceSupervisorManager);
			DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
			TaskDto taskDto1 = new TaskDto();
			taskDto1.setName("name");
			when(dataProcessorContext.getTaskDto()).thenReturn(taskDto1);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode,"dataProcessorContext",dataProcessorContext);
			when(hazelcastTargetPdkBaseNode.isRunning()).thenReturn(true);
		}
		@Test
		void test_main(){
			TapdataAdjustMemoryEvent tapdataEvent = new TapdataAdjustMemoryEvent(TapdataAdjustMemoryEvent.KEEP, 1.0);
			try(MockedStatic<AsyncUtils> mockedStatic = mockStatic(AsyncUtils.class)){
				TableNode node  = new TableNode();
				node.setName("test");
				node.setId("test");
				doReturn(node).when(hazelcastTargetPdkBaseNode).getNode();
				mockedStatic.when(() -> AsyncUtils.createThreadPoolExecutor(anyString(), anyInt(), any(ThreadGroup.class), anyString())).thenReturn(queueExecutorEx);
				doReturn(mock(ThreadGroup.class)).when(hazelcastTargetPdkBaseNode).getReuseOrNewThreadGroup(any());
				when(queueExecutorEx.isShutdown()).thenReturn(true);
				hazelcastTargetPdkBaseNode.handleTapdataAdjustMemoryEvent(tapdataEvent);
				verify(queueExecutorEx,times(1)).shutdownNow();
				verify(queueExecutorEx,times(2)).submit(any(Runnable.class));
			}
		}

		@Test
		void test_isShutdownFalse(){
			TapdataAdjustMemoryEvent tapdataEvent = new TapdataAdjustMemoryEvent(TapdataAdjustMemoryEvent.KEEP, 1.0);
			when(queueExecutorEx.isShutdown()).thenReturn(false);
			hazelcastTargetPdkBaseNode.handleTapdataAdjustMemoryEvent(tapdataEvent);
			verify(queueExecutorEx,times(1)).shutdownNow();
		}
	}
	@Nested
	class InitSyncProgressMapTest{
		@SneakyThrows
		@Test
		void test1(){
			Map<String, SyncProgress> allSyncProgressMap = new ConcurrentHashMap<>();
			HazelcastTargetPdkBaseNode hazelcastTargetPdkBaseNode = mock(HazelcastTargetPdkBaseNode.class);
			Map<String, Object> attrs = new HashMap<>();
			TaskDto taskDto=new TaskDto();
			Map<String, String> syncProgressMap = genSyncProgress();
			attrs.put("syncProgress",syncProgressMap);
			taskDto.setAttrs(attrs);
			DataProcessorContext dataProcessorContext = DataProcessorContext.newBuilder().withTaskDto(taskDto).build();
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).getDataProcessorContext();
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "dataProcessorContext", dataProcessorContext);
			ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode,"syncProgressMap",allSyncProgressMap);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).initSyncProgressMap();
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).foundAllSyncProgress(attrs);
			hazelcastTargetPdkBaseNode.initSyncProgressMap();
			assertEquals(allSyncProgressMap.size()>0,true);
		}
		@SneakyThrows
		public Map<String, String> genSyncProgress() {
			List<String> keyList = new ArrayList<>();
			keyList.add("sourceId");
			keyList.add("targetId");
			String jsonString = JSON.toJSONString(keyList);
			SyncProgress syncProgress = new SyncProgress();
			Map<String, String> syncProgressMap = new HashMap<>();
			Map<String, Object> batchOffset = new HashMap<>();
			batchOffset.put(BatchOffsetUtil.BATCH_READ_CONNECTOR_STATUS, TableBatchReadStatus.OVER);
			syncProgress.setBatchOffsetObj(batchOffset);
			syncProgress.setBatchOffset(PdkUtil.encodeOffset(batchOffset));
			String syncProgressString = JSONUtil.obj2Json(syncProgress);
			syncProgressMap.put(jsonString, syncProgressString);
			return syncProgressMap;
		}

	}
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import com.google.common.collect.Lists;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Processor;
import com.tapdata.entity.Connections;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.TapdataStartedCdcEvent;
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
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.monitor.impl.JetJobStatusMonitor;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.metric.collector.ISyncMetricCollector;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableV2Function;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.async.AsyncUtils;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import io.tapdata.utils.UnitTestUtils;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).createTable(tapTable, atomicBoolean);
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
			boolean result = hazelcastTargetPdkBaseNode.createTable(tapTable, atomicBoolean);
			Assertions.assertTrue(result);
		}

		@Test
		void testIsNotUnwindProcess() {
			hazelcastTargetPdkBaseNode.unwindProcess = false;
			TapTable tapTable = mock(TapTable.class);
			AtomicBoolean atomicBoolean = new AtomicBoolean(false);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).createTable(tapTable, atomicBoolean);
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
			boolean result = hazelcastTargetPdkBaseNode.createTable(tapTable, atomicBoolean);
			Assertions.assertTrue(result);
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
			hazelcastTargetPdkBaseNode.handleTapdataRecordEvent(tapdataEvent);
			verify(hazelcastTargetPdkBaseNode).replaceIllegalDateWithNullIfNeed(any(TapRecordEvent.class));
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
			hazelcastTargetPdkBaseNode.handleTapdataRecordEvent(tapdataEvent);
			verify(hazelcastTargetPdkBaseNode).replaceIllegalDateWithNullIfNeed(any(TapRecordEvent.class));
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
			after.put("id", "1");
			after.put("name", "test");
			after.put("date", new Date());
			after.put("last_date", new Date());
			((TapInsertRecordEvent) event).setAfter(after);
			event.setContainsIllegalDate(true);
			List<String> illegalDateFiledName = new ArrayList<>();
			illegalDateFiledName.add("date");
			illegalDateFiledName.add("last_date");
			illegalDateFiledName.add("test_date");
			((TapInsertRecordEvent) event).setAfterIllegalDateFieldName(illegalDateFiledName);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).replaceIllegalDateWithNullIfNeed(event);
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
			after.put("id", "1");
			after.put("name", "test");
			after.put("date", new Date());
			((TapUpdateRecordEvent) event).setBefore(after);
			((TapUpdateRecordEvent) event).setAfter(after);
			event.setContainsIllegalDate(true);
			List<String> illegalDateFiledName = new ArrayList<>();
			illegalDateFiledName.add("date");
			illegalDateFiledName.add("last_date");
			((TapUpdateRecordEvent) event).setBeforeIllegalDateFieldName(illegalDateFiledName);
			((TapUpdateRecordEvent) event).setAfterIllegalDateFieldName(illegalDateFiledName);
			doCallRealMethod().when(hazelcastTargetPdkBaseNode).replaceIllegalDateWithNullIfNeed(event);
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
				verify(flushOffsetExecutor).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
				assertTrue(Thread.currentThread().getName().startsWith("Target-Process"));
				verify(hazelcastTargetPdkBaseNode).checkUnwindConfiguration();
			}
		}
	}

    @Nested
    class HandleTapdataEventsTest {
        @Test
        void testHandleTapdataEvents() {
            List<TapdataEvent> tapdataEvents = new ArrayList<>();
            doCallRealMethod().when(hazelcastTargetPdkBaseNode).handleTapdataEvents(any());
            JetJobStatusMonitor jobStatusMonitor = mock(JetJobStatusMonitor.class);
            when(jobStatusMonitor.get()).thenReturn(JobStatus.RUNNING);

            UnitTestUtils.injectField(HazelcastBaseNode.class, hazelcastTargetPdkBaseNode, "running", new AtomicBoolean(true));
            UnitTestUtils.injectField(HazelcastBaseNode.class, hazelcastTargetPdkBaseNode, "jetJobStatusMonitor", jobStatusMonitor);
            UnitTestUtils.injectField(HazelcastTargetPdkBaseNode.class, hazelcastTargetPdkBaseNode, "firstStreamEvent", new AtomicBoolean(false));
            UnitTestUtils.injectField(HazelcastTargetPdkBaseNode.class, hazelcastTargetPdkBaseNode, "exactlyOnceWriteNeedLookupTables", new ConcurrentHashMap<>());
            when(hazelcastTargetPdkBaseNode.getConnectorNode()).thenReturn(mock(ConnectorNode.class));

//            hazelcastTargetPdkBaseNode.handleTapdataEvents(tapdataEvents);

            tapdataEvents.add(TapdataStartedCdcEvent.create());
            hazelcastTargetPdkBaseNode.handleTapdataEvents(tapdataEvents);

        }
    }
}

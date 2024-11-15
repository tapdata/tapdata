package io.tapdata.flow.engine.V2.task.preview.node;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.Processor;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataPreviewCompleteEvent;
import com.tapdata.entity.task.config.TaskGlobalVariable;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.pretty.ClassHandlersV2;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.task.preview.*;
import io.tapdata.flow.engine.V2.task.preview.entity.MergeReadData;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewFinishReadOperation;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewMergeReadOperation;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewOperation;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewReadOperation;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.schema.TapTableMap;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-11-05 15:56
 **/
@DisplayName("Class HazelcastPreviewSourcePdkDataNode Test")
class HazelcastPreviewSourcePdkDataNodeTest {

	private DataProcessorContext dataProcessorContext;
	private Node tableNode;
	private HazelcastPreviewSourcePdkDataNode hazelcastPreviewSourcePdkDataNode;
	private TaskDto taskDto;

	@BeforeEach
	void setUp() {
		dataProcessorContext = mock(DataProcessorContext.class);
		tableNode = new TableNode();
		tableNode.setId("1");
		((TableNode) tableNode).setTableName("test");
		when(dataProcessorContext.getNode()).thenReturn(tableNode);
		taskDto = new TaskDto();
		taskDto.setId(new ObjectId());
		taskDto.setTestTaskId(new ObjectId().toHexString());
		taskDto.setName("task 1");
		taskDto.setType(TaskDto.TYPE_INITIAL_SYNC_CDC);
		when(dataProcessorContext.getTaskDto()).thenReturn(taskDto);
		hazelcastPreviewSourcePdkDataNode = new HazelcastPreviewSourcePdkDataNode(dataProcessorContext);
		hazelcastPreviewSourcePdkDataNode = spy(hazelcastPreviewSourcePdkDataNode);
		doAnswer(invocationOnMock -> {
			throw (Throwable) invocationOnMock.getArgument(0);
		}).when(hazelcastPreviewSourcePdkDataNode).errorHandle(any(Throwable.class));
		doAnswer(invocationOnMock -> {
			throw (Throwable) invocationOnMock.getArgument(0);
		}).when(hazelcastPreviewSourcePdkDataNode).errorHandle(any(Throwable.class), anyString());
	}

	@Nested
	@DisplayName("Method doInit test")
	class doInitTest {

		private Processor.Context context;
		private HazelcastInstance hazelcastInstance;

		@BeforeEach
		void setUp() {
			context = mock(Processor.Context.class);
			hazelcastInstance = mock(HazelcastInstance.class);
			when(context.hazelcastInstance()).thenReturn(hazelcastInstance);
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			doAnswer(invocationOnMock -> null).when(hazelcastPreviewSourcePdkDataNode).createPdkConnectorNode(dataProcessorContext, hazelcastInstance);
			TaskPreviewResultVO taskPreviewResultVO = new TaskPreviewResultVO(taskDto);
			taskPreviewResultVO.setStats(new TaskPReviewStatsVO());
			TaskPreviewInstance taskPreviewInstance = new TaskPreviewInstance();
			taskPreviewInstance.setTaskPreviewResultVO(taskPreviewResultVO);
			ReflectionTestUtils.setField(hazelcastPreviewSourcePdkDataNode, "taskPreviewInstance", taskPreviewInstance);
			hazelcastPreviewSourcePdkDataNode.doInit(context);

			verify(hazelcastPreviewSourcePdkDataNode).initTapLogger();
			verify(hazelcastPreviewSourcePdkDataNode).createPdkConnectorNode(dataProcessorContext, hazelcastInstance);
			verify(hazelcastPreviewSourcePdkDataNode).initTapCodecsFilterManager();
		}
	}

	@Nested
	@DisplayName("Method initTapCodecsFilterManager test")
	class initTapCodecsFilterManagerTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			hazelcastPreviewSourcePdkDataNode.initTapCodecsFilterManager();

			Instant instant = Instant.now();
			DateTime dateTime = new DateTime(instant);
			Object defaultCodecsRegistry = ReflectionTestUtils.getField(hazelcastPreviewSourcePdkDataNode, "defaultCodecsRegistry");
			assertNotNull(defaultCodecsRegistry);
			Object defaultCodecsFilterManager = ReflectionTestUtils.getField(hazelcastPreviewSourcePdkDataNode, "defaultCodecsFilterManager");
			assertNotNull(defaultCodecsFilterManager);
			assertEquals(instant.toString(), ((TapCodecsFilterManager) defaultCodecsFilterManager)
					.getCodecsRegistry().getCustomFromTapValueCodec(TapDateTimeValue.class).fromTapValue(new TapDateTimeValue(dateTime)));
			assertEquals(instant.toString(), ((TapCodecsFilterManager) defaultCodecsFilterManager)
					.getCodecsRegistry().getCustomFromTapValueCodec(TapDateValue.class).fromTapValue(new TapDateValue(dateTime)));
			assertEquals(dateTime.toTimeStr(), ((TapCodecsFilterManager) defaultCodecsFilterManager)
					.getCodecsRegistry().getCustomFromTapValueCodec(TapTimeValue.class).fromTapValue(new TapTimeValue(dateTime)));
			assertEquals(dateTime.toLocalDateTime().getYear(), ((TapCodecsFilterManager) defaultCodecsFilterManager)
					.getCodecsRegistry().getCustomFromTapValueCodec(TapYearValue.class).fromTapValue(new TapYearValue(dateTime)));
			assertInstanceOf(Long.class, ((TapCodecsFilterManager) defaultCodecsFilterManager)
					.getCodecsRegistry().getCustomFromTapValueCodec(TapNumberValue.class).fromTapValue(new TapNumberValue(1.0D)));
			assertInstanceOf(Double.class, ((TapCodecsFilterManager) defaultCodecsFilterManager)
					.getCodecsRegistry().getCustomFromTapValueCodec(TapNumberValue.class).fromTapValue(new TapNumberValue(1.1D)));
		}
	}

	@Nested
	@DisplayName("Method complete test")
	class completeTest {

		private ClassHandlersV2 previewOperationHandlers;

		@BeforeEach
		void setUp() {
			previewOperationHandlers = mock(ClassHandlersV2.class);
			ReflectionTestUtils.setField(hazelcastPreviewSourcePdkDataNode, "previewOperationHandlers", previewOperationHandlers);
			doReturn(true).when(hazelcastPreviewSourcePdkDataNode).isRunning();
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			PreviewReadOperationQueue previewReadOperationQueue = new PreviewReadOperationQueue(100);
			TaskPreviewInstance taskPreviewInstance = mock(TaskPreviewInstance.class);
			when(taskPreviewInstance.getPreviewReadOperationQueue()).thenReturn(previewReadOperationQueue);
			ReflectionTestUtils.setField(hazelcastPreviewSourcePdkDataNode, "taskPreviewInstance", taskPreviewInstance);
			PreviewReadOperation previewReadOperation = new PreviewReadOperation(tableNode.getId());
			previewReadOperationQueue.addOperation(tableNode.getId(), previewReadOperation);
			Map<String, Object> data = new HashMap<>();
			data.put("id", 1);
			data.put("xxx", "test1");
			data.put("yyy", Instant.now());
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().after(data);
			when(previewOperationHandlers.handle(any())).thenReturn(Arrays.asList(tapInsertRecordEvent));
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertInstanceOf(TapdataEvent.class, argument1);
				TapdataEvent tapdataEvent = (TapdataEvent) argument1;
				TapEvent tapEvent = tapdataEvent.getTapEvent();
				assertInstanceOf(TapInsertRecordEvent.class, tapEvent);
				assertEquals(data, ((TapInsertRecordEvent) tapEvent).getAfter());
				return true;
			}).when(hazelcastPreviewSourcePdkDataNode).offer(any(TapdataEvent.class));

			Boolean complete = assertDoesNotThrow(() -> hazelcastPreviewSourcePdkDataNode.complete());

			verify(previewOperationHandlers).handle(any());
			assertFalse(complete);
		}

		@Test
		@DisplayName("test pending tapdata events is not empty")
		void test2() {
			Map<String, Object> data = new HashMap<>();
			data.put("id", 1);
			data.put("xxx", "test1");
			data.put("yyy", Instant.now());
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().after(data);
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
			Object offerPendingTapdataEvents = ReflectionTestUtils.getField(hazelcastPreviewSourcePdkDataNode, "offerPendingTapdataEvents");
			((List<TapdataEvent>) offerPendingTapdataEvents).add(tapdataEvent);
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertInstanceOf(TapdataEvent.class, argument1);
				TapdataEvent event = (TapdataEvent) argument1;
				TapEvent tapEvent = event.getTapEvent();
				assertInstanceOf(TapInsertRecordEvent.class, tapEvent);
				assertEquals(data, ((TapInsertRecordEvent) tapEvent).getAfter());
				return true;
			}).when(hazelcastPreviewSourcePdkDataNode).offer(any(TapdataEvent.class));

			Boolean complete = assertDoesNotThrow(() -> hazelcastPreviewSourcePdkDataNode.complete());

			verify(previewOperationHandlers, never()).handle(any());
			assertFalse(complete);
		}

		@Test
		@DisplayName("test not running")
		void test3() {
			doReturn(false).when(hazelcastPreviewSourcePdkDataNode).isRunning();
			Boolean complete = assertDoesNotThrow(() -> hazelcastPreviewSourcePdkDataNode.complete());
			assertTrue(complete);
		}

		@Test
		@DisplayName("test global PREVIEW_COMPLETE is true")
		void test4() {
			Map<String, Object> taskGlobalVariable = TaskGlobalVariable.INSTANCE.getTaskGlobalVariable(TaskPreviewService.taskPreviewInstanceId(taskDto));
			taskGlobalVariable.put(TaskGlobalVariable.PREVIEW_COMPLETE_KEY, true);
			PreviewReadOperationQueue previewReadOperationQueue = new PreviewReadOperationQueue(100);
			TaskPreviewInstance taskPreviewInstance = mock(TaskPreviewInstance.class);
			when(taskPreviewInstance.getPreviewReadOperationQueue()).thenReturn(previewReadOperationQueue);
			ReflectionTestUtils.setField(hazelcastPreviewSourcePdkDataNode, "taskPreviewInstance", taskPreviewInstance);
			AtomicBoolean running = new AtomicBoolean(true);
			ReflectionTestUtils.setField(hazelcastPreviewSourcePdkDataNode, "running", running);
			Boolean complete = assertDoesNotThrow(() -> hazelcastPreviewSourcePdkDataNode.complete());
			assertFalse(complete);
			assertFalse(running.get());
		}

		@Test
		@DisplayName("test cannot offer")
		void test5() {
			PreviewReadOperationQueue previewReadOperationQueue = new PreviewReadOperationQueue(100);
			TaskPreviewInstance taskPreviewInstance = mock(TaskPreviewInstance.class);
			when(taskPreviewInstance.getPreviewReadOperationQueue()).thenReturn(previewReadOperationQueue);
			ReflectionTestUtils.setField(hazelcastPreviewSourcePdkDataNode, "taskPreviewInstance", taskPreviewInstance);
			PreviewReadOperation previewReadOperation = new PreviewReadOperation(tableNode.getId());
			previewReadOperationQueue.addOperation(tableNode.getId(), previewReadOperation);
			Map<String, Object> data = new HashMap<>();
			data.put("id", 1);
			data.put("xxx", "test1");
			data.put("yyy", Instant.now());
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().after(data);
			when(previewOperationHandlers.handle(any())).thenReturn(Arrays.asList(tapInsertRecordEvent));
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertInstanceOf(TapdataEvent.class, argument1);
				TapdataEvent tapdataEvent = (TapdataEvent) argument1;
				TapEvent tapEvent = tapdataEvent.getTapEvent();
				assertInstanceOf(TapInsertRecordEvent.class, tapEvent);
				assertEquals(data, ((TapInsertRecordEvent) tapEvent).getAfter());
				return false;
			}).when(hazelcastPreviewSourcePdkDataNode).offer(any(TapdataEvent.class));

			Boolean complete = assertDoesNotThrow(() -> hazelcastPreviewSourcePdkDataNode.complete());

			verify(previewOperationHandlers).handle(any());
			assertFalse(complete);
			Object offerPendingTapdataEvents = ReflectionTestUtils.getField(hazelcastPreviewSourcePdkDataNode, "offerPendingTapdataEvents");
			assertEquals(1, ((List<TapdataEvent>) offerPendingTapdataEvents).size());
		}
	}

	@Nested
	@DisplayName("Method read test")
	class readTest {
		@Test
		@DisplayName("test queryByAdvanceFilterFunction")
		void test1() {
			TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create().limit(1).match(new DataMap().kv("id", 1));
			PreviewReadOperation previewReadOperation = new PreviewReadOperation(tableNode.getId());
			previewReadOperation.setTapAdvanceFilter(tapAdvanceFilter);
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapConnectorContext connectorContext = mock(TapConnectorContext.class);
			when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			doReturn(connectorNode).when(hazelcastPreviewSourcePdkDataNode).getConnectorNode();
			QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = mock(QueryByAdvanceFilterFunction.class);
			when(connectorFunctions.getQueryByAdvanceFilterFunction()).thenReturn(queryByAdvanceFilterFunction);
			TapTable tapTable = new TapTable(((TableNode) tableNode).getTableName());
			TapTableMap tapTableMap = mock(TapTableMap.class);
			when(tapTableMap.get(((TableNode) tableNode).getTableName())).thenReturn(tapTable);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			Map<String, Object> data = new HashMap<>();
			data.put("id", 1);
			data.put("xxx", "test");
			data.put("yyy", Instant.now());
			assertDoesNotThrow(() -> doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertEquals(connectorContext, argument1);
				Object argument4 = invocationOnMock.getArgument(3);
				assertInstanceOf(Consumer.class, argument4);
				Consumer<FilterResults> consumer = (Consumer<FilterResults>) argument4;
				FilterResults filterResults = new FilterResults();
				filterResults.setResults(Arrays.asList(data));
				consumer.accept(filterResults);
				return null;
			}).when(queryByAdvanceFilterFunction).query(any(), eq(tapAdvanceFilter), eq(tapTable), any()));
			TaskPreviewResultVO taskPreviewResultVO = new TaskPreviewResultVO(taskDto);
			taskPreviewResultVO.setStats(new TaskPReviewStatsVO());
			TaskPreviewInstance taskPreviewInstance = new TaskPreviewInstance();
			taskPreviewInstance.setTaskPreviewResultVO(taskPreviewResultVO);
			ReflectionTestUtils.setField(hazelcastPreviewSourcePdkDataNode, "taskPreviewInstance", taskPreviewInstance);

			List<TapInsertRecordEvent> read = hazelcastPreviewSourcePdkDataNode.read(previewReadOperation);
			assertNotNull(read);
			assertEquals(1, read.size());
			assertEquals(data, read.get(0).getAfter());
			Map<String, TaskPreviewReadStatsVO> readStats = taskPreviewResultVO.getStats().getReadStats();
			assertEquals(1, read.size());
			TaskPreviewReadStatsVO readStatsVO = readStats.get(tableNode.getId());
			assertEquals(PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER.name(), readStatsVO.getMethod());
			assertEquals(((TableNode) tableNode).getTableName(), readStatsVO.getTableName());
			assertEquals(tapAdvanceFilter.getLimit(), readStatsVO.getLimit());
			assertEquals(tapAdvanceFilter.getMatch(), readStatsVO.getMatch());
			assertEquals(read.size(), readStatsVO.getRows());
		}

		@Test
		@DisplayName("test batchReadFunction")
		void test2() {
			TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create().limit(1).match(new DataMap().kv("id", 1));
			PreviewReadOperation previewReadOperation = new PreviewReadOperation(tableNode.getId());
			previewReadOperation.setTapAdvanceFilter(tapAdvanceFilter);
			ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
			ConnectorNode connectorNode = mock(ConnectorNode.class);
			TapConnectorContext connectorContext = mock(TapConnectorContext.class);
			when(connectorNode.getConnectorContext()).thenReturn(connectorContext);
			when(connectorNode.getConnectorFunctions()).thenReturn(connectorFunctions);
			doReturn(connectorNode).when(hazelcastPreviewSourcePdkDataNode).getConnectorNode();
			BatchReadFunction batchReadFunction = mock(BatchReadFunction.class);
			when(connectorFunctions.getBatchReadFunction()).thenReturn(batchReadFunction);
			TapTable tapTable = new TapTable(((TableNode) tableNode).getTableName());
			TapTableMap tapTableMap = mock(TapTableMap.class);
			when(tapTableMap.get(((TableNode) tableNode).getTableName())).thenReturn(tapTable);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			Map<String, Object> data1 = new HashMap<>();
			data1.put("id", 1);
			data1.put("xxx", "test");
			data1.put("yyy", Instant.now());
			TapInsertRecordEvent tapInsertRecordEvent1 = TapInsertRecordEvent.create().after(data1);
			Map<String, Object> data2 = new HashMap<>();
			data2.put("id", 2);
			data2.put("xxx", "test");
			data2.put("yyy", Instant.now());
			TapInsertRecordEvent tapInsertRecordEvent2 = TapInsertRecordEvent.create().after(data2);
			taskDto.setPreviewRows(1);
			assertDoesNotThrow(() -> doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertEquals(connectorContext, argument1);
				assertNull(invocationOnMock.getArgument(2));
				assertEquals(1, (Integer) invocationOnMock.getArgument(3));
				Object argument5 = invocationOnMock.getArgument(4);
				assertInstanceOf(BiConsumer.class, argument5);
				BiConsumer<List<TapEvent>, Object> biConsumer = (BiConsumer<List<TapEvent>, Object>) argument5;
				biConsumer.accept(Arrays.asList(tapInsertRecordEvent1), null);
				biConsumer.accept(Arrays.asList(tapInsertRecordEvent2), null);
				return null;
			}).when(batchReadFunction).batchRead(any(), eq(tapTable), any(), anyInt(), any()));
			TaskPreviewResultVO taskPreviewResultVO = new TaskPreviewResultVO(taskDto);
			taskPreviewResultVO.setStats(new TaskPReviewStatsVO());
			TaskPreviewInstance taskPreviewInstance = new TaskPreviewInstance();
			taskPreviewInstance.setTaskPreviewResultVO(taskPreviewResultVO);
			ReflectionTestUtils.setField(hazelcastPreviewSourcePdkDataNode, "taskPreviewInstance", taskPreviewInstance);

			List<TapInsertRecordEvent> read = hazelcastPreviewSourcePdkDataNode.read(previewReadOperation);
			assertNotNull(read);
			assertEquals(1, read.size());
			assertEquals(data1, read.get(0).getAfter());
			Map<String, TaskPreviewReadStatsVO> readStats = taskPreviewResultVO.getStats().getReadStats();
			assertEquals(1, read.size());
			TaskPreviewReadStatsVO readStatsVO = readStats.get(tableNode.getId());
			assertEquals(PDKMethod.SOURCE_BATCH_READ.name(), readStatsVO.getMethod());
			assertEquals(((TableNode) tableNode).getTableName(), readStatsVO.getTableName());
			assertEquals(tapAdvanceFilter.getLimit(), readStatsVO.getLimit());
			assertEquals(tapAdvanceFilter.getMatch(), readStatsVO.getMatch());
			assertEquals(read.size(), readStatsVO.getRows());
		}
	}

	@Nested
	@DisplayName("Method finishRead test")
	class finishReadTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			PreviewFinishReadOperation previewFinishReadOperation = new PreviewFinishReadOperation();
			previewFinishReadOperation.last(true);
			PreviewFinishReadOperation result = hazelcastPreviewSourcePdkDataNode.finishRead(previewFinishReadOperation);
			assertEquals(previewFinishReadOperation, result);
			assertTrue(((AtomicBoolean) ReflectionTestUtils.getField(hazelcastPreviewSourcePdkDataNode, "finishPreviewRead")).get());
		}

		@Test
		@DisplayName("test isList = false")
		void test2() {
			PreviewFinishReadOperation previewFinishReadOperation = new PreviewFinishReadOperation();
			previewFinishReadOperation.last(false);
			PreviewFinishReadOperation result = hazelcastPreviewSourcePdkDataNode.finishRead(previewFinishReadOperation);
			assertNull(result);
			assertTrue(((AtomicBoolean) ReflectionTestUtils.getField(hazelcastPreviewSourcePdkDataNode, "finishPreviewRead")).get());
		}
	}

	@Nested
	@DisplayName("Method mockIfNeed test")
	class mockIfNeedTest {
		@Test
		@DisplayName("test PreviewReadOperation")
		void test1() {
			List<?> handleResult = new ArrayList<>();
			PreviewReadOperation previewReadOperation = new PreviewReadOperation(tableNode.getId());
			TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create().limit(1).match(DataMap.create().kv("id", 1));
			previewReadOperation.setTapAdvanceFilter(tapAdvanceFilter);
			TapTable tapTable = new TapTable(((TableNode) tableNode).getTableName());
			tapTable.putField("id", new TapField("id", "Integer").tapType(new TapNumber()));
			tapTable.putField("xxx", new TapField("xxx", "String").tapType(new TapString()));
			tapTable.putField("yyy", new TapField("yyy", "Instant").tapType(new TapDateTime()));
			tapTable.putField("zzz", new TapField("zzz", "Double").tapType(new TapNumber()));
			TapTableMap tapTableMap = mock(TapTableMap.class);
			when(tapTableMap.get(anyString())).thenReturn(tapTable);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			TaskPreviewResultVO taskPreviewResultVO = new TaskPreviewResultVO(taskDto);
			taskPreviewResultVO.setStats(new TaskPReviewStatsVO());
			TaskPreviewInstance taskPreviewInstance = new TaskPreviewInstance();
			taskPreviewInstance.setTaskPreviewResultVO(taskPreviewResultVO);
			ReflectionTestUtils.setField(hazelcastPreviewSourcePdkDataNode, "taskPreviewInstance", taskPreviewInstance);
			hazelcastPreviewSourcePdkDataNode.mockIfNeed(handleResult, previewReadOperation);

			assertEquals(1, handleResult.size());
			assertInstanceOf(TapInsertRecordEvent.class, handleResult.get(0));
			assertEquals(4, ((TapInsertRecordEvent) handleResult.get(0)).getAfter().size());
			assertEquals(1, ((TapInsertRecordEvent) handleResult.get(0)).getAfter().get("id"));
			Map<String, TaskPreviewReadStatsVO> readStats = taskPreviewResultVO.getStats().getReadStats();
			assertEquals(1, handleResult.size());
			TaskPreviewReadStatsVO readStatsVO = readStats.get(tableNode.getId());
			assertEquals(HazelcastPreviewSourcePdkDataNode.MOCK_METHOD, readStatsVO.getMethod());
			assertEquals(((TableNode) tableNode).getTableName(), readStatsVO.getTableName());
			assertEquals(tapAdvanceFilter.getLimit(), readStatsVO.getLimit());
			assertEquals(tapAdvanceFilter.getMatch(), readStatsVO.getMatch());
			assertEquals(handleResult.size(), readStatsVO.getRows());
		}

		@Test
		@DisplayName("test PreviewMergeReadOperation")
		void test2() {
			List<?> handleResult = new ArrayList<>();
			PreviewMergeReadOperation previewMergeReadOperation = new PreviewMergeReadOperation(tableNode.getId(), null, 1);
			TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create().limit(1).match(DataMap.create().kv("id", 1));
			previewMergeReadOperation.setTapAdvanceFilter(tapAdvanceFilter);
			TapTable tapTable = new TapTable(((TableNode) tableNode).getTableName());
			tapTable.putField("id", new TapField("id", "Integer").tapType(new TapNumber()));
			tapTable.putField("xxx", new TapField("xxx", "String").tapType(new TapString()));
			tapTable.putField("yyy", new TapField("yyy", "Instant").tapType(new TapDateTime()));
			tapTable.putField("zzz", new TapField("zzz", "Double").tapType(new TapNumber()));
			TapTableMap tapTableMap = mock(TapTableMap.class);
			when(tapTableMap.get(anyString())).thenReturn(tapTable);
			when(dataProcessorContext.getTapTableMap()).thenReturn(tapTableMap);
			TaskPreviewResultVO taskPreviewResultVO = new TaskPreviewResultVO(taskDto);
			taskPreviewResultVO.setStats(new TaskPReviewStatsVO());
			TaskPreviewInstance taskPreviewInstance = new TaskPreviewInstance();
			taskPreviewInstance.setTaskPreviewResultVO(taskPreviewResultVO);
			ReflectionTestUtils.setField(hazelcastPreviewSourcePdkDataNode, "taskPreviewInstance", taskPreviewInstance);
			hazelcastPreviewSourcePdkDataNode.mockIfNeed(handleResult, previewMergeReadOperation);

			assertEquals(1, handleResult.size());
			assertInstanceOf(TapInsertRecordEvent.class, handleResult.get(0));
			assertEquals(4, ((TapInsertRecordEvent) handleResult.get(0)).getAfter().size());
			assertEquals(1, ((TapInsertRecordEvent) handleResult.get(0)).getAfter().get("id"));
			Map<String, TaskPreviewReadStatsVO> readStats = taskPreviewResultVO.getStats().getReadStats();
			assertEquals(1, handleResult.size());
			TaskPreviewReadStatsVO readStatsVO = readStats.get(tableNode.getId());
			assertEquals(HazelcastPreviewSourcePdkDataNode.MOCK_METHOD, readStatsVO.getMethod());
			assertEquals(((TableNode) tableNode).getTableName(), readStatsVO.getTableName());
			assertEquals(tapAdvanceFilter.getLimit(), readStatsVO.getLimit());
			assertEquals(tapAdvanceFilter.getMatch(), readStatsVO.getMatch());
			assertEquals(handleResult.size(), readStatsVO.getRows());
		}
	}

	@Nested
	@DisplayName("Method replyPreviewOperationData test")
	class replyPreviewOperationDataTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			List<TapEvent> handleResult = new ArrayList<>();
			DataMap data = DataMap.create().kv("id", 1).kv("xxx", "test").kv("yyy", Instant.now());
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().after(data);
			handleResult.add(tapInsertRecordEvent);
			PreviewMergeReadOperation previewMergeReadOperation = new PreviewMergeReadOperation(tableNode.getId(), null, 10);
			assertDoesNotThrow(() -> hazelcastPreviewSourcePdkDataNode.replyPreviewOperationData(handleResult, previewMergeReadOperation));
			MergeReadData mergeReadData = previewMergeReadOperation.replyData();
			assertNotNull(mergeReadData);
			List<Map<String, Object>> d = mergeReadData.getData();
			assertEquals(1, d.size());
			assertEquals(data, d.get(0));
		}

		@Test
		@DisplayName("test handle result is null")
		void test2() {
			PreviewMergeReadOperation previewMergeReadOperation = new PreviewMergeReadOperation(tableNode.getId(), null, 10);
			assertDoesNotThrow(() -> hazelcastPreviewSourcePdkDataNode.replyPreviewOperationData(null, previewMergeReadOperation));
			assertNull(previewMergeReadOperation.replyData());
		}

		@Test
		@DisplayName("test normal read operation")
		void test3() {
			List<TapEvent> handleResult = new ArrayList<>();
			DataMap data = DataMap.create().kv("id", 1).kv("xxx", "test").kv("yyy", Instant.now());
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().after(data);
			handleResult.add(tapInsertRecordEvent);
			PreviewReadOperation previewReadOperation = new PreviewReadOperation(tableNode.getId());
			assertDoesNotThrow(() -> hazelcastPreviewSourcePdkDataNode.replyPreviewOperationData(handleResult, previewReadOperation));
		}
	}

	@Nested
	@DisplayName("Method wrapTapdataEvents test")
	class wrapTapdataEventsTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			List<TapEvent> handleResult = new ArrayList<>();
			DataMap data = DataMap.create().kv("id", 1).kv("xxx", "test").kv("yyy", Instant.now());
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().after(data);
			handleResult.add(tapInsertRecordEvent);
			PreviewMergeReadOperation previewMergeReadOperation = new PreviewMergeReadOperation(tableNode.getId(), null, 10);
			List<TapdataEvent> tapdataEvents = hazelcastPreviewSourcePdkDataNode.wrapTapdataEvents(handleResult, previewMergeReadOperation);
			assertNotNull(tapdataEvents);
			assertEquals(1, tapdataEvents.size());
			TapdataEvent tapdataEvent = tapdataEvents.get(0);
			assertEquals(previewMergeReadOperation, tapdataEvent.getInfo(PreviewOperation.class.getSimpleName()));
		}

		@Test
		@DisplayName("test finish operation")
		void test2() {
			PreviewFinishReadOperation previewFinishReadOperation = new PreviewFinishReadOperation();
			List<TapdataEvent> tapdataEvents = hazelcastPreviewSourcePdkDataNode.wrapTapdataEvents(previewFinishReadOperation, previewFinishReadOperation);
			assertNotNull(tapdataEvents);
			assertEquals(1, tapdataEvents.size());
			assertInstanceOf(TapdataPreviewCompleteEvent.class, tapdataEvents.get(0));
		}
	}
}
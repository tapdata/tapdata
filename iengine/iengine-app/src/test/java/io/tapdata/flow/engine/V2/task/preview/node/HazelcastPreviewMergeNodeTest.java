package io.tapdata.flow.engine.V2.task.preview.node;

import com.hazelcast.jet.core.Processor;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataHeartbeatEvent;
import com.tapdata.entity.TapdataPreviewCompleteEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.MockTaskUtil;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.flow.engine.V2.task.preview.TaskPreviewNodeMergeResultVO;
import io.tapdata.flow.engine.V2.task.preview.entity.MergeCache;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewMergeReadOperation;
import io.tapdata.flow.engine.V2.task.preview.operation.PreviewOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-11-06 14:34
 **/
@DisplayName("Class HazelcastPreviewMergeNode Test")
class HazelcastPreviewMergeNodeTest {

	private ProcessorBaseContext processorBaseContext;
	private HazelcastPreviewMergeNode hazelcastPreviewMergeNode;
	private TaskDto taskDto;
	private Node node;
	private MergeTableNode mergeTableNode;

	@BeforeEach
	void setUp() {
		processorBaseContext = mock(ProcessorBaseContext.class);
		hazelcastPreviewMergeNode = new HazelcastPreviewMergeNode(processorBaseContext);
		hazelcastPreviewMergeNode = spy(hazelcastPreviewMergeNode);
		taskDto = MockTaskUtil.setUpTaskDtoByJsonFile("preview/tasklet/preview1.json");
		when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
		node = taskDto.getDag().getNodes().stream().filter(n -> n instanceof MergeTableNode).findFirst().orElse(null);
		mergeTableNode = (MergeTableNode) node;
		when(processorBaseContext.getNode()).thenReturn(node);
		doAnswer(invocationOnMock -> {
			throw ((Throwable) invocationOnMock.getArgument(0));
		}).when(hazelcastPreviewMergeNode).errorHandle(any());
		doAnswer(invocationOnMock -> {
			throw ((Throwable) invocationOnMock.getArgument(0));
		}).when(hazelcastPreviewMergeNode).errorHandle(any(), any());
	}

	@Nested
	@DisplayName("Method doInit test")
	class doInitTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			Processor.Context context = mock(Processor.Context.class);
			hazelcastPreviewMergeNode.doInit(context);
			Object mergeCacheMap = ReflectionTestUtils.getField(hazelcastPreviewMergeNode, "mergeCacheMap");
			assertInstanceOf(Map.class, mergeCacheMap);
			assertEquals(2, ((Map) mergeCacheMap).size());
		}
	}

	@Nested
	@DisplayName("Method tryProcess test")
	class tryProcessTest {
		@BeforeEach
		void setUp() {
			Processor.Context context = mock(Processor.Context.class);
			hazelcastPreviewMergeNode.doInit(context);
			doReturn(true).when(hazelcastPreviewMergeNode).isRunning();
		}

		@Test
		@DisplayName("test master table, insert event")
		void test1() {
			String preNodeId1 = mergeTableNode.getMergeProperties().get(0).getId();
			TapInsertRecordEvent tapInsertRecordEvent1 = TapInsertRecordEvent.create().after(DataMap.create().kv("CUSTOMER_ID", "1").kv("NAME", "test"));
			TapdataEvent tapdataEvent = new TapdataEvent();
			tapdataEvent.setTapEvent(tapInsertRecordEvent1);
			List<String> nodeIds = new ArrayList<>();
			nodeIds.add(preNodeId1);
			tapdataEvent.setNodeIds(nodeIds);
			PreviewMergeReadOperation previewMergeReadOperation = new PreviewMergeReadOperation(preNodeId1, null, 1);
			tapdataEvent.addInfo(PreviewOperation.class.getSimpleName(), previewMergeReadOperation);
			BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = spy(new TryProcessConsumer());
			assertDoesNotThrow(() -> hazelcastPreviewMergeNode.tryProcess(tapdataEvent, consumer));
			Object mergeCacheMapObj = ReflectionTestUtils.getField(hazelcastPreviewMergeNode, "mergeCacheMap");
			Map<String, MergeCache> mergeCacheMap = (Map<String, MergeCache>) mergeCacheMapObj;
			MergeCache mergeCache = mergeCacheMap.get(preNodeId1);
			List<Map<String, Object>> data = mergeCache.getData();
			assertEquals(1, data.size());
			assertEquals(tapInsertRecordEvent1.getAfter(), data.get(0));
			verify(consumer, never()).accept(any(), any());
			assertEquals(0, previewMergeReadOperation.getMergeNodeReceived().getCount());
		}

		@Test
		@DisplayName("test heartbeat event")
		void test2() {
			TapdataHeartbeatEvent tapdataHeartbeatEvent = new TapdataHeartbeatEvent();
			assertDoesNotThrow(() -> hazelcastPreviewMergeNode.tryProcess(tapdataHeartbeatEvent, (event, result) -> {
				assertEquals(tapdataHeartbeatEvent, event);
				assertNull(result);
			}));
		}

		@Test
		@DisplayName("test merge array")
		void test3() {
			String preNodeId1 = mergeTableNode.getMergeProperties().get(0).getId();
			TapInsertRecordEvent tapInsertRecordEvent1 = TapInsertRecordEvent.create().after(DataMap.create().kv("CUSTOMER_ID", "C1").kv("NAME", "test"));
			TapdataEvent tapdataEvent1 = new TapdataEvent();
			tapdataEvent1.setTapEvent(tapInsertRecordEvent1);
			List<String> nodeIds = new ArrayList<>();
			nodeIds.add(preNodeId1);
			tapdataEvent1.setNodeIds(nodeIds);
			PreviewMergeReadOperation previewMergeReadOperation = new PreviewMergeReadOperation(preNodeId1, null, 1);
			tapdataEvent1.addInfo(PreviewOperation.class.getSimpleName(), previewMergeReadOperation);

			String preNodeId2 = mergeTableNode.getMergeProperties().get(0).getChildren().get(0).getId();
			TapInsertRecordEvent tapInsertRecordEvent2 = TapInsertRecordEvent.create().after(DataMap.create().kv("POLICY_ID", "P1").kv("CUSTOMER_ID", "C1")
					.kv("XXX", LocalDateTime.of(2024, 11, 6, 17, 12, 45, 123456).toInstant(ZoneOffset.UTC)));
			TapdataEvent tapdataEvent2 = new TapdataEvent();
			tapdataEvent2.setTapEvent(tapInsertRecordEvent2);
			List<String> nodeIds2 = new ArrayList<>();
			nodeIds2.add(preNodeId2);
			tapdataEvent2.setNodeIds(nodeIds2);
			PreviewMergeReadOperation previewMergeReadOperation2 = new PreviewMergeReadOperation(preNodeId2, null, 1);
			tapdataEvent2.addInfo(PreviewOperation.class.getSimpleName(), previewMergeReadOperation2);

			TapdataPreviewCompleteEvent tapdataPreviewCompleteEvent = new TapdataPreviewCompleteEvent();

			TryProcessConsumer tryProcessConsumer = new TryProcessConsumer();

			assertDoesNotThrow(() -> hazelcastPreviewMergeNode.tryProcess(tapdataEvent1, tryProcessConsumer));
			assertDoesNotThrow(() -> hazelcastPreviewMergeNode.tryProcess(tapdataEvent2, tryProcessConsumer));
			hazelcastPreviewMergeNode.tryProcess(tapdataPreviewCompleteEvent, (event, result) -> {
				assertEquals(tapdataPreviewCompleteEvent, event);
				assertNull(result);
			});
			Object taskPreviewNodeMergeResultVOObj = ReflectionTestUtils.getField(hazelcastPreviewMergeNode, "taskPreviewNodeMergeResultVO");
			assertInstanceOf(TaskPreviewNodeMergeResultVO.class, taskPreviewNodeMergeResultVOObj);
			TaskPreviewNodeMergeResultVO taskPreviewNodeMergeResultVO = (TaskPreviewNodeMergeResultVO) taskPreviewNodeMergeResultVOObj;
			assertEquals("[{CUSTOMER_ID=C1, POLICY=[{CUSTOMER_ID=C1, XXX=2024-11-06T17:12:45.000123456Z, POLICY_ID=P1}], NAME=test}]",
					taskPreviewNodeMergeResultVO.getData().toString());
			assertEquals("{e3a5ef29-90d2-459e-90c5-d27e813e8d0c=[CUSTOMER_ID, NAME], 7c2e681f-a875-4bb2-b0b7-5ae5a9ac19d3=[POLICY.CUSTOMER_ID, POLICY.POLICY_ID, POLICY.XXX]}",
					((TaskPreviewNodeMergeResultVO) taskPreviewNodeMergeResultVOObj).getFieldsMapping().toString());
		}

		@Test
		@DisplayName("test merge document")
		void test4() {
			taskDto = MockTaskUtil.setUpTaskDtoByJsonFile("preview/tasklet/preview3.json");
			when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
			node = taskDto.getDag().getNodes().stream().filter(n -> n instanceof MergeTableNode).findFirst().orElse(null);
			mergeTableNode = (MergeTableNode) node;
			when(processorBaseContext.getNode()).thenReturn(node);
			hazelcastPreviewMergeNode.doInit(mock(Processor.Context.class));
			String preNodeId1 = mergeTableNode.getMergeProperties().get(0).getId();
			TapInsertRecordEvent tapInsertRecordEvent1 = TapInsertRecordEvent.create().after(DataMap.create().kv("CUSTOMER_ID", "C1").kv("NAME", "test"));
			TapdataEvent tapdataEvent1 = new TapdataEvent();
			tapdataEvent1.setTapEvent(tapInsertRecordEvent1);
			List<String> nodeIds = new ArrayList<>();
			nodeIds.add(preNodeId1);
			tapdataEvent1.setNodeIds(nodeIds);
			PreviewMergeReadOperation previewMergeReadOperation = new PreviewMergeReadOperation(preNodeId1, null, 1);
			tapdataEvent1.addInfo(PreviewOperation.class.getSimpleName(), previewMergeReadOperation);

			String preNodeId2 = mergeTableNode.getMergeProperties().get(0).getChildren().get(0).getId();
			TapInsertRecordEvent tapInsertRecordEvent2 = TapInsertRecordEvent.create().after(DataMap.create().kv("POLICY_ID", "P1").kv("CUSTOMER_ID", "C1")
					.kv("XXX", LocalDateTime.of(2024, 11, 6, 17, 12, 45, 123456).toInstant(ZoneOffset.UTC)));
			TapdataEvent tapdataEvent2 = new TapdataEvent();
			tapdataEvent2.setTapEvent(tapInsertRecordEvent2);
			List<String> nodeIds2 = new ArrayList<>();
			nodeIds2.add(preNodeId2);
			tapdataEvent2.setNodeIds(nodeIds2);
			PreviewMergeReadOperation previewMergeReadOperation2 = new PreviewMergeReadOperation(preNodeId2, null, 1);
			tapdataEvent2.addInfo(PreviewOperation.class.getSimpleName(), previewMergeReadOperation2);

			TapdataPreviewCompleteEvent tapdataPreviewCompleteEvent = new TapdataPreviewCompleteEvent();

			TryProcessConsumer tryProcessConsumer = new TryProcessConsumer();

			assertDoesNotThrow(() -> hazelcastPreviewMergeNode.tryProcess(tapdataEvent1, tryProcessConsumer));
			assertDoesNotThrow(() -> hazelcastPreviewMergeNode.tryProcess(tapdataEvent2, tryProcessConsumer));
			hazelcastPreviewMergeNode.tryProcess(tapdataPreviewCompleteEvent, (event, result) -> {
				assertEquals(tapdataPreviewCompleteEvent, event);
				assertNull(result);
			});
			Object taskPreviewNodeMergeResultVOObj = ReflectionTestUtils.getField(hazelcastPreviewMergeNode, "taskPreviewNodeMergeResultVO");
			assertInstanceOf(TaskPreviewNodeMergeResultVO.class, taskPreviewNodeMergeResultVOObj);
			TaskPreviewNodeMergeResultVO taskPreviewNodeMergeResultVO = (TaskPreviewNodeMergeResultVO) taskPreviewNodeMergeResultVOObj;
			assertEquals("[{CUSTOMER_ID=C1, CUSTOMER={CUSTOMER_ID=C1, XXX=2024-11-06T17:12:45.000123456Z, POLICY_ID=P1}, NAME=test}]",
					taskPreviewNodeMergeResultVO.getData().toString());
			assertEquals("{4afcabd5-0966-4111-8039-dc9dddfe24a6=[CUSTOMER_ID, NAME], 6ce9ad39-aed0-44e1-a112-446cb7cafccc=[CUSTOMER.CUSTOMER_ID, CUSTOMER.POLICY_ID, CUSTOMER.XXX]}",
					((TaskPreviewNodeMergeResultVO) taskPreviewNodeMergeResultVOObj).getFieldsMapping().toString());
		}

		public class TryProcessConsumer implements BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> {

			@Override
			public void accept(TapdataEvent tapdataEvent, HazelcastProcessorBaseNode.ProcessResult processResult) {

			}
		}
	}
}
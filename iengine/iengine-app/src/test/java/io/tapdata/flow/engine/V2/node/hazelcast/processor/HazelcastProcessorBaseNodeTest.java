package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.BaseTest;
import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataHeartbeatEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MigrateUnionProcessorNode;
import com.tapdata.tm.commons.dag.process.UnionProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.error.TapEventException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.data.HazelcastBlank;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-05-18 17:22
 **/
@DisplayName("Class HazelcastProcessorBaseNode Test")
class HazelcastProcessorBaseNodeTest extends BaseHazelcastNodeTest {

	private HazelcastProcessorBaseNode hazelcastProcessorBaseNode;

	@BeforeEach
	void setUp() {
		hazelcastProcessorBaseNode = mock(HazelcastProcessorBaseNode.class);
	}

	@Nested
	@DisplayName("Method initFilterCodec test")
	class initFilterCodecTest {
		@BeforeEach
		void setUp() {
			when(hazelcastProcessorBaseNode.initFilterCodec()).thenCallRealMethod();
		}

		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			TapCodecsFilterManager tapCodecsFilterManager = hazelcastProcessorBaseNode.initFilterCodec();
			assertNotNull(tapCodecsFilterManager);
			TapCodecsRegistry codecsRegistry = tapCodecsFilterManager.getCodecsRegistry();
			assertNotNull(codecsRegistry);
			Object classFromTapValueCodecMap = ReflectionTestUtils.getField(codecsRegistry, "classFromTapValueCodecMap");
			assertInstanceOf(ConcurrentHashMap.class, classFromTapValueCodecMap);
			assertTrue(((ConcurrentHashMap<?, ?>) classFromTapValueCodecMap).isEmpty());
			ToTapValueCodec<?> customToTapValueCodec = codecsRegistry.getCustomToTapValueCodec(byte[].class);
			assertNotNull(customToTapValueCodec);
		}
	}

	@Nested
	@DisplayName("Method initConcurrentExecutor test")
	class initConcurrentExecutorTest {
		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastProcessorBaseNode).initConcurrentExecutor();
			TaskDto taskDto = new TaskDto();
			taskDto.setId(new ObjectId());
			taskDto.setName("task 1");
			processorBaseContext = mock(ProcessorBaseContext.class);
			when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
			ReflectionTestUtils.setField(hazelcastProcessorBaseNode, "processorBaseContext", processorBaseContext);
			ReflectionTestUtils.setField(hazelcastProcessorBaseNode, "obsLogger", mockObsLogger);
		}

		@Test
		@DisplayName("test processor node, enable concurrent process")
		void test1() {
			Node unionProcessorNode = new UnionProcessorNode();
			((UnionProcessorNode) unionProcessorNode).setEnableConcurrentProcess(true);
			((UnionProcessorNode) unionProcessorNode).setConcurrentNum(4);
			when(hazelcastProcessorBaseNode.getNode()).thenReturn(unionProcessorNode);
			when(hazelcastProcessorBaseNode.supportConcurrentProcess()).thenReturn(true);

			hazelcastProcessorBaseNode.initConcurrentExecutor();

			assertNotNull(ReflectionTestUtils.getField(hazelcastProcessorBaseNode, "simpleConcurrentProcessor"));
		}

		@Test
		@DisplayName("test migrate processor node, enable concurrent process")
		void test2() {
			Node migrateUnionProcessorNode = new MigrateUnionProcessorNode();
			((MigrateUnionProcessorNode) migrateUnionProcessorNode).setEnableConcurrentProcess(true);
			((MigrateUnionProcessorNode) migrateUnionProcessorNode).setConcurrentNum(4);
			when(hazelcastProcessorBaseNode.getNode()).thenReturn(migrateUnionProcessorNode);
			when(hazelcastProcessorBaseNode.supportConcurrentProcess()).thenReturn(true);

			hazelcastProcessorBaseNode.initConcurrentExecutor();

			assertNotNull(ReflectionTestUtils.getField(hazelcastProcessorBaseNode, "simpleConcurrentProcessor"));
		}

		@Test
		@DisplayName("test processor node, disable concurrent process")
		void test3() {
			Node unionProcessorNode = new UnionProcessorNode();
			((UnionProcessorNode) unionProcessorNode).setEnableConcurrentProcess(false);
			when(hazelcastProcessorBaseNode.getNode()).thenReturn(unionProcessorNode);
			when(hazelcastProcessorBaseNode.supportConcurrentProcess()).thenReturn(true);

			hazelcastProcessorBaseNode.initConcurrentExecutor();

			assertNull(ReflectionTestUtils.getField(hazelcastProcessorBaseNode, "simpleConcurrentProcessor"));
		}

		@Test
		@DisplayName("test when task sync type is test run")
		void test4() {
			Node unionProcessorNode = new UnionProcessorNode();
			((UnionProcessorNode) unionProcessorNode).setEnableConcurrentProcess(true);
			((UnionProcessorNode) unionProcessorNode).setConcurrentNum(4);
			when(hazelcastProcessorBaseNode.getNode()).thenReturn(unionProcessorNode);
			when(hazelcastProcessorBaseNode.supportConcurrentProcess()).thenReturn(true);
			processorBaseContext.getTaskDto().setSyncType(TaskDto.SYNC_TYPE_TEST_RUN);

			hazelcastProcessorBaseNode.initConcurrentExecutor();

			assertNull(ReflectionTestUtils.getField(hazelcastProcessorBaseNode, "simpleConcurrentProcessor"));
		}

		@Test
		@DisplayName("test when task sync type is deduce schema")
		void test5() {
			Node unionProcessorNode = new UnionProcessorNode();
			((UnionProcessorNode) unionProcessorNode).setEnableConcurrentProcess(true);
			((UnionProcessorNode) unionProcessorNode).setConcurrentNum(4);
			when(hazelcastProcessorBaseNode.getNode()).thenReturn(unionProcessorNode);
			when(hazelcastProcessorBaseNode.supportConcurrentProcess()).thenReturn(true);
			processorBaseContext.getTaskDto().setSyncType(TaskDto.SYNC_TYPE_DEDUCE_SCHEMA);

			hazelcastProcessorBaseNode.initConcurrentExecutor();

			assertNull(ReflectionTestUtils.getField(hazelcastProcessorBaseNode, "simpleConcurrentProcessor"));
		}

		@Test
		@DisplayName("test when processor node not support concurrent process")
		void test6() {
			Node unionProcessorNode = new UnionProcessorNode();
			((UnionProcessorNode) unionProcessorNode).setEnableConcurrentProcess(true);
			((UnionProcessorNode) unionProcessorNode).setConcurrentNum(4);
			when(hazelcastProcessorBaseNode.getNode()).thenReturn(unionProcessorNode);
			when(hazelcastProcessorBaseNode.supportConcurrentProcess()).thenReturn(false);

			hazelcastProcessorBaseNode.initConcurrentExecutor();

			assertNull(ReflectionTestUtils.getField(hazelcastProcessorBaseNode, "simpleConcurrentProcessor"));
		}

		@Test
		@DisplayName("test concurrent num<=1")
		void test7() {
			Node unionProcessorNode = new UnionProcessorNode();
			((UnionProcessorNode) unionProcessorNode).setEnableConcurrentProcess(true);
			((UnionProcessorNode) unionProcessorNode).setConcurrentNum(1);
			when(hazelcastProcessorBaseNode.getNode()).thenReturn(unionProcessorNode);
			when(hazelcastProcessorBaseNode.supportConcurrentProcess()).thenReturn(true);

			hazelcastProcessorBaseNode.initConcurrentExecutor();

			assertNull(ReflectionTestUtils.getField(hazelcastProcessorBaseNode, "simpleConcurrentProcessor"));
		}
	}

	@Nested
	@DisplayName("Method tryProcess test")
	class tryProcessTest {
		private TapdataEvent tapdataEvent;
		private Node<?> processorNode;

		@BeforeEach
		void setUp() {
			taskDto = new TaskDto();
			taskDto.setId(new ObjectId());
			taskDto.setName("task 1");
			processorNode = spy(new UnionProcessorNode());
			processorNode.setId(UUID.randomUUID().toString());
			processorNode.setName("processor node 1");
			processorBaseContext = new ProcessorBaseContext.ProcessorBaseContextBuilder<>()
					.withTaskDto(taskDto)
					.withNode(processorNode)
					.build();
			hazelcastProcessorBaseNode = spy(new HazelcastBlank(processorBaseContext) {
				@Override
				protected boolean isRunning() {
					return true;
				}
			});
			ReflectionTestUtils.setField(hazelcastProcessorBaseNode, "obsLogger", mockObsLogger);
			hazelcastProcessorBaseNode.doInit(mockJetContext);

			tapdataEvent = new TapdataEvent();
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create().init()
					.after(new Document("id", 1).append("title", "xxxxx").append("created", new TapDateTimeValue(new DateTime(Instant.now()))))
					.table("table1");
			tapdataEvent.setTapEvent(tapInsertRecordEvent);
		}

		@Test
		@DisplayName("test single process")
		void test1() {
			doReturn(false).when(hazelcastProcessorBaseNode).supportBatchProcess();

			doAnswer(invocationOnMock -> {
				assertEquals(tapdataEvent, ((List<?>) invocationOnMock.getArgument(0)).get(0));
				return null;
			}).when(hazelcastProcessorBaseNode).enqueue(any(List.class));

			assertDoesNotThrow(() -> hazelcastProcessorBaseNode.tryProcess(0, tapdataEvent));
			verify(hazelcastProcessorBaseNode).singleProcess(eq(tapdataEvent), any(List.class));
			verify(hazelcastProcessorBaseNode, never()).batchProcess(tapdataEvent);
		}

		@Test
		@DisplayName("test batch process")
		void test2() {
			doReturn(true).when(hazelcastProcessorBaseNode).supportBatchProcess();
			CountDownLatch countDownLatch = new CountDownLatch(2);
			TapdataHeartbeatEvent tapdataHeartbeatEvent = new TapdataHeartbeatEvent();

			doAnswer(invocationOnMock -> {
				assertEquals(tapdataHeartbeatEvent, ((List<?>) invocationOnMock.getArgument(0)).get(0));
				countDownLatch.countDown();
				assertEquals(tapdataEvent, ((List<?>) invocationOnMock.getArgument(0)).get(1));
				countDownLatch.countDown();
				return null;
			}).when(hazelcastProcessorBaseNode).enqueue(any(List.class));

			assertDoesNotThrow(() -> hazelcastProcessorBaseNode.tryProcess(0, tapdataHeartbeatEvent));
			assertDoesNotThrow(() -> hazelcastProcessorBaseNode.tryProcess(0, tapdataEvent));
			assertDoesNotThrow(() -> countDownLatch.await(5L, TimeUnit.SECONDS));
			assertEquals(0, countDownLatch.getCount());
			verify(hazelcastProcessorBaseNode, never()).singleProcess(eq(tapdataEvent), any(List.class));
			verify(hazelcastProcessorBaseNode).batchProcess(eq(tapdataEvent));
		}

		@Test
		@DisplayName("test disable node")
		void test3() {
			doReturn(true).when(processorNode).disabledNode();
			doAnswer(invocationOnMock -> {
				Object argument1 = invocationOnMock.getArgument(0);
				assertInstanceOf(List.class, argument1);
				assertEquals(tapdataEvent, ((List<?>) argument1).get(0));
				return null;
			}).when(hazelcastProcessorBaseNode).enqueue(any(List.class));

			assertDoesNotThrow(() -> hazelcastProcessorBaseNode.tryProcess(0, tapdataEvent));
			verify(hazelcastProcessorBaseNode).enqueue(any(List.class));
			verify(hazelcastProcessorBaseNode, never()).singleProcess(eq(tapdataEvent), any(List.class));
			verify(hazelcastProcessorBaseNode, never()).batchProcess(eq(tapdataEvent));
		}

		@Test
		@DisplayName("test error handle")
		void test4() {
			doReturn(false).when(hazelcastProcessorBaseNode).supportBatchProcess();

			RuntimeException runtimeException = new RuntimeException("error");
			doAnswer(invocationOnMock -> {
				assertInstanceOf(TapEventException.class, invocationOnMock.getArgument(0));
				assertEquals(runtimeException, ((TapEventException) invocationOnMock.getArgument(0)).getCause());
				return null;
			}).when(hazelcastProcessorBaseNode).errorHandle(any(Throwable.class));
			doThrow(runtimeException).when(hazelcastProcessorBaseNode).singleProcess(any(TapdataEvent.class), any(List.class));
			assertDoesNotThrow(() -> hazelcastProcessorBaseNode.tryProcess(0, tapdataEvent));

			TapCodeException tapCodeException = new TapCodeException("1", runtimeException);
			doAnswer(invocationOnMock -> {
				assertEquals(tapCodeException, invocationOnMock.getArgument(0));
				assertEquals(runtimeException, ((TapCodeException) invocationOnMock.getArgument(0)).getCause());
				return null;
			}).when(hazelcastProcessorBaseNode).errorHandle(any(Throwable.class));
			doThrow(tapCodeException).when(hazelcastProcessorBaseNode).singleProcess(any(TapdataEvent.class), any(List.class));
			assertDoesNotThrow(() -> hazelcastProcessorBaseNode.tryProcess(0, tapdataEvent));

			TapCodeException tapCodeException1 = new TapCodeException("1");
			RuntimeException runtimeException1 = new RuntimeException("error 1", tapCodeException1);
			doAnswer(invocationOnMock -> {
				assertEquals(tapCodeException1, invocationOnMock.getArgument(0));
				assertNull(((TapCodeException) invocationOnMock.getArgument(0)).getCause());
				return null;
			}).when(hazelcastProcessorBaseNode).errorHandle(any(Throwable.class));
			doThrow(runtimeException1).when(hazelcastProcessorBaseNode).singleProcess(any(TapdataEvent.class), any(List.class));
			assertDoesNotThrow(() -> hazelcastProcessorBaseNode.tryProcess(0, tapdataEvent));

			verify(hazelcastProcessorBaseNode, times(3)).singleProcess(eq(tapdataEvent), any(List.class));
			verify(hazelcastProcessorBaseNode, never()).batchProcess(eq(tapdataEvent));
		}
	}
}
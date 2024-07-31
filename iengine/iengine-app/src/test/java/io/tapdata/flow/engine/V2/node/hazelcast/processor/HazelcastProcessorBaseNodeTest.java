package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.BaseTest;
import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MigrateUnionProcessorNode;
import com.tapdata.tm.commons.dag.process.UnionProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.ConcurrentHashMap;

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
}
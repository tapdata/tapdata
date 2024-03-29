package io.tapdata.flow.engine.V2.cleaner.impl;

import com.tapdata.constant.BeanUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.cleaner.CleanResult;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastMergeNode;
import io.tapdata.utils.AppType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/12 15:56 Create
 */
class MergeNodeCleanerTest {

	@Mock
	ClientMongoOperator clientMongoOperator;

	@InjectMocks
	MergeNodeCleaner mergeNodeCleaner;

	@BeforeEach
	public void setup() {
		MockitoAnnotations.openMocks(this);
	}

	@Nested
	class FindTaskByIdTest {
		@Test
		void testReturnsTaskDto() {
			try (MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
				String taskId = "testTaskId";
				TaskDto expectedTaskDto = new TaskDto();
				when(clientMongoOperator.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(expectedTaskDto);
				beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);

				TaskDto result = mergeNodeCleaner.findTaskById(taskId);

				assertEquals(expectedTaskDto, result);
				verify(clientMongoOperator, times(1)).findOne(any(Query.class), anyString(), eq(TaskDto.class));
			}
		}

		@Test
		void testReturnsNullWhenNoTaskFound() {
			try (MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
				String taskId = "testTaskId";
				when(clientMongoOperator.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(null);
				beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);

				TaskDto result = mergeNodeCleaner.findTaskById(taskId);

				assertEquals(null, result);
				verify(clientMongoOperator, times(1)).findOne(any(Query.class), anyString(), eq(TaskDto.class));
			}
		}
	}

	@Nested
	class GetMergeTableNodeTest {
		@Test
		void returnsMergeTableNodeWhenNodeIsInstanceOfMergeTableNode() {
			String nodeId = "testNodeId";
			MergeTableNode expectedNode = new MergeTableNode();

			DAG dag = mock(DAG.class);
			when(dag.getNode(nodeId)).thenReturn((Node) expectedNode);

			MergeTableNode result = mergeNodeCleaner.getMergeTableNode(dag, nodeId);

			assertEquals(expectedNode, result);
		}

		@Test
		void returnsNullWhenNodeIsNotInstanceOfMergeTableNode() {
			String nodeId = "testNodeId";
			DAG dag = mock(DAG.class);
			when(dag.getNode(nodeId)).thenReturn(null);

			MergeTableNode result = mergeNodeCleaner.getMergeTableNode(dag, nodeId);

			assertNull(result);
		}

		@Test
		void returnsNullWhenNodeIdDoesNotExistInDag() {
			String nodeId = "testNodeId";
			DAG dag = mock(DAG.class);

			MergeTableNode result = mergeNodeCleaner.getMergeTableNode(dag, nodeId);

			assertNull(result);
		}
	}

	@Nested
	class CleanTaskNodeByAppTypeTest {
		@Test
		void whenAppTypeIsCloud() {
			List<MergeTableNode> mergeTableNodes = Arrays.asList(new MergeTableNode(), new MergeTableNode());
			DAG dag = mock(DAG.class);
			when(dag.getNodes()).thenReturn(new ArrayList<>());
			when(dag.getEdges()).thenReturn(new LinkedList<>());
			try (MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class)) {
				AppType appType = mock(AppType.class);
				when(appType.isCloud()).thenReturn(true);
				appTypeMockedStatic.when(AppType::currentType).thenReturn(appType);

				try (MockedStatic<HazelcastMergeNode> ignoreMockedStatic = mockStatic(HazelcastMergeNode.class)) {
					mergeNodeCleaner.cleanTaskNodeByAppType(mergeTableNodes, dag);
					verify(HazelcastMergeNode.class, times(mergeTableNodes.size()));
					HazelcastMergeNode.clearCache(any(MergeTableNode.class), anyList(), anyList());
				}

			}
		}

		@Test
		void whenAppTypeIsNotCloud() {
			List<MergeTableNode> mergeTableNodes = Arrays.asList(new MergeTableNode(), new MergeTableNode());
			DAG dag = mock(DAG.class);
			try (MockedStatic<AppType> appTypeMockedStatic = mockStatic(AppType.class)) {
				AppType appType = mock(AppType.class);
				when(appType.isCloud()).thenReturn(false);
				appTypeMockedStatic.when(AppType::currentType).thenReturn(appType);

				try (MockedStatic<HazelcastMergeNode> ignoreMockedStatic = mockStatic(HazelcastMergeNode.class)) {
					mergeNodeCleaner.cleanTaskNodeByAppType(mergeTableNodes, dag);
					verify(HazelcastMergeNode.class, times(mergeTableNodes.size()));
					HazelcastMergeNode.clearCache(any(MergeTableNode.class));
				}

			}
		}
	}

	@Nested
	class CleanTaskNodeTest {
		@Test
		void whenTaskDtoIsNull() {
			try (MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
				String taskId = "testTaskId";
				String nodeId = "testNodeId";
				when(clientMongoOperator.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(null);
				beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);

				try (MockedStatic<CleanResult> cleanResultMockedStatic = mockStatic(CleanResult.class)) {
					CleanResult success = CleanResult.success();
					cleanResultMockedStatic.when(CleanResult::success).thenReturn(success);

					CleanResult result = mergeNodeCleaner.cleanTaskNode(taskId, nodeId);
					assertEquals(success, result);
				}
				verify(clientMongoOperator, times(1)).findOne(any(Query.class), anyString(), eq(TaskDto.class));
			}
		}

		@Test
		void whenNodeIdIsBlankAndNoMergeTableNodes() {
			String taskId = "testTaskId";
			TaskDto taskDto = new TaskDto();
			DAG dag = mock(DAG.class);
			taskDto.setDag(dag);
			try (
				MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class);
				MockedStatic<HazelcastMergeNode> ignoreMockedStatic = mockStatic(HazelcastMergeNode.class);
				MockedStatic<CleanResult> cleanResultMockedStatic = mockStatic(CleanResult.class)
			) {
				when(clientMongoOperator.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(taskDto);
				beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);

				CleanResult success = CleanResult.success();
				cleanResultMockedStatic.when(CleanResult::success).thenReturn(success);

				CleanResult result = mergeNodeCleaner.cleanTaskNode(taskId, "");

				assertEquals(CleanResult.success(), result);
				verify(clientMongoOperator, times(1)).findOne(any(Query.class), anyString(), eq(TaskDto.class));
			}
		}

		@Test
		void whenNodeIdIsNotBlankAndNoMergeTableNode() {
			String taskId = "testTaskId";
			String nodeId = "testNodeId";
			TaskDto taskDto = new TaskDto();
			DAG dag = mock(DAG.class);
			taskDto.setDag(dag);

			try (
				MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class);
				MockedStatic<HazelcastMergeNode> ignoreMockedStatic = mockStatic(HazelcastMergeNode.class);
				MockedStatic<CleanResult> cleanResultMockedStatic = mockStatic(CleanResult.class)
			) {
				when(clientMongoOperator.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(taskDto);
				beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);

				CleanResult success = CleanResult.success();
				cleanResultMockedStatic.when(CleanResult::success).thenReturn(success);

				CleanResult result = mergeNodeCleaner.cleanTaskNode(taskId, nodeId);

				assertEquals(CleanResult.success(), result);
				verify(clientMongoOperator, times(1)).findOne(any(Query.class), anyString(), eq(TaskDto.class));
			}
		}

		@Test
		void whenNodeIdIsNotBlankAndMergeTableNodeExists() {
			String taskId = "testTaskId";
			String nodeId = "testNodeId";
			TaskDto taskDto = new TaskDto();
			DAG dag = mock(DAG.class);
			when(dag.getNode(nodeId)).thenReturn((Node) new MergeTableNode());
			taskDto.setDag(dag);

			try (
				MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class);
				MockedStatic<HazelcastMergeNode> ignoreMockedStatic = mockStatic(HazelcastMergeNode.class);
				MockedStatic<CleanResult> cleanResultMockedStatic = mockStatic(CleanResult.class)
			) {
				when(clientMongoOperator.findOne(any(Query.class), anyString(), eq(TaskDto.class))).thenReturn(taskDto);
				beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);

				CleanResult success = CleanResult.success();
				cleanResultMockedStatic.when(CleanResult::success).thenReturn(success);

				CleanResult result = mergeNodeCleaner.cleanTaskNode(taskId, nodeId);

				assertEquals(CleanResult.success(), result);
				verify(clientMongoOperator, times(1)).findOne(any(Query.class), anyString(), eq(TaskDto.class));
			}
		}

	}
}

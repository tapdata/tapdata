package com.tapdata.tm.commons.dag;

import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.task.dto.Message;
import io.github.openlg.graphlib.Graph;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class DAGTest {
	DAG dag;

	@BeforeEach
	void init() {
		dag = mock(DAG.class);
	}

	@Nested
	class GetTaskDtoIsomorphismTest {
		List<Node> nodeList;

		@BeforeEach
		void init() {
			nodeList = mock(ArrayList.class);
			when(nodeList.size()).thenReturn(2);

			when(dag.getTaskDtoIsomorphism(anyList())).thenCallRealMethod();
			when(dag.getTaskDtoIsomorphism(null)).thenCallRealMethod();
		}

		@Nested
		class getConvertTableNameMapTest {
			TableRenameProcessNode tableRenameProcessNode;

			@BeforeEach
			void setUp() {
				tableRenameProcessNode = mock(TableRenameProcessNode.class);
			}

			@DisplayName("test get convert table name map")
			@Test
			void test1() {
				String tableName = "beforeTableName";
				ArrayList<String> tableNames = new ArrayList<>();
				tableNames.add(tableName);
				Map<String, TableRenameTableInfo> tableRenameTableInfoMap = new HashMap<>();
				when(tableRenameProcessNode.originalMap()).thenReturn(tableRenameTableInfoMap);
				when(tableRenameProcessNode.convertTableName(anyMap(), anyString(), eq(false))).thenReturn("convertNewTableName");
				Map<String, String> convertTableNameMap = DAG.getConvertTableNameMap(tableRenameProcessNode, tableNames);
				assertEquals("convertNewTableName", convertTableNameMap.get("beforeTableName"));
			}
		}

		@Nested
		class GenerateTableNameRelationTest {
			DatabaseNode sourceNode;
			DatabaseNode targetNode;

			@BeforeEach
			void setUp() {
				sourceNode = mock(DatabaseNode.class);
				targetNode = mock(DatabaseNode.class);
			}

			@DisplayName("test generateTableNameRelation when hava rename node")
			@Test
			void test1() {
				String tableName = "beforeTableName";
				ArrayList<String> tableNames = new ArrayList<>();
				tableNames.add(tableName);
				TableRenameProcessNode tableRenameProcessNode = mock(TableRenameProcessNode.class);
				Map<String, TableRenameTableInfo> tableRenameTableInfoMap = new HashMap<>();
				when(tableRenameProcessNode.originalMap()).thenReturn(tableRenameTableInfoMap);
				when(tableRenameProcessNode.convertTableName(anyMap(), anyString(), eq(false))).thenReturn("convertNewTableName");
				LinkedList<Node> nodes = new LinkedList<>();
				nodes.add(sourceNode);
				nodes.add(targetNode);
				nodes.add(tableRenameProcessNode);
				LinkedHashMap<String, String> tableNameRelation = DAG.generateTableNameRelation(nodes, tableNames);
				assertEquals("convertNewTableName", tableNameRelation.get("beforeTableName"));
				assertEquals("convertNewTableName", tableNames.get(0));
			}

			@DisplayName("test generateTableNameRelation when not hava rename node")
			@Test
			void test2() {
				String tableName = "beforeTableName";
				ArrayList<String> tableNames = new ArrayList<>();
				tableNames.add(tableName);
				LinkedList<Node> nodes = new LinkedList<>();
				nodes.add(sourceNode);
				nodes.add(targetNode);
				LinkedHashMap<String, String> tableNameRelation = DAG.generateTableNameRelation(nodes, tableNames);
				assertEquals("beforeTableName", tableNameRelation.get("beforeTableName"));
				assertEquals("beforeTableName", tableNames.get(0));
			}

		}

		@Test
		void testGetTaskDtoIsomorphismNormal() {
			DataParentNode node1 = mock(DataParentNode.class);
			when(node1.getDatabaseType()).thenReturn("mock-type");
			when(nodeList.get(0)).thenReturn(node1);

			DataParentNode node2 = mock(DataParentNode.class);
			when(node2.getDatabaseType()).thenReturn("mock-type");
			when(nodeList.get(1)).thenReturn(node2);

			boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
			Assertions.assertTrue(isomorphism);
			verify(nodeList, times(1)).size();
			verify(nodeList, times(1)).get(0);
			verify(nodeList, times(1)).get(1);

			verify(node1, times(1)).getDatabaseType();
			verify(node2, times(1)).getDatabaseType();
		}

		@Test
		void testGetTaskDtoIsomorphismNullNodeList() {
			when(nodeList.size()).thenReturn(0);
			boolean isomorphism = dag.getTaskDtoIsomorphism(null);
			Assertions.assertFalse(isomorphism);
			verify(nodeList, times(0)).size();
			verify(nodeList, times(0)).get(0);
			verify(nodeList, times(0)).get(1);
		}

		@Test
		void testGetTaskDtoIsomorphismEmptyNodeList() {
			when(nodeList.size()).thenReturn(0);
			boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
			Assertions.assertFalse(isomorphism);
			verify(nodeList, times(1)).size();
			verify(nodeList, times(0)).get(0);
			verify(nodeList, times(0)).get(1);
		}

		@Test
		void testGetTaskDtoIsomorphismMoreThanTwoNode() {
			when(nodeList.size()).thenReturn(100);
			boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
			Assertions.assertFalse(isomorphism);
			verify(nodeList, times(1)).size();
			verify(nodeList, times(0)).get(0);
			verify(nodeList, times(0)).get(1);
		}

		@Test
		void testGetTaskDtoIsomorphismLessThanTwoNode() {
			when(nodeList.size()).thenReturn(1);
			boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
			Assertions.assertFalse(isomorphism);
			verify(nodeList, times(1)).size();
			verify(nodeList, times(0)).get(0);
			verify(nodeList, times(0)).get(1);
		}

		@Test
		void testGetTaskDtoIsomorphismTwoNodeAndAllNodeAreDataParentNodeButDataTypeNotEquals() {
			when(nodeList.size()).thenReturn(2);
			DataParentNode node1 = mock(DataParentNode.class);
			when(node1.getDatabaseType()).thenReturn("mock-type-cache");
			when(nodeList.get(0)).thenReturn(node1);

			DataParentNode node2 = mock(DataParentNode.class);
			when(node2.getDatabaseType()).thenReturn("mock-type");
			when(nodeList.get(1)).thenReturn(node2);
			boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
			Assertions.assertFalse(isomorphism);
			verify(nodeList, times(1)).size();
			verify(nodeList, times(1)).get(0);
			verify(nodeList, times(1)).get(1);
			verify(node2, times(1)).getDatabaseType();
			verify(node2, times(1)).getDatabaseType();
		}

		@Test
		void testGetTaskDtoIsomorphismTwoNodeButNotAllNodeAreDataParentNode() {
			when(nodeList.size()).thenReturn(2);
			JsProcessorNode node1 = mock(JsProcessorNode.class);
			when(nodeList.get(0)).thenReturn(node1);

			DataParentNode node2 = mock(DataParentNode.class);
			when(node2.getDatabaseType()).thenReturn("mock-type");
			when(nodeList.get(1)).thenReturn(node2);
			boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
			Assertions.assertFalse(isomorphism);
			verify(nodeList, times(1)).size();
			verify(nodeList, times(1)).get(0);
			verify(nodeList, times(0)).get(1);
			verify(node2, times(0)).getDatabaseType();
		}

		@Test
		void testGetTaskDtoIsomorphismTwoNodeButNotAllNodeAreDataParentNode0() {
			when(nodeList.size()).thenReturn(2);
			JsProcessorNode node2 = mock(JsProcessorNode.class);
			when(nodeList.get(0)).thenReturn(node2);

			DataParentNode node1 = mock(DataParentNode.class);
			when(node1.getDatabaseType()).thenReturn("mock-type");
			when(nodeList.get(1)).thenReturn(node1);
			boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
			Assertions.assertFalse(isomorphism);
			verify(nodeList, times(1)).size();
			verify(nodeList, times(1)).get(0);
			verify(nodeList, times(0)).get(1);
			verify(node1, times(0)).getDatabaseType();
		}

		@Test
		void testGetTaskDtoIsomorphismTwoNodeButNotAllNodeAreDataParentNode1() {
			when(nodeList.size()).thenReturn(2);
			DataParentNode node2 = mock(DataParentNode.class);
			when(node2.getDatabaseType()).thenReturn("mock-type");
			when(nodeList.get(0)).thenReturn(node2);

			JsProcessorNode node1 = mock(JsProcessorNode.class);
			when(nodeList.get(1)).thenReturn(node1);
			boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
			Assertions.assertFalse(isomorphism);
			verify(nodeList, times(1)).size();
			verify(nodeList, times(1)).get(0);
			verify(nodeList, times(1)).get(1);
			verify(node2, times(0)).getDatabaseType();
		}

		@Test
		void testGetTaskDtoIsomorphismTwoNodeButAllNodeNotDataParentNode() {
			when(nodeList.size()).thenReturn(2);
			JsProcessorNode node1 = mock(JsProcessorNode.class);
			when(nodeList.get(0)).thenReturn(node1);

			JsProcessorNode node2 = mock(JsProcessorNode.class);
			when(nodeList.get(1)).thenReturn(node2);
			boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
			Assertions.assertFalse(isomorphism);
			verify(nodeList, times(1)).size();
			verify(nodeList, times(1)).get(0);
			verify(nodeList, times(0)).get(1);
		}

		@Test
		void testGetTaskDtoIsomorphismTwoNodeButAllNodeAreNull() {
			when(nodeList.size()).thenReturn(2);
			when(nodeList.get(0)).thenReturn(null);
			when(nodeList.get(1)).thenReturn(null);
			boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
			Assertions.assertFalse(isomorphism);
			verify(nodeList, times(1)).size();
			verify(nodeList, times(1)).get(0);
			verify(nodeList, times(0)).get(1);
		}

		@Test
		void testGetTaskDtoIsomorphismTwoNodeButNotAllNodeAreNull() {
			when(nodeList.size()).thenReturn(2);
			when(nodeList.get(0)).thenReturn(null);

			JsProcessorNode node2 = mock(JsProcessorNode.class);
			when(nodeList.get(1)).thenReturn(node2);
			boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
			Assertions.assertFalse(isomorphism);
			verify(nodeList, times(1)).size();
			verify(nodeList, times(1)).get(0);
			verify(nodeList, times(0)).get(1);
		}

		@Test
		void testGetTaskDtoIsomorphismTwoNodeButNotAllNodeAreNull1() {
			when(nodeList.size()).thenReturn(2);
			when(nodeList.get(1)).thenReturn(null);

			JsProcessorNode node2 = mock(JsProcessorNode.class);
			when(nodeList.get(0)).thenReturn(node2);
			boolean isomorphism = dag.getTaskDtoIsomorphism(nodeList);
			Assertions.assertFalse(isomorphism);
			verify(nodeList, times(1)).size();
			verify(nodeList, times(1)).get(0);
			verify(nodeList, times(0)).get(1);
		}
	}

	@Nested
	class SetIsomorphismValueToOptionsTest {
		DAG.Options options;
		List<Node> nodeList;

		@BeforeEach
		void init() {
			options = mock(DAG.Options.class);
			nodeList = mock(ArrayList.class);
			doNothing().when(options).setIsomorphismTask(anyBoolean());
			when(dag.getTaskDtoIsomorphism(anyList())).thenReturn(true);

			doCallRealMethod().when(dag).setIsomorphismValueToOptions(any(DAG.Options.class), anyList());
			doCallRealMethod().when(dag).setIsomorphismValueToOptions(null, nodeList);
		}

		@Test
		void testSetIsomorphismValueToOptionsNormal() {
			dag.setIsomorphismValueToOptions(options, nodeList);
			verify(dag, times(1)).getTaskDtoIsomorphism(nodeList);
			verify(options, times(1)).setIsomorphismTask(anyBoolean());
		}

		@Test
		void testSetIsomorphismValueToOptionsWithNullOptions() {
			dag.setIsomorphismValueToOptions(null, nodeList);
			verify(dag, times(0)).getTaskDtoIsomorphism(nodeList);
			verify(options, times(0)).setIsomorphismTask(anyBoolean());
		}

	}

	@Nested
	class TransformSchemaTest {
		@BeforeEach
		void setUp() {
			ReflectionTestUtils.setField(dag, "taskId", new ObjectId());
		}

		@Test
		void testConsumerIsNotNull() {
			doCallRealMethod().when(dag).transformSchema(any(), any(DAGDataService.class), any(DAG.Options.class), any());
			Assertions.assertThrows(RuntimeException.class, () -> {
				dag.transformSchema("test", mock(DAGDataServiceImpl.class), mock(DAG.Options.class), (e) -> {
					throw new RuntimeException(e);
				});
			});
		}

		@Test
		void testConsumerIsNull() {
			doCallRealMethod().when(dag).transformSchema(any(), any(DAGDataService.class), any(DAG.Options.class), any());
			Map<String, List<Message>> result = dag.transformSchema("test", mock(DAGDataServiceImpl.class), mock(DAG.Options.class), null);
			Assertions.assertEquals(1, result.size());
		}
	}

	@Nested
	@DisplayName("Method getAllTypeTargetNodes test")
	class getAllTypeTargetNodesTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			Graph<Node, Edge> graph = new Graph<>();
			TableNode node1 = new TableNode();
			node1.setId("node 1");
			TableNode node2 = new TableNode();
			node2.setId("node 2");
			TableNode node3 = new TableNode();
			node3.setId("node 3");
			graph.setNode(node1.getId(), node1);
			graph.setNode(node2.getId(), node2);
			graph.setNode(node3.getId(), node3);
			graph.setEdge(node1.getId(), node2.getId());
			graph.setEdge(node1.getId(), node3.getId());
			DAG dag = new DAG(graph);
			List<Node> allTypeTargetNodes = dag.getAllTypeTargetNodes();
			assertEquals(2, allTypeTargetNodes.size());
			assertSame(node2, allTypeTargetNodes.get(0));
			assertSame(node3, allTypeTargetNodes.get(1));
		}
	}

	@Nested
	@DisplayName("Method replaceNode test")
	class replaceNodeTest {

		private TableNode node1;
		private TableNode node2;
		private TableNode node3;
		private DAG dag;

		@BeforeEach
		void setUp() {
			Graph<Node, Edge> graph = new Graph<>();
			node1 = new TableNode();
			node1.setId("node 1");
			node2 = new TableNode();
			node2.setId("node 2");
			node3 = new TableNode();
			node3.setId("node 3");
			graph.setNode(node1.getId(), node1);
			graph.setNode(node2.getId(), node2);
			graph.setEdge(node1.getId(), node2.getId(), new Edge(node1.getId(), node2.getId()));
			dag = new DAG(graph);
		}

		@Test
		@DisplayName("test replace source node")
		void test1() {
			dag.replaceNode(node1, node3);
			assertNull(dag.getNode(node1.getId()));
			assertNotNull(dag.getNode(node3.getId()));
			LinkedList<Edge> edges = dag.getEdges();
			assertEquals(1, edges.size());
			Edge edge = edges.get(0);
			assertEquals(node3.getId(), edge.getSource());
			assertEquals(node2.getId(), edge.getTarget());
		}

		@Test
		@DisplayName("test replace target node")
		void test2() {
			Graph<Node, Edge> graph = new Graph<>();
			TableNode node1 = new TableNode();
			node1.setId("node 1");
			TableNode node2 = new TableNode();
			node2.setId("node 2");
			TableNode node3 = new TableNode();
			node3.setId("node 3");
			graph.setNode(node1.getId(), node1);
			graph.setNode(node2.getId(), node2);
			graph.setEdge(node1.getId(), node2.getId(), new Edge(node1.getId(), node2.getId()));
			DAG dag = new DAG(graph);
			dag.replaceNode(node2, node3);
			assertNull(dag.getNode(node2.getId()));
			assertNotNull(dag.getNode(node3.getId()));
			LinkedList<Edge> edges = dag.getEdges();
			assertEquals(1, edges.size());
			Edge edge = edges.get(0);
			assertEquals(node1.getId(), edge.getSource());
			assertEquals(node3.getId(), edge.getTarget());
		}
	}

	@Nested
	@DisplayName("Method addTargetNode test")
	class addTargetNodeTest {
		@Test
		@DisplayName("test add node2 after node1, expect: node1->node2")
		void test1() {
			Graph<Node, Edge> graph = new Graph<>();
			TableNode node1 = new TableNode();
			node1.setId("node 1");
			graph.setNode(node1.getId(), node1);
			TableNode node2 = new TableNode();
			node2.setId("node 2");
			DAG dag = new DAG(graph);
			dag.addTargetNode(node1, node2);
			assertNotNull(graph.getNode(node2.getId()));
			assertNotNull(graph.getEdge(node1.getId(), node2.getId()));
		}

		@Test
		@DisplayName("test add node3 into node1->node2, expect: node1->node3->node2")
		void test2() {
			Graph<Node, Edge> graph = new Graph<>();
			TableNode node1 = new TableNode();
			node1.setId("node 1");
			graph.setNode(node1.getId(), node1);
			TableNode node2 = new TableNode();
			node2.setId("node 2");
			DAG dag = new DAG(graph);
			graph.setNode(node2.getId(), node2);
			graph.setEdge(node1.getId(), node2.getId(), new Edge(node1.getId(), node2.getId()));
			TableNode node3 = new TableNode();
			node3.setId("node 3");

			dag.addTargetNode(node1, node3);
			assertNotNull(graph.getNode(node3.getId()));
			assertNotNull(graph.getEdge(node1.getId(), node3.getId()));
			assertNotNull(graph.getEdge(node3.getId(), node2.getId()));
		}
	}
}
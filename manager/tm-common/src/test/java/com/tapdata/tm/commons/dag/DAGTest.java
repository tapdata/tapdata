package com.tapdata.tm.commons.dag;

import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.task.dto.Message;
import io.github.openlg.graphlib.Graph;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.stream.Collectors;

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

	@Nested
	@DisplayName("Method addSourceNode test")
	class addSourceNodeTest {
		@Test
		@DisplayName("test add node2 before node1, expect: node2->node1")
		void test1() {
			Graph<Node, Edge> graph = new Graph<>();
			TableNode node1 = new TableNode();
			node1.setId("node 1");
			graph.setNode(node1.getId(), node1);
			TableNode node2 = new TableNode();
			node2.setId("node 2");
			DAG dag = new DAG(graph);
			dag.addSourceNode(node2, node1);
			assertNotNull(graph.getNode(node2.getId()));
			assertNotNull(graph.getEdge(node2.getId(), node1.getId()));
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

			dag.addSourceNode(node3, node2);
			assertNotNull(graph.getNode(node3.getId()));
			assertNotNull(graph.getEdge(node1.getId(), node3.getId()));
			assertNotNull(graph.getEdge(node3.getId(), node2.getId()));
		}
	}

	@Nested
	@DisplayName("ProcessDifferenceField Tests")
	class ProcessDifferenceFieldTest {
		private DAG.Options options;
		private MetadataInstancesDto dto;
		private Map<String, List<DifferenceField>> differenceFields;
		private Map<String, MetadataInstancesDto> targetMetadataInstancesDtos;
		private List<String> applyRules;
		private List<Field> fields;
		private SourceDto sourceConnection;

	@BeforeEach
	void setUp() {
		options = new DAG.Options();
		dto = new MetadataInstancesDto();
		differenceFields = new HashMap<>();
		targetMetadataInstancesDtos = new HashMap<>();
		applyRules = new ArrayList<>();
		fields = new ArrayList<>();
		sourceConnection = new SourceDto();

		// Set up basic DTO properties
		dto.setQualifiedName("test.table1");
		dto.setName("table1");
		dto.setFields(fields);
		dto.setSource(sourceConnection);
		sourceConnection.set_id("sourceId123");

		// Set up options using reflection
		ReflectionTestUtils.setField(options, "differenceFields", differenceFields);
		ReflectionTestUtils.setField(options, "targetMetadataInstancesDtos", targetMetadataInstancesDtos);
		ReflectionTestUtils.setField(options, "applyRules", applyRules);
	}

	@Test
	@DisplayName("Should process existing difference fields from differenceFields map")
	void testProcessDifferenceField_ExistingDifferenceFields() {
		// Given
		Field field1 = createField("field1", "varchar(255)", "TapString");
		Field field2 = createField("field2", "int", "TapNumber");
		fields.add(field1);
		fields.add(field2);

		List<DifferenceField> existingDifferences = new ArrayList<>();
		DifferenceField missingField = DifferenceField.buildMissingField("field1", field1);
		existingDifferences.add(missingField);

		differenceFields.put("test.table1", existingDifferences);

		// When
		options.processDifferenceField(dto);

		// Then
		assertEquals(1, fields.size()); // field1 should be removed
		assertEquals("field2", fields.get(0).getFieldName());
	}

	@Test
	@DisplayName("Should add new difference fields from schema comparison when target metadata exists")
	void testProcessDifferenceField_WithTargetMetadata() {
		// Given
		Field sourceField1 = createField("field1", "varchar(255)", "TapString");
		Field sourceField2 = createField("field2", "int", "TapNumber");
		fields.add(sourceField1);
		fields.add(sourceField2);

		MetadataInstancesDto targetDto = new MetadataInstancesDto();
		SourceDto targetConnection = new SourceDto();
		targetConnection.set_id("sourceId123"); // Same source ID
		targetDto.setSource(targetConnection);

		List<Field> targetFields = new ArrayList<>();
		Field targetField1 = createField("field1", "text", "TapString"); // Different type
		Field targetField3 = createField("field3", "datetime", "TapDateTime"); // Additional field
		targetFields.add(targetField1);
		targetFields.add(targetField3);
		targetDto.setFields(targetFields);

		targetMetadataInstancesDtos.put("table1", targetDto);
		applyRules.add("Different");
		applyRules.add("Additional");
		applyRules.add("Missing");

		// Mock SchemaUtils.compareSchema
		try (MockedStatic<SchemaUtils> schemaUtilsMock = mockStatic(SchemaUtils.class)) {
			List<DifferenceField> comparisonResult = new ArrayList<>();
			comparisonResult.add(DifferenceField.buildDifferentField("field1", sourceField1, targetField1));
			comparisonResult.add(DifferenceField.buildAdditionalField("field3", targetField3));
			comparisonResult.add(DifferenceField.buildMissingField("field2", sourceField2));

			schemaUtilsMock.when(() -> SchemaUtils.compareSchema(dto, targetDto))
				.thenReturn(comparisonResult);

			// When
			options.processDifferenceField(dto);

			// Then
			// Verify SchemaUtils.compareSchema was called
			schemaUtilsMock.verify(() -> SchemaUtils.compareSchema(dto, targetDto));

			// Verify field modifications
			assertEquals(2, fields.size()); // field2 removed (Missing), field3 added (Additional)

			// Check field1 was modified (Different)
			Field modifiedField1 = fields.stream()
				.filter(f -> "field1".equals(f.getFieldName()))
				.findFirst().orElse(null);
			assertNotNull(modifiedField1);
			assertEquals("text", modifiedField1.getDataType());

			// Check field3 was added (Additional)
			Field addedField3 = fields.stream()
				.filter(f -> "field3".equals(f.getFieldName()))
				.findFirst().orElse(null);
			assertNotNull(addedField3);
			assertEquals("datetime", addedField3.getDataType());
		}
	}

	@Test
	@DisplayName("Should not add difference fields when rule type not in applyRules")
	void testProcessDifferenceField_RuleTypeNotInApplyRules() {
		// Given
		Field sourceField1 = createField("field1", "varchar(255)", "TapString");
		fields.add(sourceField1);

		MetadataInstancesDto targetDto = new MetadataInstancesDto();
		SourceDto targetConnection = new SourceDto();
		targetConnection.set_id("sourceId123");
		targetDto.setSource(targetConnection);

		List<Field> targetFields = new ArrayList<>();
		Field targetField1 = createField("field1", "text", "TapString");
		targetFields.add(targetField1);
		targetDto.setFields(targetFields);

		targetMetadataInstancesDtos.put("table1", targetDto);
		applyRules.add("Missing"); // Only Missing rule, not Different

		try (MockedStatic<SchemaUtils> schemaUtilsMock = mockStatic(SchemaUtils.class)) {
			List<DifferenceField> comparisonResult = new ArrayList<>();
			comparisonResult.add(DifferenceField.buildDifferentField("field1", sourceField1, targetField1));

			schemaUtilsMock.when(() -> SchemaUtils.compareSchema(dto, targetDto))
				.thenReturn(comparisonResult);

			// When
			options.processDifferenceField(dto);

			// Then
			// field1 should not be modified because "Different" is not in applyRules
			assertEquals(1, fields.size());
			assertEquals("varchar(255)", fields.get(0).getDataType()); // Original data type
		}
	}

	@Test
	@DisplayName("Should not process when target metadata has different source ID")
	void testProcessDifferenceField_DifferentSourceId() {
		// Given
		Field sourceField1 = createField("field1", "varchar(255)", "TapString");
		fields.add(sourceField1);

		MetadataInstancesDto targetDto = new MetadataInstancesDto();
		SourceDto targetConnection = new SourceDto();
		targetConnection.set_id("differentSourceId"); // Different source ID
		targetDto.setSource(targetConnection);

		targetMetadataInstancesDtos.put("table1", targetDto);
		applyRules.add("Different");

		// When
		options.processDifferenceField(dto);

		// Then
		// Should not process because source IDs are different
		assertEquals(1, fields.size());
		assertEquals("varchar(255)", fields.get(0).getDataType());
	}

	@Test
	@DisplayName("Should handle null target metadata")
	void testProcessDifferenceField_NullTargetMetadata() {
		// Given
		Field sourceField1 = createField("field1", "varchar(255)", "TapString");
		fields.add(sourceField1);

		targetMetadataInstancesDtos.put("table1", null); // Null target metadata
		applyRules.add("Different");

		// When
		options.processDifferenceField(dto);

		// Then
		// Should not process because target metadata is null
		assertEquals(1, fields.size());
		assertEquals("varchar(255)", fields.get(0).getDataType());
	}

	@Test
	@DisplayName("Should handle empty applyRules")
	void testProcessDifferenceField_EmptyApplyRules() {
		// Given
		Field sourceField1 = createField("field1", "varchar(255)", "TapString");
		fields.add(sourceField1);

		MetadataInstancesDto targetDto = new MetadataInstancesDto();
		SourceDto targetConnection = new SourceDto();
		targetConnection.set_id("sourceId123");
		targetDto.setSource(targetConnection);

		List<Field> targetFields = new ArrayList<>();
		Field targetField1 = createField("field1", "text", "TapString");
		targetFields.add(targetField1);
		targetDto.setFields(targetFields);

		targetMetadataInstancesDtos.put("table1", targetDto);
		// applyRules is empty

		try (MockedStatic<SchemaUtils> schemaUtilsMock = mockStatic(SchemaUtils.class)) {
			List<DifferenceField> comparisonResult = new ArrayList<>();
			comparisonResult.add(DifferenceField.buildDifferentField("field1", sourceField1, targetField1));

			schemaUtilsMock.when(() -> SchemaUtils.compareSchema(dto, targetDto))
				.thenReturn(comparisonResult);

			// When
			options.processDifferenceField(dto);

			// Then
			// Should not add any differences because applyRules is empty
			assertEquals(1, fields.size());
			assertEquals("varchar(255)", fields.get(0).getDataType());
		}
	}

		@Test
		@DisplayName("Should not add duplicate difference fields")
		void testProcessDifferenceField_NoDuplicateFields() {
			// Given
			Field sourceField1 = createField("field1", "varchar(255)", "TapString");
			fields.add(sourceField1);

			// Add existing difference field
			List<DifferenceField> existingDifferences = new ArrayList<>();
			DifferenceField existingDifferent = DifferenceField.buildDifferentField("field1", sourceField1, createField("field1", "text", "TapString"));
			existingDifferences.add(existingDifferent);
			differenceFields.put("test.table1", existingDifferences);

			// Set up target metadata that would generate the same difference
			MetadataInstancesDto targetDto = new MetadataInstancesDto();
			SourceDto targetConnection = new SourceDto();
			targetConnection.set_id("sourceId123");
			targetDto.setSource(targetConnection);

			List<Field> targetFields = new ArrayList<>();
			Field targetField1 = createField("field1", "text", "TapString");
			targetFields.add(targetField1);
			targetDto.setFields(targetFields);

			targetMetadataInstancesDtos.put("table1", targetDto);
			applyRules.add("Different");

			try (MockedStatic<SchemaUtils> schemaUtilsMock = mockStatic(SchemaUtils.class)) {
				List<DifferenceField> comparisonResult = new ArrayList<>();
				comparisonResult.add(DifferenceField.buildDifferentField("field1", sourceField1, targetField1));

				schemaUtilsMock.when(() -> SchemaUtils.compareSchema(dto, targetDto))
					.thenReturn(comparisonResult);

				// When
				options.processDifferenceField(dto);

				// Then
				// Should not add duplicate, field should be modified only once
				assertEquals(1, fields.size());
				assertEquals("text", fields.get(0).getDataType());
			}
		}

		@Test
		@DisplayName("Should handle all difference types correctly")
		void testProcessDifferenceField_AllDifferenceTypes() {
			// Given
			Field field1 = createField("field1", "varchar(255)", "TapString");
			Field field2 = createField("field2", "int", "TapNumber");
			Field field3 = createField("field3", "datetime", "TapDateTime");
			Field field4 = createField("field4", "boolean", "TapBoolean");
			fields.add(field1);
			fields.add(field2);
			fields.add(field3);
			fields.add(field4);

			List<DifferenceField> existingDifferences = new ArrayList<>();
			// Missing - field2 will be removed
			existingDifferences.add(DifferenceField.buildMissingField("field2", field2));
			// Different - field1 will be modified
			existingDifferences.add(DifferenceField.buildDifferentField("field1", field1, createField("field1", "text", "TapString")));
			// Additional - field5 will be added
			existingDifferences.add(DifferenceField.buildAdditionalField("field5", createField("field5", "decimal", "TapNumber")));
			// CannotWrite - field4 will be removed
			existingDifferences.add(DifferenceField.buildCannotWriteField("field4", field4, createField("field4", "bit", "TapBoolean.cannotWrite")));

			differenceFields.put("test.table1", existingDifferences);

			// When
			options.processDifferenceField(dto);

			// Then
			assertEquals(3, fields.size()); // field2 and field4 removed, field5 added

			// Check field1 was modified (Different)
			Field modifiedField1 = fields.stream()
				.filter(f -> "field1".equals(f.getFieldName()))
				.findFirst().orElse(null);
			assertNotNull(modifiedField1);
			assertEquals("text", modifiedField1.getDataType());

			// Check field3 remains unchanged
			Field unchangedField3 = fields.stream()
				.filter(f -> "field3".equals(f.getFieldName()))
				.findFirst().orElse(null);
			assertNotNull(unchangedField3);
			assertEquals("datetime", unchangedField3.getDataType());

			// Check field5 was added (Additional)
			Field addedField5 = fields.stream()
				.filter(f -> "field5".equals(f.getFieldName()))
				.findFirst().orElse(null);
			assertNotNull(addedField5);
			assertEquals("decimal", addedField5.getDataType());

			// Check field2 and field4 were removed
			assertFalse(fields.stream().anyMatch(f -> "field2".equals(f.getFieldName())));
			assertFalse(fields.stream().anyMatch(f -> "field4".equals(f.getFieldName())));
		}

		@Test
		@DisplayName("Should handle edge case with null field in fieldMap")
		void testProcessDifferenceField_NullFieldInMap() {
			// Given
			Field field1 = createField("field1", "varchar(255)", "TapString");
			fields.add(field1);

			List<DifferenceField> existingDifferences = new ArrayList<>();
			// Create a difference field for a non-existent field
			DifferenceField missingField = DifferenceField.buildMissingField("nonExistentField", createField("nonExistentField", "int", "TapNumber"));
			existingDifferences.add(missingField);

			differenceFields.put("test.table1", existingDifferences);

			// When & Then - Should not throw exception even with null field
			assertDoesNotThrow(() -> options.processDifferenceField(dto));

			// Original field should remain unchanged
			assertEquals(1, fields.size());
			assertEquals("field1", fields.get(0).getFieldName());
		}

		/**
		 * Helper method to create a Field with specified properties
		 */
		private Field createField(String fieldName, String dataType, String tapType) {
			Field field = new Field();
			field.setFieldName(fieldName);
			field.setDataType(dataType);
			field.setTapType(tapType);
			return field;
		}
	}
}
package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import io.github.openlg.graphlib.Graph;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.*;

import static org.mockito.Mockito.when;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/24 16:03 Create
 */
class HuaweiDrsKafkaConvertorNodeTest {

    ObjectId taskId;
    HuaweiDrsKafkaConvertorNode node;
    HuaweiDrsKafkaConvertorNode mockNode;
    DAGDataServiceImpl mockService;
    DAG.Options options;

    @BeforeEach
    void setUp() {
        taskId = ObjectId.get();
        Graph<Node, Edge> graph = new Graph<>();
        DAG dag = new DAG(graph);

        node = new HuaweiDrsKafkaConvertorNode();
        node.setId(UUID.randomUUID().toString());
        node.setGraph(graph);
        node.setDag(dag);
        node.getDag().setTaskId(taskId);

        TableNode tableNode = new TableNode();
        tableNode.setId(UUID.randomUUID().toString());
        tableNode.setOutputSchema(Optional.of(new Schema()).map(schema -> {
            schema.setFields(new ArrayList<>());
            schema.getFields().add(Optional.of(new Field()).map(field -> {
                field.setId("id");
                field.setFieldName("id");
                return field;
            }).get());
            schema.getFields().add(Optional.of(new Field()).map(field -> {
                field.setId("title");
                field.setFieldName("title");
                field.setSourceDbType("mysql");
                return field;
            }).get());
            return schema;
        }).get());
        tableNode.setGraph(graph);
        tableNode.setDag(dag);
        tableNode.getDag().setTaskId(taskId);

        graph.setNode(node.getId(), node);
        graph.setNode(tableNode.getId(), tableNode);
        graph.setEdge(tableNode.getId(), node.getId(), new Edge(tableNode.getId(), node.getId()));

        mockService = Mockito.mock(DAGDataServiceImpl.class);
        node.setService(mockService);
        mockNode = Mockito.spy(node);

        options = new DAG.Options();
        options.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        options.setPreview(false);
    }

    @Nested
    class LoadSchemaTest {

        @Test
        void testNotfoundTask() {
            Mockito.when(mockService.getTaskById(Mockito.any())).thenReturn(null);

            Schema resultSchema = mockNode.loadSchema(options);
            Assertions.assertNull(resultSchema);
        }

        @Test
        void testSuccess() {
            TaskDto findTask = new TaskDto();
            findTask.setTransformTaskId(taskId.toHexString());
            Mockito.when(mockService.getTaskById(Mockito.any())).thenReturn(findTask);

            TapTable mockTapTable = new TapTable();
            mockTapTable.setNameFieldMap(new LinkedHashMap<>());
            mockTapTable.getNameFieldMap().put("id", new TapField("id", "varchar"));
            mockTapTable.getNameFieldMap().put("name", new TapField("name", "varchar"));
            Mockito.when(mockService.loadTapTable(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(mockTapTable);

            DataSourceDefinitionDto dataSourceDefinitionDto = new DataSourceDefinitionDto();
            dataSourceDefinitionDto.setExpression("{}");
            Mockito.when(mockService.getDefinitionByType(Mockito.eq("mysql"))).thenReturn(dataSourceDefinitionDto);

            Schema resultSchema = mockNode.loadSchema(options);
            Assertions.assertNotNull(resultSchema);
            Assertions.assertNotNull(resultSchema.getFields());
            Assertions.assertEquals(2, resultSchema.getFields().size());
        }
    }

    @Nested
    class BuildSampleTaskTest {
        @Test
        void testNotfoundDAG() {
            TaskDto findTask = new TaskDto();
            findTask.setTransformTaskId(taskId.toHexString());
            when(mockService.getTaskById(Mockito.any())).thenReturn(findTask);
            Mockito.doReturn(null).when(mockNode).buildSampleDag(Mockito.any());

            TaskDto sampleTask = mockNode.buildSampleTask("");
            Assertions.assertNull(sampleTask);
        }
    }

    @Nested
    class ResetFieldTypeWithFirstExpressionTest {
        @Test
        void testEmptySchema() {
            List<Schema> inputSchemas = new ArrayList<>();
            TapTable sampleTapTable = new TapTable();
            Assertions.assertDoesNotThrow(() -> {
                mockNode.resetFieldTypeWithFirstExpression(inputSchemas, sampleTapTable);
            });
        }

        @Test
        void testEmptySchemaFields() {
            Schema schema = new Schema();
            schema.setFields(new ArrayList<>());
            List<Schema> inputSchemas = new ArrayList<>();
            inputSchemas.add(schema);
            TapTable sampleTapTable = new TapTable();
            Assertions.assertDoesNotThrow(() -> {
                mockNode.resetFieldTypeWithFirstExpression(inputSchemas, sampleTapTable);
            });
        }
    }

    @Nested
    class BuildSampleDagTest {
        @Test
        void testParseJsonNull() {
            try (MockedStatic<JsonUtil> jsonUtilMockedStatic = Mockito.mockStatic(JsonUtil.class)) {
                jsonUtilMockedStatic.when(() -> JsonUtil.parseJsonUseJackson(Mockito.anyString(), Mockito.eq(Dag.class))).thenReturn(null);
                DAG result = mockNode.buildSampleDag("");
                Assertions.assertNull(result);
            }
        }
    }

    @Nested
    class MergeSchemaTest {

        @Test
        void testUseInputSchema() {
            Schema schema = Mockito.mock(Schema.class);
            Assertions.assertEquals(schema, mockNode.mergeSchema(null, schema, null));
        }

        @Test
        void testWithSuperMerge() {
            Schema outputSchema = Mockito.mock(Schema.class);

            try (MockedStatic<SchemaUtils> mockStatic = Mockito.mockStatic(SchemaUtils.class)) {
                mockStatic.when(() -> SchemaUtils.mergeSchema(Mockito.any(), Mockito.any())).thenReturn(outputSchema);
                Assertions.assertEquals(outputSchema, mockNode.mergeSchema(null, null, null));
            }
        }

    }
}

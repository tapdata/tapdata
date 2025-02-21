package com.tapdata.tm.commons.dag.nodes;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.FieldProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateUnionProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldProcess;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import io.github.openlg.graphlib.Graph;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.mockito.Mockito.*;

public class DatabaseNodeTest {
    DatabaseNode databaseNode;

    @BeforeEach
    void init() {
        databaseNode = mock(DatabaseNode.class);
    }
    @Nested
    class transformSchemaTest {
        @DisplayName("test transformSchema main process ")
        @Test
        void test() {
            DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
            ReflectionTestUtils.setField(databaseNode, "tableNames", new ArrayList<>());
            ReflectionTestUtils.setField(databaseNode, "service", dagDataService);
            DAG.Options options = new DAG.Options();
            options.setBatchNum(1);
            doCallRealMethod().when(databaseNode).transformSchema(options);
            List<String> tableNames = new ArrayList<>();
            tableNames.add("test1");
            Graph graph = mock(Graph.class);
            when(databaseNode.getGraph()).thenReturn(graph);
            when(graph.predecessors(any())).thenReturn(new ArrayList<>());
            when(databaseNode.getSourceNodeTableNames(any())).thenReturn(tableNames);
            databaseNode.transformSchema(options);
            verify(dagDataService, times(0)).initializeModel(false);

        }

        @DisplayName("test transformSchema initializeModel ")
        @Test
        void test1() {
            DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
            ReflectionTestUtils.setField(databaseNode, "tableNames", new ArrayList<>());
            ReflectionTestUtils.setField(databaseNode, "service", dagDataService);
            DAG.Options options = new DAG.Options();
            options.setBatchNum(1);
            doCallRealMethod().when(databaseNode).transformSchema(options);
            List<String> tableNames = new ArrayList<>();
            tableNames.add("test1");
            Graph graph = mock(Graph.class);
            when(databaseNode.getGraph()).thenReturn(graph);
            when(graph.predecessors(any())).thenReturn(new ArrayList<>());
            when(databaseNode.getSourceNodeTableNames(any())).thenReturn(tableNames);
            when(dagDataService.whetherEngineDeduction()).thenReturn(true);
            databaseNode.transformSchema(options);
            verify(dagDataService, times(1)).initializeModel(false);

        }
    }

    @Nested
    class LoadSchemaTest {
        @Test
        void test_hasMigrateUnionNode(){
            DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
            ReflectionTestUtils.setField(databaseNode, "service", dagDataService);
            List<String> includes = new ArrayList<>();
            includes.add("test");
            when(databaseNode.getSyncType()).thenReturn("migrate");
            LinkedList<Node<?>> nodes = new LinkedList<>();
            nodes.add(new MigrateUnionProcessorNode());
            when(databaseNode.getPreNodes(any())).thenReturn(nodes);
            doCallRealMethod().when(databaseNode).loadSchema(includes);
            Assertions.assertEquals(0,databaseNode.loadSchema(includes).size());
        }
        @Test
        void test_main(){
            DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
            ReflectionTestUtils.setField(databaseNode, "service", dagDataService);
            List<String> includes = new ArrayList<>();
            includes.add("test");
            when(databaseNode.getSyncType()).thenReturn("migrate");
            when(dagDataService.loadSchema(any(),any(),any(),any())).thenReturn(new ArrayList<>());
            doCallRealMethod().when(databaseNode).loadSchema(includes);
            Assertions.assertEquals(0,databaseNode.loadSchema(includes).size());
        }
    }

    @Test
    public void testFieldDdlEvent() throws Exception {

        DatabaseNode databaseNode = new DatabaseNode();
        FieldProcess fieldProcess = new FieldProcess();
        fieldProcess.setOperations(Arrays.asList(new FieldProcessorNode.Operation()));
        databaseNode.setFieldProcess(Collections.singletonList(fieldProcess));

        databaseNode.setSyncSourcePartitionTableEnable(false);
        TapCreateTableEvent event = new TapCreateTableEvent();
        event.setTableId("test_1");
        TapTable tapTable = new TapTable();
        event.setTable(tapTable);
        event.getTable().setId("test_1");
        event.getTable().setName("test_1");
        event.getTable().setPartitionMasterTableId("test");
        databaseNode.fieldDdlEvent(event);

        Assertions.assertNotNull(databaseNode.getTableNames());
        Assertions.assertEquals(1, databaseNode.getTableNames().size());

        databaseNode.setSyncSourcePartitionTableEnable(true);
        databaseNode.fieldDdlEvent(event);

        Assertions.assertNotNull(databaseNode.getTableNames());
        Assertions.assertEquals(1, databaseNode.getTableNames().size());

        databaseNode.fieldDdlEvent(event);
        Assertions.assertEquals(1, databaseNode.getTableNames().size());

        event.setTableId("test");
        event.getTable().setId("test");
        event.getTable().setName("test");
        event.getTable().setPartitionMasterTableId(null);
        databaseNode.fieldDdlEvent(event);
        Assertions.assertEquals(2, databaseNode.getTableNames().size());

        TapDropTableEvent dropEvent = new TapDropTableEvent();
        dropEvent.setTableId("test");
        databaseNode.fieldDdlEvent(dropEvent);
        Assertions.assertEquals(1, databaseNode.getTableNames().size());
    }
    @Nested
    class MergeSchemaTest{
        DatabaseNode databaseNode;
        @BeforeEach
        void beforeEach(){
            databaseNode = mock(DatabaseNode.class);
            databaseNode.setTableNames(new ArrayList<>());
            DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
            ReflectionTestUtils.setField(databaseNode, "connectionId", "connectionId");
            ReflectionTestUtils.setField(databaseNode, "service", dagDataService);
            DataSourceConnectionDto dataSourceConnectionDto = new DataSourceConnectionDto();
            dataSourceConnectionDto.setDatabase_type("MongoDB");
            when(dagDataService.getDataSource("connectionId")).thenReturn(dataSourceConnectionDto);
            doCallRealMethod().when(databaseNode).mergeSchema(any(), any(), any());
        }
        @Test
        void test_mongoSchema_not_exist_id(){
            DAG dag = mock(DAG.class);
            when(dag.getSyncType()).thenReturn("logCollector");
            when(databaseNode.getDag()).thenReturn(dag);
            List<List<Schema>> inputSchemas = new ArrayList<>();
            List<Schema> schemas = new ArrayList<>();
            Schema schema = new Schema();
            schema.setName("test1");
            schema.setOriginalName("test1");
            schema.setAncestorsName("test1");
            List<Field> fields = new ArrayList<>();
            Field field = new Field();
            field.setFieldName("test1");
            field.setPrimaryKey(false);
            fields.add(field);
            schema.setFields(fields);
            schemas.add(schema);
            inputSchemas.add(schemas);
            when(databaseNode.transformFields(any(), any(),any())).thenReturn(fields);
            List<Schema> result = databaseNode.mergeSchema(inputSchemas,schemas, new DAG.Options());
            Assertions.assertEquals(1,result.size());
            Assertions.assertEquals(2,result.get(0).getFields().size());
        }

        @Test
        void test_mongoSchema_exist_id(){
            DAG dag = mock(DAG.class);
            when(dag.getSyncType()).thenReturn("logCollector");
            when(databaseNode.getDag()).thenReturn(dag);
            List<List<Schema>> inputSchemas = new ArrayList<>();
            List<Schema> schemas = new ArrayList<>();
            Schema schema = new Schema();
            schema.setName("test1");
            schema.setOriginalName("test1");
            schema.setAncestorsName("test1");
            List<Field> fields = new ArrayList<>();
            Field field = new Field();
            field.setFieldName("_id");
            field.setPrimaryKey(true);
            fields.add(field);
            schema.setFields(fields);
            schemas.add(schema);
            inputSchemas.add(schemas);
            when(databaseNode.transformFields(any(), any(),any())).thenReturn(fields);
            List<Schema> result = databaseNode.mergeSchema(inputSchemas,schemas, new DAG.Options());
            Assertions.assertEquals(1,result.size());
            Assertions.assertEquals(1,result.get(0).getFields().size());
        }

        @Test
        void test_mongoSchema_fields_is_empty(){
            DAG dag = mock(DAG.class);
            when(dag.getSyncType()).thenReturn("logCollector");
            when(databaseNode.getDag()).thenReturn(dag);
            List<List<Schema>> inputSchemas = new ArrayList<>();
            List<Schema> schemas = new ArrayList<>();
            Schema schema = new Schema();
            schema.setName("test1");
            schema.setOriginalName("test1");
            schema.setAncestorsName("test1");
            List<Field> fields = new ArrayList<>();
            schema.setFields(fields);
            schemas.add(schema);
            inputSchemas.add(schemas);
            when(databaseNode.transformFields(any(), any(),any())).thenReturn(fields);
            List<Schema> result = databaseNode.mergeSchema(inputSchemas,schemas, new DAG.Options());
            Assertions.assertEquals(1,result.size());
            Assertions.assertEquals(0,result.get(0).getFields().size());
        }

        @Test
        void test_mongoSchema_hasPrimaryKey(){
            DAG dag = mock(DAG.class);
            when(dag.getSyncType()).thenReturn("logCollector");
            when(databaseNode.getDag()).thenReturn(dag);
            List<List<Schema>> inputSchemas = new ArrayList<>();
            List<Schema> schemas = new ArrayList<>();
            Schema schema = new Schema();
            schema.setName("test1");
            schema.setOriginalName("test1");
            schema.setAncestorsName("test1");
            List<Field> fields = new ArrayList<>();
            Field field = new Field();
            field.setFieldName("test1");
            field.setPrimaryKey(true);
            fields.add(field);
            schema.setFields(fields);
            schemas.add(schema);
            inputSchemas.add(schemas);
            when(databaseNode.transformFields(any(), any(),any())).thenReturn(fields);
            List<Schema> result = databaseNode.mergeSchema(inputSchemas,schemas, new DAG.Options());
            Assertions.assertEquals(1,result.size());
            Assertions.assertEquals(2,result.get(0).getFields().size());
        }

        @Test
        void test_mongoSchema_notPrimaryKey(){
            DAG dag = mock(DAG.class);
            when(dag.getSyncType()).thenReturn("logCollector");
            when(databaseNode.getDag()).thenReturn(dag);
            List<List<Schema>> inputSchemas = new ArrayList<>();
            List<Schema> schemas = new ArrayList<>();
            Schema schema = new Schema();
            schema.setName("test1");
            schema.setOriginalName("test1");
            schema.setAncestorsName("test1");
            List<Field> fields = new ArrayList<>();
            Field field = new Field();
            field.setFieldName("test1");
            fields.add(field);
            schema.setFields(fields);
            schemas.add(schema);
            inputSchemas.add(schemas);
            when(databaseNode.transformFields(any(), any(),any())).thenReturn(fields);
            List<Schema> result = databaseNode.mergeSchema(inputSchemas,schemas, new DAG.Options());
            Assertions.assertEquals(1,result.size());
            Assertions.assertEquals(2,result.get(0).getFields().size());
        }
    }
}

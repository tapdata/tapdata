package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataService;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class MigrateAddDateFieldProcessorNodeTest {
    private MigrateAddDateFieldProcessorNode migrateAddDateFieldProcessorNode=new MigrateAddDateFieldProcessorNode();

    @Test
    void loadSchemaTest(){
        List<Schema> schemaList = migrateAddDateFieldProcessorNode.loadSchema(null);
        assertEquals(null,schemaList);
    }
    public List<Field> getFields() {
        Field field = new Field();
        field.setFieldName("name");
        field.setDataType("String");
        field.setColumnSize(100);
        field.setOriPrecision(100);
        field.setSource("manual");
        List<Field> list=new ArrayList<>();
        list.add(field);
        return list;
    }
    @Test
    void getConnectIdTest(){
        MigrateAddDateFieldProcessorNode mockMigrateAddDateFieldProcessorNode = mock(MigrateAddDateFieldProcessorNode.class);
        doCallRealMethod().when(mockMigrateAddDateFieldProcessorNode).getConnectId();
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setConnectionId("123");
        List<DatabaseNode> databaseNodes=new ArrayList<>();
        databaseNodes.add(databaseNode);
        when(mockMigrateAddDateFieldProcessorNode.getSourceNode()).thenReturn(databaseNodes);
        String connectId = mockMigrateAddDateFieldProcessorNode.getConnectId();
        assertEquals("123",connectId);
    }

    @Test
    void saveSchemaTest(){
        Schema schema = new Schema();
        List<Field> fields = getFields();
        schema.setFields(fields);
        List<Schema> schemaList=new ArrayList<>();
        schemaList.add(schema);
        DAGDataService dagDataService = mock(DAGDataService.class);
        doAnswer(invocationOnMock -> {
            List<Schema> argument = (List<Schema>) invocationOnMock.getArgument(2);
            argument.forEach(schema1 -> {
                assertEquals("123",schema1.getNodeId());
            });
            return null;
        }).when(dagDataService).createOrUpdateSchema(any(),any(),any(),any(),any());
        DAG dag = mock(DAG.class);
        ReflectionTestUtils.setField(dag,"ownerId","123");
        migrateAddDateFieldProcessorNode.setDag(dag);
        migrateAddDateFieldProcessorNode.setService(dagDataService);
        migrateAddDateFieldProcessorNode.saveSchema(null,"123",schemaList,null);
    }
    @Test
    void cloneSchemaTest(){
        try(MockedStatic<SchemaUtils> schemaUtilsMockedStatic = mockStatic(SchemaUtils.class)){
            Schema schema = new Schema();
            List<Field> fields = getFields();
            schema.setFields(fields);
            List<Schema> schemaList=new ArrayList<>();
            schemaList.add(schema);
            when(SchemaUtils.cloneSchema(anyList())).thenReturn(schemaList);
            assertDoesNotThrow(()->{migrateAddDateFieldProcessorNode.cloneSchema(schemaList);});
        }
    }
    @Nested
    class MergeSchemaTest{
        private MigrateAddDateFieldProcessorNode migrateAddDateFieldProcessorNode=new MigrateAddDateFieldProcessorNode();
        @DisplayName("test mergeSchema,schema is null")
        @Test
        void mergeSchemaTest1(){
            List<Schema> schemaList = migrateAddDateFieldProcessorNode.mergeSchema(null, null, null);
            assertEquals(0,schemaList.size());
        }
        @DisplayName("test mergeSchema, dateFieldName is null")
        @Test
        void mergeSchemaTest2(){
            Schema schema = new Schema();
            List<Field> fields = getFields();
            schema.setFields(fields);
            List<Schema> schemaList=new ArrayList<>();
            schemaList.add(schema);
            List<List<Schema>> inputSchema=new ArrayList<>();
            inputSchema.add(schemaList);
            List<Schema> schemaListResult = migrateAddDateFieldProcessorNode.mergeSchema(inputSchema, null, null);
            assertEquals(1,schemaListResult.size());
        }
        @DisplayName("test mergeSchema normal")
        @Test
        void mergeSchemaTest3(){
            migrateAddDateFieldProcessorNode.setDateFieldName("createTime");
            try(MockedStatic<SchemaUtils> schemaUtilsMockedStatic = mockStatic(SchemaUtils.class)){
                Schema schema = new Schema();
                List<Field> fields = getFields();
                schema.setFields(fields);
                List<Schema> schemaList=new ArrayList<>();
                schemaList.add(schema);
                List<List<Schema>> inputSchema=new ArrayList<>();
                inputSchema.add(schemaList);
                Field dateField = new Field();
                dateField.setId("createTime");
                dateField.setDataType("Date");
                dateField.setFieldName("createTime");
                when(SchemaUtils.createField(any(),any(),any())).thenReturn(dateField);
                List<Schema> schemaList1 = migrateAddDateFieldProcessorNode.mergeSchema(inputSchema, null, null);
                List<Field> resultField = schemaList1.get(0).getFields();
                assertEquals(2,resultField.size());
                assertEquals("createTime",resultField.get(1).getId());
                assertEquals("Date",resultField.get(1).getDataType());
            }

        }

    }
}

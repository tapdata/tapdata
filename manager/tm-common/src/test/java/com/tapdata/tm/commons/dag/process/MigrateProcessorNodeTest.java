package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataService;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

public class MigrateProcessorNodeTest {
    private MigrateProcessorNode migrateProcessorNode;
    @BeforeEach
    void setUp(){
        migrateProcessorNode=new MigrateDateProcessorNode();
    }
    @Test
    void loadSchemaTest(){
        List<Schema> schemaList = migrateProcessorNode.loadSchema(null);
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
        migrateProcessorNode.setDag(dag);
        migrateProcessorNode.setService(dagDataService);
        migrateProcessorNode.saveSchema(null,"123",schemaList,null);
    }
    @DisplayName("test Clone Schema normal")
    @Test
    void cloneSchemaTest(){
        try(MockedStatic<SchemaUtils> schemaUtilsMockedStatic = mockStatic(SchemaUtils.class)){
            Schema schema = new Schema();
            List<Field> fields = getFields();
            schema.setFields(fields);
            List<Schema> schemaList=new ArrayList<>();
            schemaList.add(schema);
            when(SchemaUtils.cloneSchema(anyList())).thenReturn(schemaList);
            assertDoesNotThrow(()->{migrateProcessorNode.cloneSchema(schemaList);});
        }
    }
    @DisplayName("test Clone Schema null")
    @Test
    void cloneSchemaTest2(){
        List<Schema> schemaList = migrateProcessorNode.cloneSchema(null);
        assertEquals(0,schemaList.size());
    }
}

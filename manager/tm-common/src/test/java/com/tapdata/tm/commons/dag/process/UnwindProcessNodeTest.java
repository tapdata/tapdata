package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.ArrayModel;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.UnwindModel;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class UnwindProcessNodeTest {
    UnwindProcessNode unwindProcessNode = spy(UnwindProcessNode.class);

    @BeforeEach
    void before(){

    }
    @Test
    void mergeSchemaTest_flatten_object(){
        unwindProcessNode.setPath("t");
        unwindProcessNode.setUnwindModel(UnwindModel.FLATTEN);
        unwindProcessNode.setArrayModel(ArrayModel.OBJECT);
        unwindProcessNode.setJoiner("_");
        List<Schema> inputSchema = mock(List.class);
        Schema schema = mock(Schema.class);
        Schema outputSchema = new Schema();
        List<Field> fields = new ArrayList<>();
        Field field1 = new Field();
        field1.setFieldName("t");
        Field field2 = new Field();
        field2.setFieldName("t.id");
        fields.add(field1);
        fields.add(field2);
        outputSchema.setFields(fields);
        doReturn(outputSchema).when(unwindProcessNode).superMergeSchema(inputSchema,schema);
        Schema result = unwindProcessNode.mergeSchema(inputSchema,schema,mock(DAG.Options.class));
        Assertions.assertEquals(result.getFields().get(0).getFieldName(),"t_id");
        Assertions.assertEquals("_",unwindProcessNode.getJoiner());
        Assertions.assertEquals(UnwindModel.FLATTEN,unwindProcessNode.getUnwindModel());
        Assertions.assertEquals(ArrayModel.OBJECT,unwindProcessNode.getArrayModel());
    }

    @Test
    void mergeSchemaTest_flatten_mix(){
        unwindProcessNode.setPath("t");
        unwindProcessNode.setUnwindModel(UnwindModel.FLATTEN);
        unwindProcessNode.setArrayModel(ArrayModel.MIX);
        unwindProcessNode.setJoiner("_");
        List<Schema> inputSchema = mock(List.class);
        Schema schema = mock(Schema.class);
        Schema outputSchema = new Schema();
        List<Field> fields = new ArrayList<>();
        Field field1 = new Field();
        field1.setFieldName("t");
        Field field2 = new Field();
        field2.setFieldName("t.id");
        fields.add(field1);
        fields.add(field2);
        outputSchema.setFields(fields);
        doReturn(outputSchema).when(unwindProcessNode).superMergeSchema(inputSchema,schema);
        Schema result = unwindProcessNode.mergeSchema(inputSchema,schema,mock(DAG.Options.class));
        Assertions.assertEquals(result.getFields().size(),2);
    }

    @Test
    void mergeSchemaTest_flatten_basic(){
        unwindProcessNode.setPath("t");
        unwindProcessNode.setUnwindModel(UnwindModel.FLATTEN);
        unwindProcessNode.setArrayModel(ArrayModel.BASIC);
        unwindProcessNode.setJoiner("_");
        List<Schema> inputSchema = mock(List.class);
        Schema schema = mock(Schema.class);
        Schema outputSchema = new Schema();
        List<Field> fields = new ArrayList<>();
        Field field1 = new Field();
        field1.setFieldName("t");
        fields.add(field1);
        outputSchema.setFields(fields);
        doReturn(outputSchema).when(unwindProcessNode).superMergeSchema(inputSchema,schema);
        Schema result = unwindProcessNode.mergeSchema(inputSchema,schema,mock(DAG.Options.class));
        Assertions.assertTrue(result.getFields().get(0).getDataType().equals("String"));
    }

    @Test
    void mergeSchemaTest_embedded(){
        unwindProcessNode.setPath("t");
        unwindProcessNode.setUnwindModel(UnwindModel.FLATTEN);
        unwindProcessNode.setArrayModel(ArrayModel.BASIC);
        unwindProcessNode.setJoiner("_");
        List<Schema> inputSchema = mock(List.class);
        Schema schema = mock(Schema.class);
        Schema outputSchema = new Schema();
        List<Field> fields = new ArrayList<>();
        Field field1 = new Field();
        field1.setFieldName("t");
        Field field2 = new Field();
        field2.setFieldName("t.id");
        fields.add(field1);
        fields.add(field2);
        outputSchema.setFields(fields);
        doReturn(outputSchema).when(unwindProcessNode).superMergeSchema(inputSchema,schema);
        unwindProcessNode.mergeSchema(inputSchema,schema,mock(DAG.Options.class));
        Assertions.assertTrue(unwindProcessNode.getFlattenMap().isEmpty());
    }
}

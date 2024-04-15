package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.schema.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class FieldAddDelProcessorNodeTest {
    private FieldAddDelProcessorNode fieldAddDelProcessorNode = mock(FieldAddDelProcessorNode.class);
    @Nested
    class TestMergeSchema{
        private List<FieldProcessorNode.Operation> operations;
        private List<Schema> inputSchemas;
        private Schema schema;
        private DAG.Options options;
        private Schema outputSchema;
        private List<Field> fields;
        @BeforeEach
        void beforeEach(){
            inputSchemas = new ArrayList<>();
            inputSchemas.add(mock(Schema.class));
            schema = mock(Schema.class);
            options = mock(DAG.Options.class);
            outputSchema = new Schema();
            fields = new ArrayList<>();
            doCallRealMethod().when(fieldAddDelProcessorNode).mergeSchema(inputSchemas,schema,options);
        }
        @Test
        @DisplayName("test mergeSchema method when deleteAllFields is true and operations is null")
        void testMergeSchema1(){
            ReflectionTestUtils.setField(fieldAddDelProcessorNode,"deleteAllFields",true);
            operations = new ArrayList<>();
            ReflectionTestUtils.setField(fieldAddDelProcessorNode,"operations",operations);
            Field field = new Field();
            fields.add(field);
            outputSchema.setFields(fields);
            when(fieldAddDelProcessorNode.superMergeSchema(inputSchemas,schema)).thenReturn(outputSchema);
            fieldAddDelProcessorNode.mergeSchema(inputSchemas,schema,options);
            verify(fieldAddDelProcessorNode, new Times(1)).deleteIndicesIfNeed(outputSchema);
            assertEquals(true, outputSchema.getFields().get(0).isDeleted());
        }
        @Test
        @DisplayName("test mergeSchema method when deleteAllFields is true and operations is not null")
        void testMergeSchema2(){
            ReflectionTestUtils.setField(fieldAddDelProcessorNode,"deleteAllFields",true);
            operations = new ArrayList<>();
            FieldProcessorNode.Operation operation = mock(FieldProcessorNode.Operation.class);
            operations.add(operation);
            when(operation.getOp()).thenReturn("REMOVE");
            when(operation.getOperand()).thenReturn("false");
            when(operation.getField()).thenReturn("rollbackField");
            ReflectionTestUtils.setField(fieldAddDelProcessorNode,"operations",operations);
            Field field1 = new Field();
            field1.setFieldName("rollbackField");
            Field field2 = new Field();
            field2.setFieldName("field");
            fields.add(field1);
            fields.add(field2);
            outputSchema.setFields(fields);
            when(fieldAddDelProcessorNode.superMergeSchema(inputSchemas,schema)).thenReturn(outputSchema);
            fieldAddDelProcessorNode.mergeSchema(inputSchemas,schema,options);
            verify(fieldAddDelProcessorNode, new Times(1)).deleteIndicesIfNeed(outputSchema);
            assertEquals(false, outputSchema.getFields().get(0).isDeleted());
            assertEquals(true, outputSchema.getFields().get(1).isDeleted());
        }
        @Test
        @DisplayName("test mergeSchema method when deleteAllFields is false and operation is create")
        void testMergeSchema3(){
            Field createField = new Field();
            createField.setDataType("Integer");
            try (MockedStatic<SchemaUtils> mb = Mockito
                    .mockStatic(SchemaUtils.class)) {
                mb.when(()->SchemaUtils.createField(anyString(),anyString(),any(FieldProcessorNode.Operation.class))).thenReturn(createField);
                //注意：调用待测试方法的时候一定要在try里面写
                when(fieldAddDelProcessorNode.getId()).thenReturn("111");
                ReflectionTestUtils.setField(fieldAddDelProcessorNode,"deleteAllFields",false);
                operations = new ArrayList<>();
                FieldProcessorNode.Operation operation = mock(FieldProcessorNode.Operation.class);
                operations.add(operation);
                when(operation.getId()).thenReturn("222");
                when(operation.getOp()).thenReturn("CREATE");
                when(operation.getOperand()).thenReturn("true");
                when(operation.getField()).thenReturn("testField");
                ReflectionTestUtils.setField(fieldAddDelProcessorNode,"operations",operations);
                Field field = new Field();
                field.setFieldName("testField");
                fields.add(field);
                outputSchema.setFields(fields);
                outputSchema.setOriginalName("originName");
                when(fieldAddDelProcessorNode.superMergeSchema(inputSchemas,schema)).thenReturn(outputSchema);
                fieldAddDelProcessorNode.mergeSchema(inputSchemas,schema,options);
                assertEquals(2, outputSchema.getFields().size());
            }

        }
        @Test
        @DisplayName("test mergeSchema method when deleteAllFields is false and operation is remove")
        void testMergeSchema4(){
            ReflectionTestUtils.setField(fieldAddDelProcessorNode,"deleteAllFields",false);
            operations = new ArrayList<>();
            FieldProcessorNode.Operation operation = mock(FieldProcessorNode.Operation.class);
            operations.add(operation);
            when(operation.getId()).thenReturn("111");
            when(operation.getOp()).thenReturn("REMOVE");
            when(operation.getOperand()).thenReturn("true");
            when(operation.getField()).thenReturn("testField");
            ReflectionTestUtils.setField(fieldAddDelProcessorNode,"operations",operations);
            Field field = new Field();
            field.setFieldName("testField");
            fields.add(field);
            outputSchema.setFields(fields);
            when(fieldAddDelProcessorNode.superMergeSchema(inputSchemas,schema)).thenReturn(outputSchema);
            fieldAddDelProcessorNode.mergeSchema(inputSchemas,schema,options);
            assertEquals(true, outputSchema.getFields().get(0).isDeleted());
        }
        @Test
        @DisplayName("test mergeSchema method when fieldsAfter is not empty")
        void testMergeSchema5(){
            ReflectionTestUtils.setField(fieldAddDelProcessorNode,"deleteAllFields",false);
            operations = new ArrayList<>();
            ReflectionTestUtils.setField(fieldAddDelProcessorNode,"operations",operations);
            List<FieldAddDelProcessorNode.Postion> fieldsAfter = new ArrayList<>();
            FieldAddDelProcessorNode.Postion postion = new FieldAddDelProcessorNode.Postion();
            postion.setField_name("col1");
            postion.setColumnPosition(1);
            FieldAddDelProcessorNode.Postion postion1 = new FieldAddDelProcessorNode.Postion();
            postion1.setField_name("col2");
            postion1.setColumnPosition(2);
            fieldsAfter.add(postion);
            fieldsAfter.add(postion1);
            ReflectionTestUtils.setField(fieldAddDelProcessorNode,"fieldsAfter",fieldsAfter);
            Field field = new Field();
            field.setFieldName("col1");
            fields.add(field);
            outputSchema.setFields(fields);
            when(fieldAddDelProcessorNode.superMergeSchema(inputSchemas,schema)).thenReturn(outputSchema);
            fieldAddDelProcessorNode.mergeSchema(inputSchemas,schema,options);
            assertEquals(1, outputSchema.getFields().get(0).getColumnPosition());
        }
    }

    @Nested
    class TestDeleteIndicesIfNeed{
        private Schema outputSchema;
        @Test
        void testDeleteIndicesIfNeed(){
            outputSchema = new Schema();
            List<Field> deleteFields = new ArrayList<>();
            Field field = mock(Field.class);
            when(field.getFieldName()).thenReturn("testField");
            when(field.isDeleted()).thenReturn(true);
            deleteFields.add(field);
            outputSchema.setFields(deleteFields);
            List<TableIndex> indices = new ArrayList<>();
            TableIndex index = mock(TableIndex.class);
            List<TableIndexColumn> columns = new ArrayList<>();
            TableIndexColumn column = mock(TableIndexColumn.class);
            when(column.getColumnName()).thenReturn("testField");
            columns.add(column);
            when(index.getColumns()).thenReturn(columns);
            indices.add(index);
            outputSchema.setIndices(indices);
            doCallRealMethod().when(fieldAddDelProcessorNode).deleteIndicesIfNeed(outputSchema);
            fieldAddDelProcessorNode.deleteIndicesIfNeed(outputSchema);
            assertEquals(0,outputSchema.getIndices().size());
        }
    }
}

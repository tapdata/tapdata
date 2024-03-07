package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AddDateFieldProcessorNodeTest {

    @Nested
    class mergetSchemaTest{

        private AddDateFieldProcessorNode addDateFieldProcessorNode;
        @BeforeEach
        void setUp(){
            addDateFieldProcessorNode= new AddDateFieldProcessorNode();

        }
        @DisplayName("test get schema normal")
        @Test
        void test1(){
            try(MockedStatic<SchemaUtils> schemaUtilsMockedStatic = mockStatic(SchemaUtils.class)){
                addDateFieldProcessorNode.setDateFieldName("timeCreate");
                Schema schema = new Schema();

                Field field = new Field();
                field.setFieldName("name");
                field.setDataType("String");
                field.setColumnSize(100);
                field.setOriPrecision(100);
                field.setSource("manual");
                List<Field> list=new ArrayList<>();
                list.add(field);
                schema.setFields(list);
                List<Schema> schemaList=new ArrayList<>();
                schemaList.add(schema);
                when(SchemaUtils.mergeSchema(schemaList,schema)).thenReturn(schema);
                Field dateField = new Field();
                dateField.setId("createTime");
                dateField.setDataType("Date");
                dateField.setFieldName("createTime");
                when(SchemaUtils.createField(any(),any(),any())).thenReturn(dateField);

                Schema resultSchema = addDateFieldProcessorNode.mergeSchema(Arrays.asList(schema), schema, null);
                assertEquals(2,resultSchema.getFields().size());
                Field resultField = resultSchema.getFields().get(1);
                assertEquals("createTime",resultField.getId());
                assertEquals("Date",resultField.getDataType());
            }
        }
        @DisplayName("test get Schema when dateFieldName is null")
        @Test
        void test2(){
            try(MockedStatic<SchemaUtils> schemaUtilsMockedStatic = mockStatic(SchemaUtils.class)){
                Schema schema = new Schema();
                Field field = new Field();
                field.setFieldName("name");
                field.setDataType("String");
                field.setColumnSize(100);
                field.setOriPrecision(100);
                field.setSource("manual");
                List<Field> list=new ArrayList<>();
                list.add(field);
                schema.setFields(list);
                List<Schema> schemaList=new ArrayList<>();
                schemaList.add(schema);
                when(SchemaUtils.mergeSchema(schemaList,schema)).thenReturn(schema);
                Schema resultSchema = addDateFieldProcessorNode.mergeSchema(Arrays.asList(schema), schema, null);
                assertEquals(1,resultSchema.getFields().size());
            }
        }
    }
}

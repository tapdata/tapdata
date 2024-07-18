package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class MigrateUnionProcessorNodeTest {
    @Nested
    class MergeSchemaTest{
        @Test
        void test_main(){
            MigrateUnionProcessorNode migrateUnionProcessorNode = new MigrateUnionProcessorNode();
            List<List<Schema>> inputSchemas = new ArrayList<>();
            List<Schema> schemas = new ArrayList<>();
            Schema schema1 = new Schema();
            schema1.setName("schema1");
            schema1.setAncestorsName("schema1");
            schema1.setOriginalName("schema1");
            List<Field> fields = new ArrayList<>();
            Field field1 = new Field();
            field1.setId("1");
            field1.setFieldName("name_1");
            fields.add(field1);
            schema1.setFields(fields);
            Schema schema2 = new Schema();
            schema2.setName("schema1");
            schema2.setAncestorsName("schema1");
            schema2.setOriginalName("schema1");
            List<Field> field2s = new ArrayList<>();
            Field field2 = new Field();
            field2.setId("2");
            field2.setFieldName("name_2");
            field2s.add(field2);
            schema1.setFields(fields);
            schema2.setFields(field2s);
            schemas.add(schema1);
            schemas.add(schema2);
            inputSchemas.add(schemas);
            List<Schema> result = migrateUnionProcessorNode.mergeSchema(inputSchemas,null,null);
            Assertions.assertEquals(2,result.get(0).getFields().size());
            Assertions.assertTrue(result.get(0).getFields().contains(field1));
            Assertions.assertTrue(result.get(0).getFields().contains(field2));
        }

        @Test
        void test_inputSchemaIsNull(){
            MigrateUnionProcessorNode migrateUnionProcessorNode = new MigrateUnionProcessorNode();
            List<Schema> result = migrateUnionProcessorNode.mergeSchema(null,null,null);
            Assertions.assertEquals(0,result.size());
        }

    }
}

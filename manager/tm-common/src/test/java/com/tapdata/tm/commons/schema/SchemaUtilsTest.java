package com.tapdata.tm.commons.schema;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SchemaUtilsTest {
    @Nested
    class RemoveSubFieldsWhichFromFreeSchemaTest {
        DataSourceConnectionDto dataSource;
        List<Schema> schemas;
        Schema schema;
        List<String> definitionTags;
        List<Field> fields;
        Field field;
        @BeforeEach
        void init() {
            schema = mock(Schema.class);
            field = mock(Field.class);
            dataSource = new DataSourceConnectionDto();
            definitionTags = new ArrayList<>();
            schemas = new ArrayList<>();
            fields = new ArrayList<>();
            schemas.add(schema);
            fields.add(field);
            when(schema.getFields()).thenReturn(fields);
            when(field.getFieldName()).thenReturn("name");
            dataSource.setDefinitionTags(definitionTags);
        }

        @DisplayName("The test target is not a schema free type database")
        @Test
        void testOneSchema() {
            Schema s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schema);
            Assertions.assertNotNull(s);
            Assertions.assertEquals(schema, s);
        }

        @DisplayName("The test target is not a schema free type database")
        @Test
        void testAllSchema() {
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            Assertions.assertNotNull(s);
            Assertions.assertEquals(1, s.size());
            Assertions.assertEquals(schema, s.get(0));
        }

        @DisplayName("The test target is a schema free type database")
        @Test
        void testDataBaseIsFreeSchemaOne() {
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            Schema s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schema);
            Assertions.assertNotNull(s);
            Assertions.assertEquals(schema, s);
        }

        @DisplayName("The test target is a schema free type database")
        @Test
        void testDataBaseIsFreeSchemaALl() {
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            Assertions.assertNotNull(s);
            Assertions.assertEquals(1, s.size());
            Assertions.assertEquals(schema, s.get(0));
        }

        @DisplayName("The test target is not a schema-free type database")
        @Test
        void testDataBaseNotFreeSchemaOne() {
            definitionTags.add("database");
            Schema s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schema);
            Assertions.assertNotNull(s);
            Assertions.assertEquals(schema, s);
        }

        @DisplayName("The test target is not a schema free type database")
        @Test
        void testDataBaseNotFreeSchemaALl() {
            definitionTags.add("database");
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            Assertions.assertNotNull(s);
            Assertions.assertEquals(1, s.size());
            Assertions.assertEquals(schema, s.get(0));
        }

        @DisplayName("The test schema does not contain any fields")
        @Test
        void testSchemaNotAnyFields() {
            fields.clear();
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            Assertions.assertNotNull(s);
            Assertions.assertEquals(1, s.size());
            Assertions.assertEquals(schema, s.get(0));
        }

        @DisplayName("The test schema does not contain any sub attribute fields")
        @Test
        void testContainsSubFields() {
            Field fieldSub = mock(Field.class);
            fields.add(fieldSub);
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            when(fieldSub.getFieldName()).thenReturn("name.id");
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            Assertions.assertNotNull(s);
            Assertions.assertEquals(1, s.size());
            Assertions.assertEquals(schema, s.get(0));
        }

        @DisplayName("Test attribute names with dots that do not contain parent attributes")
        @Test
        void testContainsSubFieldsButNotFatherField() {
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            when(field.getFieldName()).thenReturn("name.id");
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            Assertions.assertNotNull(s);
            Assertions.assertEquals(1, s.size());
            Assertions.assertEquals(schema, s.get(0));
        }

        @DisplayName("The test parent property is TapMap")
        @Test
        void testContainsSubFieldsButNotFatherFieldAndFieldIsTAP_MAP() {
            Field fieldSub = mock(Field.class);
            when(field.getTapType()).thenReturn(SchemaUtils.TAP_MAP);
            fields.add(fieldSub);
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            when(fieldSub.getFieldName()).thenReturn("name.id");
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            Assertions.assertNotNull(s);
            Assertions.assertEquals(1, s.size());
            Assertions.assertEquals(schema, s.get(0));
        }

        @DisplayName("The test parent property is TapArray")
        @Test
        void testContainsSubFieldsButNotFatherFieldAndFieldIsTAP_ARRAY() {
            Field fieldSub = mock(Field.class);
            when(field.getTapType()).thenReturn(SchemaUtils.TAP_ARRAY);
            fields.add(fieldSub);
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            when(fieldSub.getFieldName()).thenReturn("name.id");
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            Assertions.assertNotNull(s);
            Assertions.assertEquals(1, s.size());
            Assertions.assertEquals(schema, s.get(0));
        }
    }
}
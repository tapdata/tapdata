package com.tapdata.tm.commons.schema;


import com.tapdata.tm.commons.dag.process.FieldProcessorNode;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
            assertNotNull(s);
            assertEquals(schema, s);
        }

        @DisplayName("The test target is not a schema free type database")
        @Test
        void testAllSchema() {
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            assertNotNull(s);
            assertEquals(1, s.size());
            assertEquals(schema, s.get(0));
        }

        @DisplayName("The test target is a schema free type database")
        @Test
        void testDataBaseIsFreeSchemaOne() {
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            Schema s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schema);
            assertNotNull(s);
            assertEquals(schema, s);
        }

        @DisplayName("The test target is a schema free type database")
        @Test
        void testDataBaseIsFreeSchemaALl() {
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            assertNotNull(s);
            assertEquals(1, s.size());
            assertEquals(schema, s.get(0));
        }

        @DisplayName("The test target is not a schema-free type database")
        @Test
        void testDataBaseNotFreeSchemaOne() {
            definitionTags.add("database");
            Schema s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schema);
            assertNotNull(s);
            assertEquals(schema, s);
        }

        @DisplayName("The test target is not a schema free type database")
        @Test
        void testDataBaseNotFreeSchemaALl() {
            definitionTags.add("database");
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            assertNotNull(s);
            assertEquals(1, s.size());
            assertEquals(schema, s.get(0));
        }

        @DisplayName("The test schema does not contain any fields")
        @Test
        void testSchemaNotAnyFields() {
            fields.clear();
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            assertNotNull(s);
            assertEquals(1, s.size());
            assertEquals(schema, s.get(0));
        }

        @DisplayName("The test schema does not contain any sub attribute fields")
        @Test
        void testContainsSubFields() {
            Field fieldSub = mock(Field.class);
            fields.add(fieldSub);
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            when(fieldSub.getFieldName()).thenReturn("name.id");
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            assertNotNull(s);
            assertEquals(1, s.size());
            assertEquals(schema, s.get(0));
        }

        @DisplayName("Test attribute names with dots that do not contain parent attributes")
        @Test
        void testContainsSubFieldsButNotFatherField() {
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            when(field.getFieldName()).thenReturn("name.id");
            List<Schema> s = SchemaUtils.removeSubFieldsWhichFromFreeSchema(dataSource, schemas);
            assertNotNull(s);
            assertEquals(1, s.size());
            assertEquals(schema, s.get(0));
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
            assertNotNull(s);
            assertEquals(1, s.size());
            assertEquals(schema, s.get(0));
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
            assertNotNull(s);
            assertEquals(1, s.size());
            assertEquals(schema, s.get(0));
        }
    }

    @Nested
    class CloneSchemaInfoTest {
        @Test
        void testNormal() {
            Schema inputSchema = mock(Schema.class);
            Schema targetSchema = mock(Schema.class);
            when(inputSchema.getPartitionInfo()).thenReturn(null);
            when(inputSchema.getPartitionMasterTableId()).thenReturn(null);
            doNothing().when(targetSchema).setPartitionMasterTableId(null);
            doNothing().when(targetSchema).setPartitionInfo(null);
            SchemaUtils.cloneSchemaInfo(inputSchema, targetSchema);
            verify(inputSchema, times(1)).getPartitionInfo();
            verify(inputSchema, times(1)).getPartitionMasterTableId();
        }
    }

    @Test
    public void testRemoveDeleteFields() {
        AtomicInteger fieldCounter = new AtomicInteger();

        List<Schema> schemas = Stream.generate(() -> {
            Schema schema = new Schema();
            List<Field> fields = Stream.generate(() -> {
                Field field = new Field();
                field.setFieldName("test_1");
                field.setDeleted(RandomUtils.nextBoolean());
                if (!field.isDeleted()) {
                    fieldCounter.incrementAndGet();
                }
                return field;
            }).limit(10).collect(Collectors.toList());
            schema.setFields(fields);
            return schema;
        }).limit(5).collect(Collectors.toList());

        SchemaUtils.removeDeleteFields(schemas);

        assertEquals(fieldCounter.get(), schemas.stream().map(s -> s.getFields().size()).reduce(Integer::sum).orElse(0));

    }

    @Test
    public void testMergeSchema() {
        Assertions.assertNull(SchemaUtils.mergeSchema(Collections.emptyList(), null, true));
        AtomicInteger counter = new AtomicInteger();
        List<Schema> inputSchemas = Stream.generate(() -> {
            Schema schema = new Schema();
            List<Field> fields = Stream.generate(() -> {
                Field field = new Field();
                field.setFieldName("test_" + counter.incrementAndGet());

                return field;
            }).limit(10).collect(Collectors.toList());
            schema.setFields(fields);
            schema.setName("test");
            return schema;
        }).limit(3).collect(Collectors.toList());
        Schema schema = SchemaUtils.mergeSchema(inputSchemas, null, false);
        assertNotNull(schema);
        assertEquals(counter.get(), schema.getFields().size());

        Schema targetSchema = inputSchemas.remove(2);
        schema = SchemaUtils.mergeSchema(inputSchemas, targetSchema, false);
        assertNotNull(schema);
        assertEquals(counter.get(), schema.getFields().size());
    }

    @Nested
    class createFieldTest {
        String nodeId;
        String tableName;
        FieldProcessorNode.Operation operation;
        @BeforeEach
        void beforeEach() {
            nodeId = "test";
            tableName = "table1";
            operation = new FieldProcessorNode.Operation();
            operation.setField(nodeId);
        }
        @Test
        void testCreateFieldNormal() {
            Field field = SchemaUtils.createField(nodeId, tableName, operation);
            assertNotNull(field);
            assertEquals("test", field.getFieldName());
            assertTrue((Boolean) field.getIsNullable());
        }
    }
}
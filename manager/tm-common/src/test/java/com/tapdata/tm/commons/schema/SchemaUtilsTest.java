package com.tapdata.tm.commons.schema;


import com.tapdata.tm.commons.dag.process.FieldProcessorNode;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
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
            Schema s = SchemaUtils.removeSubFieldsForFreeSchemaDatasource(dataSource, schema);
            assertNotNull(s);
            assertEquals(schema, s);
        }

        @DisplayName("The test target is not a schema free type database")
        @Test
        void testAllSchema() {
            List<Schema> s = SchemaUtils.removeSubFieldsForFreeSchemaDatasource(dataSource, schemas);
            assertNotNull(s);
            assertEquals(1, s.size());
            assertEquals(schema, s.get(0));
        }

        @DisplayName("The test target is a schema free type database")
        @Test
        void testDataBaseIsFreeSchemaOne() {
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            Schema s = SchemaUtils.removeSubFieldsForFreeSchemaDatasource(dataSource, schema);
            assertNotNull(s);
            assertEquals(schema, s);
        }

        @DisplayName("The test target is a schema free type database")
        @Test
        void testDataBaseIsFreeSchemaALl() {
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            List<Schema> s = SchemaUtils.removeSubFieldsForFreeSchemaDatasource(dataSource, schemas);
            assertNotNull(s);
            assertEquals(1, s.size());
            assertEquals(schema, s.get(0));
        }

        @DisplayName("The test target is not a schema-free type database")
        @Test
        void testDataBaseNotFreeSchemaOne() {
            definitionTags.add("database");
            Schema s = SchemaUtils.removeSubFieldsForFreeSchemaDatasource(dataSource, schema);
            assertNotNull(s);
            assertEquals(schema, s);
        }

        @DisplayName("The test target is not a schema free type database")
        @Test
        void testDataBaseNotFreeSchemaALl() {
            definitionTags.add("database");
            List<Schema> s = SchemaUtils.removeSubFieldsForFreeSchemaDatasource(dataSource, schemas);
            assertNotNull(s);
            assertEquals(1, s.size());
            assertEquals(schema, s.get(0));
        }

        @DisplayName("The test schema does not contain any fields")
        @Test
        void testSchemaNotAnyFields() {
            fields.clear();
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            List<Schema> s = SchemaUtils.removeSubFieldsForFreeSchemaDatasource(dataSource, schemas);
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
            List<Schema> s = SchemaUtils.removeSubFieldsForFreeSchemaDatasource(dataSource, schemas);
            assertNotNull(s);
            assertEquals(1, s.size());
            assertEquals(schema, s.get(0));
        }

        @DisplayName("Test attribute names with dots that do not contain parent attributes")
        @Test
        void testContainsSubFieldsButNotFatherField() {
            definitionTags.add(SchemaUtils.SCHEMA_FREE);
            when(field.getFieldName()).thenReturn("name.id");
            List<Schema> s = SchemaUtils.removeSubFieldsForFreeSchemaDatasource(dataSource, schemas);
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
            List<Schema> s = SchemaUtils.removeSubFieldsForFreeSchemaDatasource(dataSource, schemas);
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
            List<Schema> s = SchemaUtils.removeSubFieldsForFreeSchemaDatasource(dataSource, schemas);
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

    @Nested
    @DisplayName("CompareSchema Tests")
    class CompareSchemaTest {
        private MetadataInstancesDto sourceMetadata;
        private MetadataInstancesDto targetMetadata;
        private List<Field> sourceFields;
        private List<Field> targetFields;

        @BeforeEach
        void setUp() {
            sourceMetadata = new MetadataInstancesDto();
            targetMetadata = new MetadataInstancesDto();
            sourceFields = new ArrayList<>();
            targetFields = new ArrayList<>();
            sourceMetadata.setFields(sourceFields);
            targetMetadata.setFields(targetFields);
        }

        @Test
        @DisplayName("Should return empty list when both metadata instances are null")
        void testCompareSchema_BothNull() {
            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(null, null);

            // Then
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when source metadata is null")
        void testCompareSchema_SourceNull() {
            // Given
            targetFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(null, targetMetadata);

            // Then
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when target metadata is null")
        void testCompareSchema_TargetNull() {
            // Given
            sourceFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(sourceMetadata, null);

            // Then
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return empty list when both schemas are identical")
        void testCompareSchema_IdenticalSchemas() {
            // Given
            sourceFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field2", "int", "{\"bytes\":8,\"type\":9}"));

            targetFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));
            targetFields.add(createField("field2", "int", "{\"bytes\":8,\"type\":9}"));

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(sourceMetadata, targetMetadata);

            // Then
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should detect missing fields in target")
        void testCompareSchema_MissingFields() {
            // Given
            sourceFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field2", "int", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field3", "datetime", "{\"bytes\":8,\"type\":9}"));

            targetFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));
            // field2 and field3 are missing in target

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(sourceMetadata, targetMetadata);

            // Then
            assertNotNull(result);
            assertEquals(2, result.size());

            DifferenceField missingField1 = result.stream()
                .filter(df -> "field2".equals(df.getColumnName()))
                .findFirst().orElse(null);
            assertNotNull(missingField1);
            assertEquals(DifferenceTypeEnum.Missing, missingField1.getType());
            assertEquals("field2", missingField1.getColumnName());
            assertEquals("int", missingField1.getSourceField().getDataType());

            DifferenceField missingField2 = result.stream()
                .filter(df -> "field3".equals(df.getColumnName()))
                .findFirst().orElse(null);
            assertNotNull(missingField2);
            assertEquals(DifferenceTypeEnum.Missing, missingField2.getType());
            assertEquals("field3", missingField2.getColumnName());
            assertEquals("datetime", missingField2.getSourceField().getDataType());
        }

        @Test
        @DisplayName("Should detect additional fields in target")
        void testCompareSchema_AdditionalFields() {
            // Given
            sourceFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));

            targetFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));
            targetFields.add(createField("field2", "int", "{\"bytes\":8,\"type\":9}"));
            targetFields.add(createField("field3", "datetime", "{\"bytes\":8,\"type\":9}"));

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(sourceMetadata, targetMetadata);

            // Then
            assertNotNull(result);
            assertEquals(2, result.size());

            DifferenceField additionalField1 = result.stream()
                .filter(df -> "field2".equals(df.getColumnName()))
                .findFirst().orElse(null);
            assertNotNull(additionalField1);
            assertEquals(DifferenceTypeEnum.Additional, additionalField1.getType());
            assertEquals("field2", additionalField1.getColumnName());
            assertEquals("int", additionalField1.getTargetField().getDataType());

            DifferenceField additionalField2 = result.stream()
                .filter(df -> "field3".equals(df.getColumnName()))
                .findFirst().orElse(null);
            assertNotNull(additionalField2);
            assertEquals(DifferenceTypeEnum.Additional, additionalField2.getType());
            assertEquals("field3", additionalField2.getColumnName());
            assertEquals("datetime", additionalField2.getTargetField().getDataType());
        }

        @Test
        @DisplayName("Should detect precision data types")
        void testCompareSchema_PrecisionDataTypes() {
            // Given
            sourceFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field2", "int", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field3", "datetime", "{\"bytes\":8,\"type\":9}"));

            targetFields.add(createField("field1", "text", "{\"bytes\":8,\"type\":9}"));
            targetFields.add(createField("field2", "bigint", "{\"bytes\":8,\"type\":9}"));
            targetFields.add(createField("field3", "timestamp", "{\"bytes\":8,\"type\":9}"));

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(sourceMetadata, targetMetadata);

            // Then
            assertNotNull(result);
            assertEquals(3, result.size());

            for (DifferenceField diff : result) {
                assertEquals(DifferenceTypeEnum.Precision, diff.getType());
                assertNotNull(diff.getSourceField());
                assertNotNull(diff.getTargetField());
                assertEquals(diff.getColumnName(), diff.getSourceField().getFieldName());
                assertEquals(diff.getColumnName(), diff.getTargetField().getFieldName());
            }
        }

        @Test
        @DisplayName("Should detect cannotWrite fields")
        void testCompareSchema_CannotWriteFields() {
            // Given
            sourceFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field2", "int", "{\"bytes\":8,\"type\":9}"));

            targetFields.add(createField("field1", "text", "{\"bytes\":8,\"cannotWrite\":true,\"type\":9}"));
            targetFields.add(createField("field2", "bigint", "{\"bytes\":8,\"cannotWrite\":true,\"type\":9}"));

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(sourceMetadata, targetMetadata);

            // Then
            assertNotNull(result);
            assertEquals(2, result.size());

            for (DifferenceField diff : result) {
                assertEquals(DifferenceTypeEnum.CannotWrite, diff.getType());
                assertNotNull(diff.getSourceField());
            }
        }

        @Test
        @DisplayName("Should handle mixed differences - missing, additional, different, and cannotWrite")
        void testCompareSchema_MixedDifferences() {
            // Given
            sourceFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field2", "int", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field3", "datetime", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field4", "boolean", "{\"bytes\":8,\"type\":9}"));

            targetFields.add(createField("field1", "text", "{\"bytes\":8,\"type\":9}"));                    // Different
            targetFields.add(createField("field2", "bigint", "{\"bytes\":8,\"cannotWrite\":true,\"type\":9}"));     // CannotWrite
            targetFields.add(createField("field5", "decimal", "{\"bytes\":8,\"type\":9}"));                // Additional

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(sourceMetadata, targetMetadata);

            // Then
            assertNotNull(result);
            assertEquals(5, result.size()); // field3 missing, field4 missing, field5 additional, field1 different, field2 cannotWrite

            // Check missing field
            DifferenceField missingField = result.stream()
                .filter(df -> "field3".equals(df.getColumnName()) && df.getType() == DifferenceTypeEnum.Missing)
                .findFirst().orElse(null);
            assertNotNull(missingField);

            // Check additional field
            DifferenceField additionalField = result.stream()
                .filter(df -> "field5".equals(df.getColumnName()) && df.getType() == DifferenceTypeEnum.Additional)
                .findFirst().orElse(null);
            assertNotNull(additionalField);

            // Check different field
            DifferenceField differentField = result.stream()
                .filter(df -> "field1".equals(df.getColumnName()) && df.getType() == DifferenceTypeEnum.Precision)
                .findFirst().orElse(null);
            assertNotNull(differentField);

            // Check cannotWrite field
            DifferenceField cannotWriteField = result.stream()
                .filter(df -> "field2".equals(df.getColumnName()) && df.getType() == DifferenceTypeEnum.CannotWrite)
                .findFirst().orElse(null);
            assertNotNull(cannotWriteField);

            // Check missing field4
            DifferenceField missingField4 = result.stream()
                .filter(df -> "field4".equals(df.getColumnName()) && df.getType() == DifferenceTypeEnum.Missing)
                .findFirst().orElse(null);
            assertNotNull(missingField4);
        }

        @Test
        @DisplayName("Should handle empty source fields")
        void testCompareSchema_EmptySourceFields() {
            // Given
            // sourceFields is empty
            targetFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));
            targetFields.add(createField("field2", "int", "{\"bytes\":8,\"type\":9}"));

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(sourceMetadata, targetMetadata);

            // Then
            assertNotNull(result);
            assertEquals(2, result.size());

            for (DifferenceField diff : result) {
                assertEquals(DifferenceTypeEnum.Additional, diff.getType());
                assertNotNull(diff.getTargetField());
                assertNull(diff.getSourceField());
            }
        }

        @Test
        @DisplayName("Should handle empty target fields")
        void testCompareSchema_EmptyTargetFields() {
            // Given
            sourceFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field2", "int", "{\"bytes\":8,\"type\":9}"));
            // targetFields is empty

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(sourceMetadata, targetMetadata);

            // Then
            assertNotNull(result);
            assertEquals(2, result.size());

            for (DifferenceField diff : result) {
                assertEquals(DifferenceTypeEnum.Missing, diff.getType());
                assertNotNull(diff.getSourceField());
                assertNull(diff.getTargetField());
            }
        }

        @Test
        @DisplayName("Should handle both empty fields")
        void testCompareSchema_BothEmptyFields() {
            // Given
            // Both sourceFields and targetFields are empty

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(sourceMetadata, targetMetadata);

            // Then
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should handle case insensitive data type comparison")
        void testCompareSchema_CaseInsensitiveDataTypes() {
            // Given
            sourceFields.add(createField("field1", "VARCHAR", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field2", "INT", "{\"bytes\":8,\"type\":9}"));

            targetFields.add(createField("field1", "varchar", "{\"bytes\":8,\"type\":9}"));
            targetFields.add(createField("field2", "int", "{\"bytes\":8,\"type\":9}"));

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(sourceMetadata, targetMetadata);

            // Then
            assertNotNull(result);
            assertTrue(result.isEmpty(), "Case insensitive comparison should find no differences");
        }

        @Test
        @DisplayName("Should handle fields with null data types")
        void testCompareSchema_NullDataTypes() {
            // Given
            Field sourceField = createField("field1", null, "{\"bytes\":8,\"type\":9}");
            Field targetField = createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}");

            sourceFields.add(sourceField);
            targetFields.add(targetField);

            // When & Then - Should throw NullPointerException due to null dataType
            assertThrows(NullPointerException.class, () -> {
                SchemaUtils.compareSchema(sourceMetadata, targetMetadata);
            }, "Should throw NullPointerException when dataType is null");
        }

        @Test
        @DisplayName("Should handle duplicate field names in source")
        void testCompareSchema_DuplicateSourceFields() {
            // Given
            sourceFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field1", "text", "{\"bytes\":8,\"type\":9}")); // Duplicate name, different type

            targetFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));

            // When & Then - Should throw IllegalStateException due to duplicate keys
            assertThrows(IllegalStateException.class, () -> {
                SchemaUtils.compareSchema(sourceMetadata, targetMetadata);
            }, "Should throw IllegalStateException when duplicate field names exist in source");
        }

        @Test
        @DisplayName("Should handle complex field names with special characters")
        void testCompareSchema_SpecialCharacterFieldNames() {
            // Given
            sourceFields.add(createField("field_with_underscore", "varchar(255)", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field-with-dash", "int", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field.with.dots", "datetime", "{\"bytes\":8,\"type\":9}"));
            sourceFields.add(createField("field with spaces", "boolean", "{\"bytes\":8,\"type\":9}"));

            targetFields.add(createField("field_with_underscore", "text", "{\"bytes\":8,\"type\":9}"));
            targetFields.add(createField("field-with-dash", "bigint", "{\"bytes\":8,\"type\":9}"));
            targetFields.add(createField("field.with.dots", "timestamp", "{\"bytes\":8,\"type\":9}"));
            targetFields.add(createField("field with spaces", "bit", "{\"bytes\":8,\"type\":9}"));

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(sourceMetadata, targetMetadata);

            // Then
            assertNotNull(result);
            assertEquals(4, result.size());

            for (DifferenceField diff : result) {
                assertEquals(DifferenceTypeEnum.Precision, diff.getType());
                assertTrue(diff.getColumnName().contains("field"));
            }
        }

        @Test
        @DisplayName("Should prioritize cannotWrite over different when tapType contains cannotWrite")
        void testCompareSchema_CannotWritePriority() {
            // Given
            sourceFields.add(createField("field1", "varchar(255)", "{\"bytes\":8,\"type\":9}"));

            Field targetField = createField("field1", "text", "{\"bytes\":8,\"cannotWrite\":true,\"type\":9}");
            targetFields.add(targetField);

            // When
            List<DifferenceField> result = SchemaUtils.compareSchema(sourceMetadata, targetMetadata);

            // Then
            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals(DifferenceTypeEnum.CannotWrite, result.get(0).getType());
            assertEquals("field1", result.get(0).getColumnName());
        }

        /**
         * Helper method to create a Field with specified properties
         */
        private Field createField(String fieldName, String dataType, String tapType) {
            Field field = new Field();
            field.setFieldName(fieldName);
            field.setDataType(dataType);
            field.setTapType(tapType);
            return field;
        }
    }
}
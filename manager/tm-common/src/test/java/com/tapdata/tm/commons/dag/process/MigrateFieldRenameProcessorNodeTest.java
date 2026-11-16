package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.Operation;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.util.PartitionTableFieldRenameOperator;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.schema.TapConstraint;
import io.tapdata.entity.schema.TapConstraintMapping;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/15 22:28
 */

public class MigrateFieldRenameProcessorNodeTest {

    @Test
    public void testMergeSchema() {

        MigrateFieldRenameProcessorNode renameProcessorNode = new MigrateFieldRenameProcessorNode();
        AtomicInteger counter = new AtomicInteger();
        LinkedList<TableFieldInfo> fieldMapping = Stream.generate(() -> {
            TableFieldInfo tableFieldInfo = new TableFieldInfo();
            int count = counter.incrementAndGet();
            if (count == 1)
                tableFieldInfo.setPreviousTableName("test");
            else
                tableFieldInfo.setPreviousTableName("test_" + count);

            tableFieldInfo.setFields(
                    Stream.generate(() -> {
                        FieldInfo fieldInfo = new FieldInfo();
                        fieldInfo.setSourceFieldName("name");
                        fieldInfo.setTargetFieldName("name_1");
                        return fieldInfo;
                    }).limit(1).collect(Collectors.toCollection(LinkedList::new))
            );

            return tableFieldInfo;
        }).limit(3).collect(Collectors.toCollection(LinkedList::new));

        renameProcessorNode.setFieldsMapping(fieldMapping);


        Assertions.assertDoesNotThrow(() -> {
            List<Schema> result = renameProcessorNode.mergeSchema(null, null, null);
            Assertions.assertNotNull(result);
            Assertions.assertEquals(0, result.size());
        });

        List<List<Schema>> inputSchemas = Stream.generate(() -> {
            AtomicInteger counterTable = new AtomicInteger(0);
            List<Schema> schemas = Stream.generate(() -> {
                Schema schema = new Schema();

                AtomicInteger counterField = new AtomicInteger(0);
                int countTable = counterTable.incrementAndGet();
                schema.setName("test_" + countTable);
                schema.setOriginalName("test_" + countTable);
                schema.setFields(
                        Stream.generate(() -> {
                            Field field = new Field();
                            field.setFieldName("test_field_" + counterField.incrementAndGet());
                            return field;
                        }).limit(countTable).collect(Collectors.toList())
                );
                return schema;
            }).limit(2).collect(Collectors.toList());

            return schemas;
        }).limit(2).collect(Collectors.toList());

        Assertions.assertEquals(2, inputSchemas.size());
        Assertions.assertEquals(4, inputSchemas.stream().mapToLong(Collection::size).sum());

        List<Schema> result = renameProcessorNode.mergeSchema(inputSchemas, null, null);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testMergeSchemaOperator() {

        MigrateFieldRenameProcessorNode renameProcessorNode = new MigrateFieldRenameProcessorNode();
        AtomicInteger counter = new AtomicInteger(1);
        LinkedList<TableFieldInfo> fieldMapping = Stream.generate(() -> {
            TableFieldInfo tableFieldInfo = new TableFieldInfo();
            int count = counter.incrementAndGet();
            tableFieldInfo.setPreviousTableName("test_" + count);

            tableFieldInfo.setFields(
                    Stream.generate(() -> {
                        FieldInfo fieldInfo = new FieldInfo();
                        fieldInfo.setSourceFieldName("test_field_1");
                        fieldInfo.setTargetFieldName("test_field_2");
                        fieldInfo.setIsShow(false);
                        return fieldInfo;
                    }).limit(1).collect(Collectors.toCollection(LinkedList::new))
            );

            return tableFieldInfo;
        }).limit(3).collect(Collectors.toCollection(LinkedList::new));

        renameProcessorNode.setFieldsMapping(fieldMapping);
        renameProcessorNode.setFieldsOperation(new Operation());
        renameProcessorNode.getFieldsOperation().setSuffix("_t");
        renameProcessorNode.getFieldsOperation().setPrefix("sys_");
        renameProcessorNode.getFieldsOperation().setCapitalized("toUpperCase");


        Assertions.assertDoesNotThrow(() -> {
            List<Schema> result = renameProcessorNode.mergeSchema(null, null, null);
            Assertions.assertNotNull(result);
            Assertions.assertEquals(0, result.size());
        });

        List<List<Schema>> inputSchemas = Stream.generate(() -> {
            AtomicInteger counterTable = new AtomicInteger(0);
            List<Schema> schemas = Stream.generate(() -> {
                Schema schema = new Schema();

                AtomicInteger counterField = new AtomicInteger(0);
                int countTable = counterTable.incrementAndGet();
                schema.setName("test_" + countTable);
                schema.setOriginalName("test_" + countTable);
                schema.setFields(
                        Stream.generate(() -> {
                            Field field = new Field();
                            field.setFieldName("test_field_" + counterField.incrementAndGet());
                            return field;
                        }).limit(countTable).collect(Collectors.toList())
                );
                return schema;
            }).limit(2).collect(Collectors.toList());

            return schemas;
        }).limit(2).collect(Collectors.toList());

        Assertions.assertEquals(2, inputSchemas.size());
        Assertions.assertEquals(4, inputSchemas.stream().mapToLong(Collection::size).sum());

        List<Schema> result = renameProcessorNode.mergeSchema(inputSchemas, null, null);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testFieldDdlEvent() {

        MigrateFieldRenameProcessorNode renameProcessorNode = new MigrateFieldRenameProcessorNode();
        Assertions.assertDoesNotThrow(() -> {
            TapAlterFieldNameEvent event = new TapAlterFieldNameEvent();
            renameProcessorNode.fieldDdlEvent(event);

            event.setTableId("test");

            AtomicInteger counter = new AtomicInteger();
            LinkedList<TableFieldInfo> fieldMapping = Stream.generate(() -> {
                TableFieldInfo tableFieldInfo = new TableFieldInfo();
                int count = counter.incrementAndGet();
                if (count == 1)
                    tableFieldInfo.setPreviousTableName("test");
                else
                    tableFieldInfo.setPreviousTableName("test_" + count);

                tableFieldInfo.setFields(
                        Stream.generate(() -> {
                            FieldInfo fieldInfo = new FieldInfo();
                            fieldInfo.setSourceFieldName("name");
                            fieldInfo.setTargetFieldName("name_1");
                            return fieldInfo;
                        }).limit(1).collect(Collectors.toCollection(LinkedList::new))
                );

                return tableFieldInfo;
            }).limit(3).collect(Collectors.toCollection(LinkedList::new));

            renameProcessorNode.setFieldsMapping(fieldMapping);

            ValueChange<String> nameChange = new ValueChange<>();
            nameChange.setAfter("name");
            nameChange.setBefore("name_1");
            event.setNameChange(nameChange);

            renameProcessorNode.fieldDdlEvent(event);

            Assertions.assertEquals("name_1", fieldMapping.get(0).getFields().get(0).getSourceFieldName());
        });

    }

    @Nested
    class ApplyConfigTest {
        @Test
        void testApplyConfigConstructor() {
            MigrateFieldRenameProcessorNode.ApplyConfig config = new MigrateFieldRenameProcessorNode.ApplyConfig(mock(MigrateFieldRenameProcessorNode.class));
            assertNotNull(config.targetFieldExistMaps);
        }

        @Test
        void testApplyConfigConstructorWithTableFieldInfos() {
            MigrateFieldRenameProcessorNode node = mock(MigrateFieldRenameProcessorNode.class);
            LinkedList<TableFieldInfo> tableFieldInfos = new LinkedList<>();
            TableFieldInfo tableFieldInfo = new TableFieldInfo();
            tableFieldInfo.setPreviousTableName("t1");
            LinkedList<FieldInfo> fields = new LinkedList<>();
            FieldInfo fieldInfo = new FieldInfo();
            fieldInfo.setSourceFieldName("A");
            fieldInfo.setTargetFieldName("B");
            FieldInfo fieldInfo1 = new FieldInfo();
            fieldInfo1.setSourceFieldName("B");
            fieldInfo1.setTargetFieldName("C");
            fields.add(fieldInfo);
            fields.add(fieldInfo1);
            tableFieldInfo.setFields(fields);
            tableFieldInfos.add(tableFieldInfo);
            when(node.getFieldsMapping()).thenReturn(tableFieldInfos);
            MigrateFieldRenameProcessorNode.ApplyConfig config = new MigrateFieldRenameProcessorNode.ApplyConfig(node);
            assertTrue(config.targetFieldExistMaps.containsKey("t1"));
            assertTrue(config.targetFieldExistMaps.get("t1").contains("B"));
        }
    }

    @Nested
    @DisplayName("Method handleForeignKeyConstraints test")
    class handleForeignKeyConstraintsTest {

        private MigrateFieldRenameProcessorNode migrateFieldRenameProcessorNode;
        private String tableName;

        @BeforeEach
        void setUp() {
            migrateFieldRenameProcessorNode = new MigrateFieldRenameProcessorNode();
            tableName = "test";
        }

        @Test
        @DisplayName("test rename foreignkey")
        void test1() {
            Schema schema = new Schema();
            List<TapConstraint> constraints = new ArrayList<>();
            TapConstraint tapConstraint = new TapConstraint("fk_1", TapConstraint.ConstraintType.FOREIGN_KEY);
            List<TapConstraintMapping> tapConstraintMappings = new ArrayList<>();
            TapConstraintMapping tapConstraintMapping = new TapConstraintMapping();
            tapConstraintMapping.foreignKey("fid");
            tapConstraintMapping.referenceKey("rid");
            tapConstraintMappings.add(tapConstraintMapping);
            tapConstraint.setMappingFields(tapConstraintMappings);
            constraints.add(tapConstraint);
            schema.setConstraints(constraints);
            MigrateFieldRenameProcessorNode.ApplyConfig applyConfig = mock(MigrateFieldRenameProcessorNode.ApplyConfig.class);
            MigrateFieldRenameProcessorNode.IOperator<List<TapConstraint>> foreignKeyConstraintIOperator = migrateFieldRenameProcessorNode.createForeignKeyConstraintIOperator();
            when(applyConfig.apply(eq(tableName), anyString(), any(), eq(foreignKeyConstraintIOperator))).thenAnswer(invocationOnMock -> {
                Object argument2 = invocationOnMock.getArgument(2);
                Object argument3 = invocationOnMock.getArgument(3);
                ((MigrateFieldRenameProcessorNode.IOperator<List<TapConstraint>>)argument3).renameField((List<TapConstraint>) argument2, "fid", "fid_new");
                return true;
            });
            Field field = new Field();
            field.setOriginalFieldName("fid");
            migrateFieldRenameProcessorNode.handleForeignKeyConstraints(schema, field, applyConfig, tableName, foreignKeyConstraintIOperator);
            TapConstraintMapping tapConstraintMappingRes = assertDoesNotThrow(() -> constraints.get(0).getMappingFields().get(0));
            assertEquals("fid_new", tapConstraintMappingRes.getForeignKey());
        }

        @Test
        @DisplayName("test delete foreignkey")
        void test2() {
            Schema schema = new Schema();
            List<TapConstraint> constraints = new ArrayList<>();
            TapConstraint tapConstraint = new TapConstraint("fk_1", TapConstraint.ConstraintType.FOREIGN_KEY);
            List<TapConstraintMapping> tapConstraintMappings = new ArrayList<>();
            TapConstraintMapping tapConstraintMapping = new TapConstraintMapping();
            tapConstraintMapping.foreignKey("fid");
            tapConstraintMapping.referenceKey("rid");
            tapConstraintMappings.add(tapConstraintMapping);
            tapConstraint.setMappingFields(tapConstraintMappings);
            constraints.add(tapConstraint);
            schema.setConstraints(constraints);
            MigrateFieldRenameProcessorNode.ApplyConfig applyConfig = mock(MigrateFieldRenameProcessorNode.ApplyConfig.class);
            MigrateFieldRenameProcessorNode.IOperator<List<TapConstraint>> foreignKeyConstraintIOperator = migrateFieldRenameProcessorNode.createForeignKeyConstraintIOperator();
            when(applyConfig.apply(eq(tableName), anyString(), any(), eq(foreignKeyConstraintIOperator))).thenAnswer(invocationOnMock -> {
                Object argument2 = invocationOnMock.getArgument(2);
                Object argument3 = invocationOnMock.getArgument(3);
                ((MigrateFieldRenameProcessorNode.IOperator<List<TapConstraint>>)argument3).deleteField((List<TapConstraint>) argument2, "fid");
                return true;
            });
            Field field = new Field();
            field.setOriginalFieldName("fid");
            migrateFieldRenameProcessorNode.handleForeignKeyConstraints(schema, field, applyConfig, tableName, foreignKeyConstraintIOperator);
            assertTrue(constraints.isEmpty());
        }

        @Test
        @DisplayName("test rename reference foreignkey")
        void test3() {
            Schema schema = new Schema();
            List<TapConstraint> constraints = new ArrayList<>();
            TapConstraint tapConstraint = new TapConstraint("fk_1", TapConstraint.ConstraintType.FOREIGN_KEY);
            tapConstraint.setReferencesTableName(tableName);
            List<TapConstraintMapping> tapConstraintMappings = new ArrayList<>();
            TapConstraintMapping tapConstraintMapping = new TapConstraintMapping();
            tapConstraintMapping.foreignKey("fid");
            tapConstraintMapping.referenceKey("rid");
            tapConstraintMappings.add(tapConstraintMapping);
            tapConstraint.setMappingFields(tapConstraintMappings);
            constraints.add(tapConstraint);
            schema.setConstraints(constraints);
            MigrateFieldRenameProcessorNode.ApplyConfig applyConfig = mock(MigrateFieldRenameProcessorNode.ApplyConfig.class);
            MigrateFieldRenameProcessorNode.IOperator<List<TapConstraint>> referenceForeignKeyConstraintIOperator = migrateFieldRenameProcessorNode.createReferenceForeignKeyConstraintIOperator();
            when(applyConfig.apply(eq(tableName), anyString(), any(), eq(referenceForeignKeyConstraintIOperator))).thenAnswer(invocationOnMock -> {
                Object argument2 = invocationOnMock.getArgument(2);
                Object argument3 = invocationOnMock.getArgument(3);
                ((MigrateFieldRenameProcessorNode.IOperator<List<TapConstraint>>)argument3).renameField((List<TapConstraint>) argument2, "rid", "rid_new");
                return true;
            });
            Field field = new Field();
            field.setOriginalFieldName("rid");
            migrateFieldRenameProcessorNode.handleReferenceForeignKeyConstraints(schema, applyConfig, referenceForeignKeyConstraintIOperator);
            TapConstraintMapping tapConstraintMappingRes = assertDoesNotThrow(() -> constraints.get(0).getMappingFields().get(0));
            assertEquals("rid_new", tapConstraintMappingRes.getReferenceKey());
        }

        @Test
        @DisplayName("test delete reference foreignkey")
        void test4() {
            Schema schema = new Schema();
            List<TapConstraint> constraints = new ArrayList<>();
            TapConstraint tapConstraint = new TapConstraint("fk_1", TapConstraint.ConstraintType.FOREIGN_KEY);
            tapConstraint.setReferencesTableName(tableName);
            List<TapConstraintMapping> tapConstraintMappings = new ArrayList<>();
            TapConstraintMapping tapConstraintMapping = new TapConstraintMapping();
            tapConstraintMapping.foreignKey("fid");
            tapConstraintMapping.referenceKey("rid");
            tapConstraintMappings.add(tapConstraintMapping);
            tapConstraint.setMappingFields(tapConstraintMappings);
            constraints.add(tapConstraint);
            schema.setConstraints(constraints);
            MigrateFieldRenameProcessorNode.ApplyConfig applyConfig = mock(MigrateFieldRenameProcessorNode.ApplyConfig.class);
            MigrateFieldRenameProcessorNode.IOperator<List<TapConstraint>> referenceForeignKeyConstraintIOperator = migrateFieldRenameProcessorNode.createReferenceForeignKeyConstraintIOperator();
            when(applyConfig.apply(eq(tableName), anyString(), any(), eq(referenceForeignKeyConstraintIOperator))).thenAnswer(invocationOnMock -> {
                Object argument2 = invocationOnMock.getArgument(2);
                Object argument3 = invocationOnMock.getArgument(3);
                ((MigrateFieldRenameProcessorNode.IOperator<List<TapConstraint>>)argument3).deleteField((List<TapConstraint>) argument2, "rid");
                return true;
            });
            Field field = new Field();
            field.setOriginalFieldName("rid");
            migrateFieldRenameProcessorNode.handleReferenceForeignKeyConstraints(schema, applyConfig, referenceForeignKeyConstraintIOperator);
            assertTrue(constraints.isEmpty());
            assertTrue(schema.getConstraints().isEmpty());
        }
    }

    @Nested
    class applyTest {
        @Test
        void testForTapAlterFieldNameEvent() {
            MigrateFieldRenameProcessorNode.ApplyConfig applyConfig = mock(MigrateFieldRenameProcessorNode.ApplyConfig.class);
            TapAlterFieldNameEvent operatorParam = new TapAlterFieldNameEvent();
            ValueChange<String> valueChange = new ValueChange<>("COLUMN", "TEST_COLUMN");
            operatorParam.setNameChange(valueChange);
            MigrateFieldRenameProcessorNode.IOperator<TapAlterFieldNameEvent> alterFieldNameOperator = new MigrateFieldRenameProcessorNode.IOperator<TapAlterFieldNameEvent>() {
                @Override
                public void renameField(TapAlterFieldNameEvent param, String fromName, String toName) {
                    param.getNameChange().setBefore(toName);
                }

                @Override
                public void renameAfterField(TapAlterFieldNameEvent param, String fromName, String toName) {
                    param.getNameChange().setAfter(toName);
                }
            };
            PartitionTableFieldRenameOperator partitionTableFieldRenameOperator = mock(PartitionTableFieldRenameOperator.class);
            Operation fieldsOperation = new Operation();
            fieldsOperation.setPrefix("p_");
            fieldsOperation.setCapitalized("toLowerCase");
            fieldsOperation.setSuffix("_t");
            ReflectionTestUtils.setField(applyConfig, "fieldsOperation", fieldsOperation);
            ReflectionTestUtils.setField(applyConfig, "partitionTableFieldRenameOperator", partitionTableFieldRenameOperator);
            doCallRealMethod().when(applyConfig).apply(any(Operation.class), anyString());
            doCallRealMethod().when(applyConfig).apply("test_table", "COLUMN", operatorParam, alterFieldNameOperator);
            applyConfig.apply("test_table", "COLUMN", operatorParam, alterFieldNameOperator);
            assertEquals("p_test_column_t", operatorParam.getNameChange().getAfter());
        }
    }

    @Nested
    @DisplayName("TAP-13224 foreign key without reference column")
    class NullReferenceKeyTest {

        private TapConstraint foreignKeyWithoutReferenceKey() {
            TapConstraint constraint = new TapConstraint("fk_batch", TapConstraint.ConstraintType.FOREIGN_KEY);
            constraint.setReferencesTableName("batch");
            List<TapConstraintMapping> mappings = new ArrayList<>();
            mappings.add(new TapConstraintMapping().foreignKey("batch_id"));
            constraint.setMappingFields(mappings);
            return constraint;
        }

        private Field field(String name) {
            Field field = new Field();
            field.setFieldName(name);
            field.setOriginalFieldName(name);
            field.setPreviousFieldName(name);
            return field;
        }

        @Test
        @DisplayName("mergeSchema upper-cases fields and drops a foreign key that has no reference column")
        void mergeSchemaWithNullReferenceKey() {
            MigrateFieldRenameProcessorNode node = new MigrateFieldRenameProcessorNode();
            node.setFieldsMapping(new LinkedList<>());
            Operation operation = new Operation();
            operation.setCapitalized("toUpperCase");
            node.setFieldsOperation(operation);

            Schema schema = new Schema();
            schema.setName("child");
            schema.setOriginalName("child");
            List<Field> fields = new ArrayList<>();
            fields.add(field("id"));
            fields.add(field("batch_id"));
            schema.setFields(fields);
            List<TapConstraint> constraints = new ArrayList<>();
            constraints.add(foreignKeyWithoutReferenceKey());
            schema.setConstraints(constraints);
            List<List<Schema>> inputSchemas = new ArrayList<>();
            List<Schema> input = new ArrayList<>();
            input.add(schema);
            inputSchemas.add(input);

            List<Schema> result = assertDoesNotThrow(() -> node.mergeSchema(inputSchemas, null, null));

            Schema merged = result.get(0);
            assertEquals(Stream.of("ID", "BATCH_ID").collect(Collectors.toList()),
                    merged.getFields().stream().map(Field::getFieldName).collect(Collectors.toList()));
            assertTrue(merged.getConstraints().isEmpty());
        }

        @Test
        @DisplayName("ApplyConfig.apply ignores a null field name")
        void applyIgnoresNullFieldName() {
            MigrateFieldRenameProcessorNode node = new MigrateFieldRenameProcessorNode();
            Operation operation = new Operation();
            operation.setPrefix("p_");
            operation.setCapitalized("toUpperCase");
            node.setFieldsOperation(operation);
            MigrateFieldRenameProcessorNode.ApplyConfig applyConfig = new MigrateFieldRenameProcessorNode.ApplyConfig(node);
            MigrateFieldRenameProcessorNode.IOperator<List<TapConstraint>> operator = mock(MigrateFieldRenameProcessorNode.IOperator.class);

            assertTrue(assertDoesNotThrow(() -> applyConfig.apply("batch", null, new ArrayList<>(), operator)));
            verifyNoInteractions(operator);
        }

        @Test
        @DisplayName("foreign key operators skip mappings with null keys")
        void operatorsSkipNullKeys() {
            MigrateFieldRenameProcessorNode node = new MigrateFieldRenameProcessorNode();
            TapConstraint withoutForeignKey = new TapConstraint("fk_null", TapConstraint.ConstraintType.FOREIGN_KEY);
            List<TapConstraintMapping> nullMappings = new ArrayList<>();
            nullMappings.add(new TapConstraintMapping());
            withoutForeignKey.setMappingFields(nullMappings);
            TapConstraint normal = new TapConstraint("fk_ok", TapConstraint.ConstraintType.FOREIGN_KEY);
            List<TapConstraintMapping> mappings = new ArrayList<>();
            mappings.add(new TapConstraintMapping().foreignKey("fid").referenceKey("rid"));
            normal.setMappingFields(mappings);
            List<TapConstraint> constraints = new ArrayList<>();
            constraints.add(withoutForeignKey);
            constraints.add(normal);

            assertDoesNotThrow(() -> {
                node.createForeignKeyConstraintIOperator().renameField(constraints, "fid", "FID");
                node.createReferenceForeignKeyConstraintIOperator().renameField(constraints, "rid", "RID");
            });
            assertEquals("FID", normal.getMappingFields().get(0).getForeignKey());
            assertEquals("RID", normal.getMappingFields().get(0).getReferenceKey());

            assertDoesNotThrow(() -> node.createReferenceForeignKeyConstraintIOperator().deleteField(constraints, "RID"));
            assertDoesNotThrow(() -> node.createForeignKeyConstraintIOperator().deleteField(constraints, "missing"));
            assertEquals(1, constraints.size());
            assertSame(withoutForeignKey, constraints.get(0));
        }

        @Test
        @DisplayName("foreign key operators skip constraints without mapping fields")
        void operatorsSkipNullMappingFields() {
            MigrateFieldRenameProcessorNode node = new MigrateFieldRenameProcessorNode();
            TapConstraint withoutMappings = new TapConstraint("fk_empty", TapConstraint.ConstraintType.FOREIGN_KEY);
            List<TapConstraint> constraints = new ArrayList<>();
            constraints.add(withoutMappings);

            assertDoesNotThrow(() -> {
                node.createForeignKeyConstraintIOperator().renameField(constraints, "fid", "FID");
                node.createForeignKeyConstraintIOperator().deleteField(constraints, "fid");
                node.createReferenceForeignKeyConstraintIOperator().renameField(constraints, "rid", "RID");
                node.createReferenceForeignKeyConstraintIOperator().deleteField(constraints, "rid");
            });
            assertEquals(1, constraints.size());
        }
    }

    @Nested
    @DisplayName("mergeSchema reference foreign key handling")
    class MergeSchemaReferenceKeyTest {

        private Field field(String name) {
            Field field = new Field();
            field.setFieldName(name);
            field.setOriginalFieldName(name);
            field.setPreviousFieldName(name);
            return field;
        }

        private TapConstraint foreignKey(String name, String referencesTable, String foreignKey, String referenceKey) {
            TapConstraint constraint = new TapConstraint(name, TapConstraint.ConstraintType.FOREIGN_KEY);
            constraint.setReferencesTableName(referencesTable);
            List<TapConstraintMapping> mappings = new ArrayList<>();
            mappings.add(new TapConstraintMapping().foreignKey(foreignKey).referenceKey(referenceKey));
            constraint.setMappingFields(mappings);
            return constraint;
        }

        private Schema childSchema(TapConstraint... constraints) {
            Schema schema = new Schema();
            schema.setName("child");
            schema.setOriginalName("child");
            List<Field> fields = new ArrayList<>();
            fields.add(field("id"));
            fields.add(field("batch_id"));
            fields.add(field("owner_id"));
            schema.setFields(fields);
            List<TapConstraint> list = new ArrayList<>();
            for (TapConstraint constraint : constraints) list.add(constraint);
            schema.setConstraints(list);
            return schema;
        }

        private List<List<Schema>> input(Schema schema) {
            List<Schema> input = new ArrayList<>();
            input.add(schema);
            List<List<Schema>> inputSchemas = new ArrayList<>();
            inputSchemas.add(input);
            return inputSchemas;
        }

        @Test
        @DisplayName("prefix is applied to each reference key once, regardless of field count")
        void prefixAppliedOnce() {
            MigrateFieldRenameProcessorNode node = new MigrateFieldRenameProcessorNode();
            node.setFieldsMapping(new LinkedList<>());
            Operation operation = new Operation();
            operation.setPrefix("sys_");
            node.setFieldsOperation(operation);
            Schema schema = childSchema(
                    foreignKey("fk_batch", "batch", "batch_id", "id"),
                    foreignKey("fk_owner", "owner", "owner_id", "id"));

            Schema merged = node.mergeSchema(input(schema), null, null).get(0);

            assertEquals(2, merged.getConstraints().size());
            for (TapConstraint constraint : merged.getConstraints()) {
                assertEquals("sys_id", constraint.getMappingFields().get(0).getReferenceKey());
                assertTrue(constraint.getMappingFields().get(0).getForeignKey().startsWith("sys_"));
                assertFalse(constraint.getMappingFields().get(0).getForeignKey().startsWith("sys_sys_"));
            }
        }

        @Test
        @DisplayName("deleting a referenced field removes the foreign key from the schema")
        void referenceDeleteKeptInSchema() {
            MigrateFieldRenameProcessorNode node = new MigrateFieldRenameProcessorNode();
            LinkedList<TableFieldInfo> mapping = new LinkedList<>();
            TableFieldInfo batch = new TableFieldInfo();
            batch.setPreviousTableName("batch");
            LinkedList<FieldInfo> batchFields = new LinkedList<>();
            FieldInfo hidden = new FieldInfo();
            hidden.setSourceFieldName("id");
            hidden.setIsShow(false);
            batchFields.add(hidden);
            batch.setFields(batchFields);
            mapping.add(batch);
            node.setFieldsMapping(mapping);
            TapConstraint fkOwner = foreignKey("fk_owner", "owner", "owner_id", "id");
            Schema schema = childSchema(foreignKey("fk_batch", "batch", "batch_id", "id"), fkOwner);

            Schema merged = node.mergeSchema(input(schema), null, null).get(0);

            assertEquals(1, merged.getConstraints().size());
            assertSame(fkOwner, merged.getConstraints().get(0));
            assertEquals("id", fkOwner.getMappingFields().get(0).getReferenceKey());
        }
    }
}

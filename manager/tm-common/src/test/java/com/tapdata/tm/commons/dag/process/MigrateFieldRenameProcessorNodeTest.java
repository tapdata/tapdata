package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.Operation;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.schema.TapConstraint;
import io.tapdata.entity.schema.TapConstraintMapping;
import org.junit.jupiter.api.*;

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
        }
    }
}

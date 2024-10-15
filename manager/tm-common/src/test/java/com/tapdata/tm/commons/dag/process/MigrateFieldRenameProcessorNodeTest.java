package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.Operation;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.util.JsonUtil;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

}

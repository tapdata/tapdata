package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/10 17:49
 */
public class FieldRenameProcessorNodeTest {

    @Test
    public void testEquals() {
        FieldRenameProcessorNode node = new FieldRenameProcessorNode();
        Assertions.assertTrue(node.equals(node));

        FieldRenameProcessorNode node1 = new FieldRenameProcessorNode();
        Assertions.assertTrue(node.equals(node1));

        node1.setFieldsNameTransform("test");
        Assertions.assertFalse(node.equals(node1));
    }

    @Test
    public void testFieldDdlEvent() throws Exception {
        FieldRenameProcessorNode node = new FieldRenameProcessorNode();
        List<FieldProcessorNode.Operation> operations = new ArrayList<>();
        operations.add(new FieldProcessorNode.Operation());
        operations.get(0).setField("test");
        operations.get(0).setOp("RENAME");
        operations.get(0).setOperand("test_1");
        node.setOperations(operations);

        TapCreateTableEvent event = new TapCreateTableEvent();
        event.setTableId("test");
        event.setTable(new TapTable());
        event.getTable().setId("test");
        event.getTable().setName("test");
        TapField field = new TapField();
        field.setName("test");
        event.getTable().add(field);
        node.fieldDdlEvent(event);

        Assertions.assertEquals(1, event.getTable().getNameFieldMap().size());

    }

    @Test
    public void testMerge() {
        FieldRenameProcessorNode node = new FieldRenameProcessorNode();
        List<FieldProcessorNode.Operation> operations = new ArrayList<>();
        operations.add(new FieldProcessorNode.Operation());
        operations.get(0).setField("test");
        operations.get(0).setOp("RENAME");
        operations.get(0).setOperand("test_1");
        node.setOperations(operations);

        List<Schema> schemas = Stream.generate(() -> {
            Schema schema = new Schema();
            List<Field> fields = Stream.generate(() -> {
                Field field = new Field();
                field.setFieldName("test_1");
                field.setDeleted(RandomUtils.nextBoolean());
                return field;
            }).limit(10).collect(Collectors.toList());
            schema.setFields(fields);
            return schema;
        }).limit(5).collect(Collectors.toList());

        Assertions.assertDoesNotThrow(() -> {
            node.mergeSchema(schemas, null, null);
        });

    }
}

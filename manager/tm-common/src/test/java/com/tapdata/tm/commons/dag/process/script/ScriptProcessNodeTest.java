package com.tapdata.tm.commons.dag.process.script;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class ScriptProcessNodeTest {

    @Test
    void analyseFieldsTerminatesWhenParsedMappingCyclesThroughMissingOutputFields() throws InterruptedException {
        ScriptProcessNode node = new ScriptProcessNode("processor");
        node.setScript("""
                var ret = {};
                ret.a = record.b;
                ret.b = record.c;
                ret.c = record.b;
                return ret;
                """);
        Field field = field("a", "String");
        Schema schema = schema(field);
        TapTable sourceTable = sourceTable("a");

        Throwable thrown = runAnalyseFields(node, schema, sourceTable);

        assertNull(thrown);
        assertEquals("b", field.getOriginalFieldName());
        assertEquals("b", field.getPreviousFieldName());
    }

    private Throwable runAnalyseFields(ScriptProcessNode node, Schema schema, TapTable sourceTable) throws InterruptedException {
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                node.analyseFields(schema, sourceTable);
            } catch (Throwable t) {
                thrown.set(t);
            }
        });
        thread.setDaemon(true);
        thread.start();
        thread.join(500L);

        assertFalse(thread.isAlive(), "analyseFields should terminate when fieldNameMapping contains a cycle");
        return thrown.get();
    }

    private Schema schema(Field field) {
        Schema schema = new Schema();
        schema.setFields(new ArrayList<>(List.of(field)));
        return schema;
    }

    private Field field(String fieldName, String dataType) {
        Field field = new Field();
        field.setFieldName(fieldName);
        field.setDataType(dataType);
        return field;
    }

    private TapTable sourceTable(String fieldName) {
        TapTable table = new TapTable("source");
        table.add(new TapField(fieldName, "String"));
        return table;
    }
}

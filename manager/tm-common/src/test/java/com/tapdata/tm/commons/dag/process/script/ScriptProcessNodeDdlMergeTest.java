package com.tapdata.tm.commons.dag.process.script;

import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Schema;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("开发任务脚本节点增量 DDL 推演无试运行结果时保留脚本输出模型")
class ScriptProcessNodeDdlMergeTest {
    private static final String TABLE = "tt_test_data";
    private ScriptProcessNode node;

    @BeforeEach
    void beforeEach() {
        node = new ScriptProcessNode("js_processor");
        node.setId("js-node");
        node.setName("JS");
        ReflectionTestUtils.setField(node, "trialRunUnavailable", true);
    }

    private static Field field(String name, String dataType) {
        Field field = new Field();
        field.setFieldName(name);
        field.setOriginalFieldName(name);
        field.setDataType(dataType);
        return field;
    }

    private static Field jsField(String name, String dataType) {
        Field field = new Field();
        field.setFieldName(name);
        field.setDataType(dataType);
        return field;
    }

    private static Schema schema(Field... fields) {
        Schema schema = new Schema();
        schema.setName(TABLE);
        schema.setOriginalName(TABLE);
        schema.setFields(new ArrayList<>(Arrays.asList(fields)));
        return schema;
    }

    private static Schema previousOutput() {
        return schema(field("id", "INT"), field("name", "VARCHAR(20)"),
                jsField("cdc_create_time", "DATETIME"), jsField("cdc_update_time", "DATETIME"), jsField("is_delete", "INT"));
    }

    private static <T extends TapDDLEvent> T table(T event) {
        event.setTableId(TABLE);
        return event;
    }

    private static TapNewFieldEvent addCol1() {
        return table(new TapNewFieldEvent().field(new TapField("col1", "VARCHAR(20)")));
    }

    private static List<String> names(Schema schema) {
        return schema.getFields().stream().map(Field::getFieldName).collect(Collectors.toList());
    }

    private Schema merge(TapDDLEvent event, Schema input) throws Exception {
        node.fieldDdlEvent(event);
        return node.mergeSchema(Collections.singletonList(input), null, null);
    }

    @Test
    @DisplayName("S1 加字段 DDL 后保留脚本新增字段并加入新字段")
    void testAddFieldKeepsScriptFields() throws Exception {
        node.setOutputSchema(previousOutput());

        Schema result = merge(addCol1(), schema(field("id", "INT"), field("name", "VARCHAR(20)"), field("col1", "VARCHAR(20)")));

        assertEquals(Arrays.asList("id", "name", "cdc_create_time", "cdc_update_time", "is_delete", "col1"), names(result));
    }

    @Test
    @DisplayName("S2 修改属性、删除字段 DDL 后保留脚本新增字段")
    void testAlterAndDropKeepScriptFields() throws Exception {
        node.setOutputSchema(previousOutput());
        Schema altered = merge(table(new TapAlterFieldAttributesEvent().fieldName("name")
                .dataType(ValueChange.create("VARCHAR(20)", "VARCHAR(30)"))), schema(field("id", "INT"), field("name", "VARCHAR(30)")));
        assertEquals(Arrays.asList("id", "name", "cdc_create_time", "cdc_update_time", "is_delete"), names(altered));
        assertEquals("VARCHAR(30)", altered.getFields().get(1).getDataType());

        node.setOutputSchema(altered);
        Schema dropped = merge(table(new TapDropFieldEvent().fieldName("name")), schema(field("id", "INT")));
        assertEquals(Arrays.asList("id", "cdc_create_time", "cdc_update_time", "is_delete"), names(dropped));
    }

    @Test
    @DisplayName("S3 上次输出模型可从任务元数据 PN_<节点> 读取")
    void testPreviousOutputFromMetadata() throws Exception {
        DAGDataServiceImpl service = mock(DAGDataServiceImpl.class);
        MetadataInstancesDto metadata = new MetadataInstancesDto();
        metadata.setOriginalName(TABLE);
        metadata.setFields(previousOutput().getFields());
        when(service.getMetadata(eq("PN_js-node"))).thenReturn(metadata);
        node.setService(service);

        Schema result = merge(addCol1(), schema(field("id", "INT"), field("name", "VARCHAR(20)"), field("col1", "VARCHAR(20)")));

        assertEquals(Arrays.asList("id", "name", "cdc_create_time", "cdc_update_time", "is_delete", "col1"), names(result));
    }

    @Test
    @DisplayName("S4 无上次输出、表不匹配或字段对应不上时使用输入模型")
    void testFallbackToInput() throws Exception {
        Schema input = schema(field("id", "INT"), field("col1", "VARCHAR(20)"));
        assertEquals(Arrays.asList("id", "col1"), names(merge(addCol1(), input)));

        node.setOutputSchema(previousOutput());
        TapNewFieldEvent otherTable = new TapNewFieldEvent().field(new TapField("col1", "VARCHAR(20)"));
        otherTable.setTableId("other");
        assertEquals(Arrays.asList("id", "col1"), names(merge(otherTable, input)));
        assertEquals(Arrays.asList("id", "col1"), names(merge(table(new TapNewFieldEvent().field(new TapField("missing", "INT"))), input)));
    }

    @Test
    @DisplayName("S5 有试运行结果、可试运行或非字段 DDL 时保持原行为")
    void testOriginalBehaviorKept() throws Exception {
        node.setOutputSchema(previousOutput());
        Schema input = schema(field("id", "INT"), field("col1", "VARCHAR(20)"));
        Schema trialRun = schema(field("id", "INT"), jsField("is_delete", "INT"));
        node.fieldDdlEvent(addCol1());
        assertSame(trialRun, node.mergeSchema(Collections.singletonList(input), trialRun, null));

        ReflectionTestUtils.setField(node, "trialRunUnavailable", false);
        assertEquals(Arrays.asList("id", "col1"), names(merge(addCol1(), input)));

        ReflectionTestUtils.setField(node, "trialRunUnavailable", true);
        assertEquals(Arrays.asList("id", "col1"), names(merge(new TapCreateTableEvent(), input)));
        assertEquals(Arrays.asList("id", "col1"), names(node.mergeSchema(Collections.singletonList(input), null, null)));
    }
}

package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.script.MigrateScriptProcessNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.*;

public class MigrateScriptProcessNodeTest {

    @Nested
    class AddCrateTapFieldTest{
        private MigrateScriptProcessNode migrateScriptProcessNode;
        @BeforeEach
        void beforeEach(){
            migrateScriptProcessNode=new MigrateScriptProcessNode("processor",Node.NodeCatalog.processor);
        }
        @Test
        void testAddCrateTapFieldNullMap(){
            TapTable table = new TapTable("test1","test1Id");
            migrateScriptProcessNode.addCrateTapField(null, table);
            assertNull(table.getNameFieldMap());
        }
        @Test
        void testAddCrateTapFieldCreateMap(){
            String dataType = "string";
            String tapFieldName = "name1";
            Map<String, TapField> createTapFieldMap = new HashMap<>();
            createTapFieldMap.put("create", new TapField(tapFieldName, dataType));
            TapTable table = new TapTable("test1", "testId");
            migrateScriptProcessNode.addCrateTapField(createTapFieldMap, table);
            assertNotNull(table.getNameFieldMap());
            LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
            TapField tapField = nameFieldMap.get(tapFieldName);
            assertNotNull(tapField);
            assertEquals(dataType, tapField.getDataType());
        }
    }

    @Nested
    @DisplayName("增量 DDL 推演无试运行结果时保留 JS 节点输出模型")
    class MergeSchemaWithoutJsResultTest {
        private static final String SOURCE_TABLE = "tt_test_data";
        private static final String RENAMED_TABLE = "ods_car_xdms_tt_test_data";
        private MigrateScriptProcessNode node;

        @BeforeEach
        void beforeEach() {
            node = new MigrateScriptProcessNode("migrate_js_processor", Node.NodeCatalog.processor);
            node.setId("js-node");
            node.setName("标准JS");
            ReflectionTestUtils.setField(node, "jsResultUnavailable", true);
        }

        private Field field(String name, String dataType) {
            Field field = new Field();
            field.setFieldName(name);
            field.setOriginalFieldName(name);
            field.setDataType(dataType);
            return field;
        }

        private Field jsField(String name, String dataType) {
            Field field = new Field();
            field.setFieldName(name);
            field.setDataType(dataType);
            return field;
        }

        private Schema schema(Field... fields) {
            Schema schema = new Schema();
            schema.setName(RENAMED_TABLE);
            schema.setOriginalName(RENAMED_TABLE);
            schema.setAncestorsName(SOURCE_TABLE);
            schema.setFields(new ArrayList<>(Arrays.asList(fields)));
            return schema;
        }

        /** 上次 JS 输出：源字段 id、name，加 JS 新增的 cdc_create_time、cdc_update_time、is_delete */
        private Schema previousOutput() {
            Field id = field("id", "INT");
            id.setPrimaryKey(true);
            id.setPrimaryKeyPosition(1);
            return schema(id, field("name", "VARCHAR(20)"),
                    jsField("cdc_create_time", "DATETIME"), jsField("cdc_update_time", "DATETIME"), jsField("is_delete", "INT"));
        }

        private List<List<Schema>> inputs(Schema... schemas) {
            return Collections.singletonList(new ArrayList<>(Arrays.asList(schemas)));
        }

        private <T extends TapDDLEvent> T table(T event) {
            event.setTableId(SOURCE_TABLE);
            return event;
        }

        private List<String> fieldNames(Schema schema) {
            return schema.getFields().stream().map(Field::getFieldName).collect(Collectors.toList());
        }

        private Field fieldOf(Schema schema, String name) {
            return schema.getFields().stream().filter(f -> name.equals(f.getFieldName())).findFirst().orElse(null);
        }

        private Schema merge(TapDDLEvent event, Schema input) throws Exception {
            node.fieldDdlEvent(event);
            List<Schema> result = node.mergeSchema(inputs(input), Collections.emptyList(), null);
            assertEquals(1, result.size());
            return result.get(0);
        }

        @Test
        @DisplayName("U1 加字段 DDL 后保留 JS 新增字段并加入新字段")
        void testAddFieldKeepsJsFields() throws Exception {
            node.setOutputSchema(Collections.singletonList(previousOutput()));
            Schema input = schema(field("id", "INT"), field("name", "VARCHAR(20)"), field("col1", "VARCHAR(20)"));

            Schema result = merge(table(new TapNewFieldEvent().field(new TapField("col1", "VARCHAR(20)"))), input);

            assertEquals(Arrays.asList("id", "name", "cdc_create_time", "cdc_update_time", "is_delete", "col1"), fieldNames(result));
            assertEquals(RENAMED_TABLE, result.getOriginalName());
            assertEquals(SOURCE_TABLE, result.getAncestorsName());
        }

        @Test
        @DisplayName("U2 修改字段属性后更新字段定义并保留 JS 字段")
        void testAlterFieldAttributesKeepsJsFields() throws Exception {
            node.setOutputSchema(Collections.singletonList(previousOutput()));
            Schema input = schema(field("id", "INT"), field("name", "VARCHAR(30)"));

            Schema result = merge(table(new TapAlterFieldAttributesEvent().fieldName("name")
                    .dataType(ValueChange.create("VARCHAR(20)", "VARCHAR(30)"))), input);

            assertEquals(Arrays.asList("id", "name", "cdc_create_time", "cdc_update_time", "is_delete"), fieldNames(result));
            assertEquals("VARCHAR(30)", fieldOf(result, "name").getDataType());
        }

        @Test
        @DisplayName("U3 删除字段后移除该字段并保留 JS 字段")
        void testDropFieldKeepsJsFields() throws Exception {
            node.setOutputSchema(Collections.singletonList(previousOutput()));
            Schema input = schema(field("id", "INT"));

            Schema result = merge(table(new TapDropFieldEvent().fieldName("name")), input);

            assertEquals(Arrays.asList("id", "cdc_create_time", "cdc_update_time", "is_delete"), fieldNames(result));
        }

        @Test
        @DisplayName("U4 字段改名后在原位置替换为新字段并保留 JS 字段")
        void testRenameFieldKeepsJsFields() throws Exception {
            node.setOutputSchema(Collections.singletonList(previousOutput()));
            Schema input = schema(field("id", "INT"), field("full_name", "VARCHAR(20)"));

            Schema result = merge(table(new TapAlterFieldNameEvent().nameChange(ValueChange.create("name", "full_name"))), input);

            assertEquals(Arrays.asList("id", "full_name", "cdc_create_time", "cdc_update_time", "is_delete"), fieldNames(result));
        }

        @Test
        @DisplayName("U5 JS 删除字段、转换类型、设置主键及索引在 DDL 后保持")
        void testJsRemoveConvertAndPrimaryKeyKept() throws Exception {
            Field id = field("id", "BIGINT");
            id.setPrimaryKey(true);
            id.setPrimaryKeyPosition(1);
            Field code = field("code", "VARCHAR(10)");
            code.setPrimaryKey(true);
            code.setPrimaryKeyPosition(2);
            Schema previous = schema(id, code, jsField("is_delete", "INT"));
            TableIndex index = new TableIndex();
            index.setIndexName("idx_code");
            TableIndexColumn column = new TableIndexColumn();
            column.setColumnName("code");
            index.setColumns(new ArrayList<>(Collections.singletonList(column)));
            previous.setIndices(new ArrayList<>(Collections.singletonList(index)));
            node.setOutputSchema(Collections.singletonList(previous));
            // 输入仍含 JS 已删除的 secret，id 仍为 INT，code 非主键
            Schema input = schema(field("id", "INT"), field("code", "VARCHAR(10)"), field("secret", "VARCHAR(10)"), field("col1", "VARCHAR(20)"));

            Schema result = merge(table(new TapNewFieldEvent().field(new TapField("col1", "VARCHAR(20)"))), input);

            assertEquals(Arrays.asList("id", "code", "is_delete", "col1"), fieldNames(result));
            assertEquals("BIGINT", fieldOf(result, "id").getDataType());
            assertTrue(fieldOf(result, "code").getPrimaryKey());
            assertEquals(1, result.getIndices().size());
            assertEquals("idx_code", result.getIndices().get(0).getIndexName());
        }

        @Test
        @DisplayName("U5b JS 已删除的字段不因属性变更 DDL 而恢复")
        void testAlterOnJsRemovedFieldNotRestored() throws Exception {
            node.setOutputSchema(Collections.singletonList(schema(field("id", "INT"), jsField("is_delete", "INT"))));
            Schema input = schema(field("id", "INT"), field("secret", "VARCHAR(30)"));

            Schema result = merge(table(new TapAlterFieldAttributesEvent().fieldName("secret")
                    .dataType(ValueChange.create("VARCHAR(10)", "VARCHAR(30)"))), input);

            assertEquals(Arrays.asList("id", "is_delete"), fieldNames(result));
        }

        @Test
        @DisplayName("U6 无上次输出模型时与原行为一致使用输入模型")
        void testNoPreviousOutputUsesInput() throws Exception {
            Schema input = schema(field("id", "INT"), field("col1", "VARCHAR(20)"));

            Schema result = merge(table(new TapNewFieldEvent().field(new TapField("col1", "VARCHAR(20)"))), input);

            assertSame(input, result);
        }

        @Test
        @DisplayName("U6b 上次输出模型可从任务元数据读取")
        void testPreviousOutputFromMetadata() throws Exception {
            DAGDataServiceImpl service = mock(DAGDataServiceImpl.class);
            MetadataInstancesDto metadata = new MetadataInstancesDto();
            metadata.setOriginalName(RENAMED_TABLE);
            metadata.setFields(previousOutput().getFields());
            when(service.getSchemaByNodeAndTableName(eq("js-node"), eq(RENAMED_TABLE))).thenReturn(metadata);
            node.setService(service);
            Schema input = schema(field("id", "INT"), field("name", "VARCHAR(20)"), field("col1", "VARCHAR(20)"));

            Schema result = merge(table(new TapNewFieldEvent().field(new TapField("col1", "VARCHAR(20)"))), input);

            assertEquals(Arrays.asList("id", "name", "cdc_create_time", "cdc_update_time", "is_delete", "col1"), fieldNames(result));
        }

        @Test
        @DisplayName("U6c DDL 字段在输入模型中找不到或表不匹配时使用输入模型")
        void testUnmatchedDdlUsesInput() throws Exception {
            node.setOutputSchema(Collections.singletonList(previousOutput()));
            Schema input = schema(field("id", "INT"), field("name", "VARCHAR(20)"));
            assertSame(input, merge(table(new TapNewFieldEvent().field(new TapField("not_exists", "VARCHAR(20)"))), input));

            TapNewFieldEvent otherTable = new TapNewFieldEvent().field(new TapField("col1", "VARCHAR(20)"));
            otherTable.setTableId("other_table");
            assertSame(input, merge(otherTable, input));
        }

        @Test
        @DisplayName("U7 可试运行或无 DDL 时保持原行为")
        void testJsResultAvailableKeepsOriginalBehavior() throws Exception {
            node.setOutputSchema(Collections.singletonList(previousOutput()));
            Schema input = schema(field("id", "INT"), field("col1", "VARCHAR(20)"));
            List<Schema> jsSchemas = Collections.singletonList(schema(field("id", "INT"), jsField("is_delete", "INT")));
            node.fieldDdlEvent(table(new TapNewFieldEvent().field(new TapField("col1", "VARCHAR(20)"))));
            assertSame(jsSchemas, node.mergeSchema(inputs(input), jsSchemas, null));

            ReflectionTestUtils.setField(node, "jsResultUnavailable", false);
            node.fieldDdlEvent(table(new TapNewFieldEvent().field(new TapField("col1", "VARCHAR(20)"))));
            assertSame(input, node.mergeSchema(inputs(input), Collections.emptyList(), null).get(0));

            ReflectionTestUtils.setField(node, "jsResultUnavailable", true);
            assertSame(input, node.mergeSchema(inputs(input), Collections.emptyList(), null).get(0));
        }

        @Test
        @DisplayName("U8 连续 DDL 基于上一次推演结果叠加")
        void testConsecutiveDdl() throws Exception {
            node.setOutputSchema(Collections.singletonList(previousOutput()));
            Schema first = merge(table(new TapNewFieldEvent().field(new TapField("col1", "VARCHAR(20)"))),
                    schema(field("id", "INT"), field("name", "VARCHAR(20)"), field("col1", "VARCHAR(20)")));
            node.setOutputSchema(Collections.singletonList(first));

            Schema second = merge(table(new TapNewFieldEvent().field(new TapField("col2", "VARCHAR(20)"))),
                    schema(field("id", "INT"), field("name", "VARCHAR(20)"), field("col1", "VARCHAR(20)"), field("col2", "VARCHAR(20)")));

            assertEquals(Arrays.asList("id", "name", "cdc_create_time", "cdc_update_time", "is_delete", "col1", "col2"), fieldNames(second));
        }

        @Test
        @DisplayName("U9 快照清空 outputSchema 后连续 DDL 仍基于上一次 DDL 的输出叠加")
        void testConsecutiveDdlAfterOutputSchemaCleared() throws Exception {
            node.setOutputSchema(Collections.singletonList(previousOutput()));
            Schema first = merge(table(new TapNewFieldEvent().field(new TapField("col1", "VARCHAR(20)"))),
                    schema(field("id", "INT"), field("name", "VARCHAR(20)"), field("col1", "VARCHAR(20)")));
            ReflectionTestUtils.setField(node, "lastSavedOutputSchema", Collections.singletonList(first));
            // 引擎保存快照时清空同一 DAG 各节点的 outputSchema；元数据中只有任务启动时的模型
            node.setOutputSchema(null);
            DAGDataServiceImpl service = mock(DAGDataServiceImpl.class);
            MetadataInstancesDto startup = new MetadataInstancesDto();
            startup.setOriginalName(RENAMED_TABLE);
            startup.setFields(previousOutput().getFields());
            when(service.getSchemaByNodeAndTableName(eq("js-node"), eq(RENAMED_TABLE))).thenReturn(startup);
            node.setService(service);

            Schema second = merge(table(new TapNewFieldEvent().field(new TapField("col2", "VARCHAR(20)"))),
                    schema(field("id", "INT"), field("name", "VARCHAR(20)"), field("col1", "VARCHAR(20)"), field("col2", "VARCHAR(20)")));

            assertEquals(Arrays.asList("id", "name", "cdc_create_time", "cdc_update_time", "is_delete", "col1", "col2"), fieldNames(second));
        }

        @Test
        @DisplayName("U10 类型变更不回退 JS 的类型转换，其它属性按 DDL 更新")
        void testAlterKeepsJsTypeConversion() throws Exception {
            Field id = field("id", "STRING");
            id.setOriginalDataType("INT");
            Field name = field("name", "VARCHAR(20)");
            name.setIsNullable(true);
            node.setOutputSchema(Collections.singletonList(schema(id, name, jsField("is_delete", "INT"))));
            Field inputName = field("name", "VARCHAR(20)");
            inputName.setIsNullable(false);

            Schema typeChanged = merge(table(new TapAlterFieldAttributesEvent().fieldName("id")
                    .dataType(ValueChange.create("INT", "BIGINT"))), schema(field("id", "BIGINT"), inputName));
            assertEquals("STRING", fieldOf(typeChanged, "id").getDataType());

            node.setOutputSchema(Collections.singletonList(typeChanged));
            Schema nullableChanged = merge(table(new TapAlterFieldAttributesEvent().fieldName("name")
                    .nullable(ValueChange.create(true, false))), schema(field("id", "BIGINT"), inputName));
            assertEquals(false, fieldOf(nullableChanged, "name").getIsNullable());
            assertEquals("STRING", fieldOf(nullableChanged, "id").getDataType());
            assertEquals(Arrays.asList("id", "name", "is_delete"), fieldNames(nullableChanged));
        }

        @Test
        @DisplayName("U11 JS 改名的字段在属性变更后不产生重复列")
        void testAlterOnJsRenamedFieldNoDuplicate() throws Exception {
            Field custName = jsField("cust_name", "VARCHAR(20)");
            custName.setOriginalFieldName("name");
            node.setOutputSchema(Collections.singletonList(schema(field("id", "INT"), custName, jsField("is_delete", "INT"))));

            Schema result = merge(table(new TapAlterFieldAttributesEvent().fieldName("name")
                    .dataType(ValueChange.create("VARCHAR(20)", "VARCHAR(30)"))), schema(field("id", "INT"), field("name", "VARCHAR(30)")));

            assertEquals(Arrays.asList("id", "cust_name", "is_delete"), fieldNames(result));
            assertEquals("VARCHAR(30)", fieldOf(result, "cust_name").getDataType());
        }

        @Test
        @DisplayName("U12 字段改名后保留该列索引并更新列名")
        void testRenameKeepsIndex() throws Exception {
            Schema previous = previousOutput();
            TableIndex index = new TableIndex();
            index.setIndexName("idx_name");
            TableIndexColumn column = new TableIndexColumn();
            column.setColumnName("name");
            index.setColumns(new ArrayList<>(Collections.singletonList(column)));
            previous.setIndices(new ArrayList<>(Collections.singletonList(index)));
            node.setOutputSchema(Collections.singletonList(previous));

            Schema result = merge(table(new TapAlterFieldNameEvent().nameChange(ValueChange.create("name", "full_name"))),
                    schema(field("id", "INT"), field("full_name", "VARCHAR(20)")));

            assertEquals(1, result.getIndices().size());
            assertEquals("full_name", result.getIndices().get(0).getColumns().get(0).getColumnName());
        }

        @Test
        @DisplayName("U8b 非 DDL 表同样保留 JS 输出，非字段 DDL 不记录")
        void testOtherTableKeepsJsOutput() throws Exception {
            Schema otherPrevious = schema(field("k", "INT"), jsField("is_delete", "INT"));
            otherPrevious.setName("other");
            otherPrevious.setOriginalName("other");
            otherPrevious.setAncestorsName("other");
            node.setOutputSchema(Arrays.asList(previousOutput(), otherPrevious));
            Schema otherInput = schema(field("k", "INT"));
            otherInput.setName("other");
            otherInput.setOriginalName("other");
            otherInput.setAncestorsName("other");
            node.fieldDdlEvent(table(new TapNewFieldEvent().field(new TapField("col1", "VARCHAR(20)"))));

            List<Schema> result = node.mergeSchema(inputs(
                    schema(field("id", "INT"), field("name", "VARCHAR(20)"), field("col1", "VARCHAR(20)")), otherInput),
                    Collections.emptyList(), null);

            assertEquals(Arrays.asList("k", "is_delete"), fieldNames(result.get(1)));
            node.fieldDdlEvent(new io.tapdata.entity.event.ddl.table.TapCreateTableEvent());
            assertSame(otherInput, node.mergeSchema(inputs(otherInput), Collections.emptyList(), null).get(0));
        }
    }
}

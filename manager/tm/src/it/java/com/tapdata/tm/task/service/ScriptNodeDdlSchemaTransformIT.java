package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 脚本节点增量 DDL 模型推演（L2-D）：用生产节点反序列化构建迁移任务「库节点 → 表编辑 → 标准JS → 库节点」
 * 与开发任务「表节点 → 标准JS → 表节点」DAG，按引擎 HazelcastSourcePdkBaseNode 处理字段 DDL 的顺序调用
 * DAG.filedDdlEvent 与 DAG.transformSchema。推演服务为不具备脚本试运行能力的 DAGDataServiceImpl
 * （getJsResult/loadTapTable 为 null），模型存储是外部边界。
 */
@Timeout(30)
class ScriptNodeDdlSchemaTransformIT {
    private static final String SOURCE_NODE = "src-node";
    private static final String RENAME_NODE = "rename-node";
    private static final String JS_NODE = "js-node";
    private static final String TARGET_NODE = "target-node";
    private static final String SOURCE_TABLE = "tt_test_data";
    private static final String TARGET_TABLE = "ods_car_xdms_tt_test_data";
    private static final List<String> JS_FIELDS = Arrays.asList("cdc_create_time", "cdc_update_time", "is_delete");

    private final ObjectId taskId = new ObjectId();
    private final String sourceConnectionId = new ObjectId().toHexString();
    private final String targetConnectionId = new ObjectId().toHexString();
    private final Map<String, List<Schema>> savedSchemas = new HashMap<>();
    private final List<Field> sourceFields = new ArrayList<>();
    private DAGDataServiceImpl service;
    private DAG dag;
    private String syncType;

    @BeforeEach
    void setUp() {
        sourceFields.clear();
        sourceFields.add(field("id", "INT", true));
        sourceFields.add(field("name", "VARCHAR(20)", false));
        savedSchemas.clear();
    }

    @Test
    @DisplayName("迁移任务增量加字段 DDL 推演后目标模型保留 JS 新增字段")
    void should_keep_js_fields_after_add_column_ddl() throws Exception {
        useMigrateTask();

        applyFieldDdl(addField("col1"));

        List<String> targetFields = targetFieldNames();
        assertTrue(targetFields.containsAll(JS_FIELDS), "目标模型缺少 JS 新增字段: " + targetFields);
        assertTrue(targetFields.containsAll(Arrays.asList("id", "name", "col1")), "目标模型缺少源字段: " + targetFields);
    }

    @Test
    @DisplayName("迁移任务连续加字段、删字段 DDL（中间经快照清空节点模型）推演后目标模型始终保留 JS 新增字段及上一次 DDL 的字段")
    void should_keep_js_fields_across_consecutive_ddl() throws Exception {
        useMigrateTask();

        applyFieldDdl(addField("col1"));
        assertTrue(targetFieldNames().containsAll(JS_FIELDS), "第一次 DDL 后缺少 JS 字段: " + targetFieldNames());
        snapshotClearsNodeSchemas();

        applyFieldDdl(addField("col2"));
        List<String> afterSecond = targetFieldNames();
        assertTrue(afterSecond.containsAll(JS_FIELDS), "第二次 DDL 后缺少 JS 字段: " + afterSecond);
        assertTrue(afterSecond.containsAll(Arrays.asList("col1", "col2")), "第二次 DDL 后缺少新增字段: " + afterSecond);

        applyFieldDdl(dropField("col1"));
        List<String> afterDrop = targetFieldNames();
        assertTrue(afterDrop.containsAll(JS_FIELDS), "删字段 DDL 后缺少 JS 字段: " + afterDrop);
        assertTrue(afterDrop.contains("col2"), "删字段 DDL 后缺少 col2: " + afterDrop);
        assertFalse(afterDrop.contains("col1"), "删字段 DDL 后仍保留 col1: " + afterDrop);
    }

    @Test
    @DisplayName("开发任务增量加字段、删字段 DDL（中间经快照清空节点模型）推演后目标模型保留 JS 新增字段及上一次 DDL 的字段")
    void should_keep_js_fields_in_sync_task_across_ddl() throws Exception {
        useSyncTask();

        applyFieldDdl(addField("col1"));
        List<String> afterAdd = targetFieldNames();
        assertTrue(afterAdd.containsAll(JS_FIELDS), "加字段 DDL 后缺少 JS 字段: " + afterAdd);
        assertTrue(afterAdd.containsAll(Arrays.asList("id", "name", "col1")), "加字段 DDL 后缺少源字段: " + afterAdd);
        snapshotClearsNodeSchemas();

        applyFieldDdl(dropField("name"));
        List<String> afterDrop = targetFieldNames();
        assertTrue(afterDrop.containsAll(JS_FIELDS), "删字段 DDL 后缺少 JS 字段: " + afterDrop);
        assertTrue(afterDrop.contains("col1"), "快照清空模型后的下一次 DDL 丢失上一次新增的 col1: " + afterDrop);
        assertFalse(afterDrop.contains("name"), "删字段 DDL 后仍保留 name: " + afterDrop);
    }

    /**
     * 与引擎目标节点 saveToSnapshot 上传 DDL 元数据时一致：清空同一 DAG 所有节点的 schema 与 outputSchema
     * （HazelcastTargetPdkBaseNode），源节点下一次 DDL 推演仍使用这个 DAG
     */
    private void snapshotClearsNodeSchemas() {
        for (Node node : dag.getNodes()) {
            node.setSchema(null);
            node.setOutputSchema(null);
        }
    }

    /** 与引擎 DDL 分支一致：先按 DDL 更新源表模型，再 filedDdlEvent，最后整 DAG 推演 */
    private void applyFieldDdl(TapDDLEvent event) throws Exception {
        if (event instanceof TapNewFieldEvent) {
            for (TapField tapField : ((TapNewFieldEvent) event).getNewFields()) {
                sourceFields.add(field(tapField.getName(), tapField.getDataType(), false));
            }
        } else if (event instanceof TapDropFieldEvent) {
            String name = ((TapDropFieldEvent) event).getFieldName();
            sourceFields.removeIf(f -> name.equals(f.getFieldName()));
        }
        dag.filedDdlEvent(SOURCE_NODE, event);
        DAG.Options options = new DAG.Options();
        options.setSyncType(syncType);
        options.setBatchNum(100);
        options.setUuid(String.valueOf(System.nanoTime()));
        Map<String, ?> errors = dag.transformSchema(null, service, options);
        assertTrue(errors.isEmpty(), "推演失败: " + errors);
    }

    private List<String> targetFieldNames() {
        List<Schema> target = savedSchemas.get(TARGET_NODE);
        assertNotNull(target, "目标节点模型未保存");
        assertEquals(1, target.size());
        assertEquals(TARGET_TABLE, target.get(0).getOriginalName());
        return target.get(0).getFields().stream().filter(f -> !f.isDeleted()).map(Field::getFieldName).collect(Collectors.toList());
    }

    private TapNewFieldEvent addField(String name) {
        TapNewFieldEvent event = new TapNewFieldEvent().field(new TapField(name, "VARCHAR(20)"));
        event.setTableId(SOURCE_TABLE);
        return event;
    }

    private TapDropFieldEvent dropField(String name) {
        TapDropFieldEvent event = new TapDropFieldEvent().fieldName(name);
        event.setTableId(SOURCE_TABLE);
        return event;
    }

    private static Field field(String name, String dataType, boolean primaryKey) {
        Field field = new Field();
        field.setFieldName(name);
        field.setOriginalFieldName(name);
        field.setDataType(dataType);
        field.setPrimaryKey(primaryKey);
        if (primaryKey) {
            field.setPrimaryKeyPosition(1);
        }
        return field;
    }

    private static Field copy(Field field) {
        return JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(field), Field.class);
    }

    private Schema sourceSchema() {
        Schema schema = new Schema();
        schema.setName(SOURCE_TABLE);
        schema.setOriginalName(SOURCE_TABLE);
        schema.setDatabaseId(new ObjectId().toHexString());
        schema.setFields(sourceFields.stream().map(ScriptNodeDdlSchemaTransformIT::copy).collect(Collectors.toList()));
        return schema;
    }

    private static List<Field> scriptOutputFields() {
        List<Field> fields = new ArrayList<>();
        fields.add(field("id", "INT", true));
        fields.add(field("name", "VARCHAR(20)", false));
        for (String jsField : JS_FIELDS) {
            fields.add(field(jsField, "is_delete".equals(jsField) ? "INT" : "DATETIME", false));
        }
        return fields;
    }

    /** 启动时引擎试运行得到的 JS 节点输出模型（任务元数据） */
    private static MetadataInstancesDto jsTrialRunOutput(String tableName) {
        MetadataInstancesDto metadata = new MetadataInstancesDto();
        metadata.setName(tableName);
        metadata.setOriginalName(tableName);
        metadata.setAncestorsName(SOURCE_TABLE);
        metadata.setFields(scriptOutputFields());
        return metadata;
    }

    /** 目标表已按启动时推演结果建好，包含 JS 新增字段 */
    private static Schema existingTargetSchema() {
        Schema schema = new Schema();
        schema.setName(TARGET_TABLE);
        schema.setOriginalName(TARGET_TABLE);
        schema.setFields(scriptOutputFields());
        return schema;
    }

    private void useMigrateTask() {
        syncType = TaskDto.SYNC_TYPE_MIGRATE;
        dag = migrateDag();
        service = storageBoundary();
        when(service.loadSchema(any(), eq(new ObjectId(sourceConnectionId)), anyList(), any()))
                .thenAnswer(invocation -> new ArrayList<>(Collections.singletonList(sourceSchema())));
        when(service.loadSchema(any(), eq(new ObjectId(targetConnectionId)), anyList(), any()))
                .thenAnswer(invocation -> new ArrayList<>());
        when(service.getSchemaByNodeAndTableName(eq(JS_NODE), eq(TARGET_TABLE))).thenReturn(jsTrialRunOutput(TARGET_TABLE));
    }

    private void useSyncTask() {
        syncType = TaskDto.SYNC_TYPE_SYNC;
        dag = syncDag();
        service = storageBoundary();
        when(service.loadSchema(any(), eq(new ObjectId(sourceConnectionId)), eq(SOURCE_TABLE)))
                .thenAnswer(invocation -> sourceSchema());
        when(service.loadSchema(any(), eq(new ObjectId(targetConnectionId)), eq(TARGET_TABLE)))
                .thenAnswer(invocation -> existingTargetSchema());
        when(service.getMetadata(eq("PN_" + JS_NODE))).thenReturn(jsTrialRunOutput(SOURCE_TABLE));
    }

    private DAGDataServiceImpl storageBoundary() {
        DAGDataServiceImpl storage = mock(DAGDataServiceImpl.class);
        TaskDto taskDto = new TaskDto();
        taskDto.setId(taskId);
        taskDto.setName("script-node-ddl-it");
        taskDto.setSyncType(syncType);
        taskDto.setTransformTaskId(new ObjectId().toHexString());
        taskDto.setDag(dag);
        when(storage.getTaskId()).thenReturn(taskId);
        when(storage.getTaskById(anyString())).thenReturn(taskDto);
        when(storage.getTaskById(isNull())).thenReturn(taskDto);
        // 增量 DDL 推演服务不具备脚本试运行能力
        when(storage.getJsResult(anyString(), anyString(), any())).thenReturn(null);
        when(storage.loadTapTable(anyString(), anyString(), any())).thenReturn(null);
        when(storage.getDataSource(eq(sourceConnectionId))).thenReturn(connection(sourceConnectionId, "Dameng"));
        when(storage.getDataSource(eq(targetConnectionId))).thenReturn(connection(targetConnectionId, "Doris"));
        doAnswer(invocation -> {
            List<Schema> schemas = invocation.getArgument(2);
            Node<?> node = invocation.getArgument(4);
            savedSchemas.put(node.getId(), schemas);
            return schemas;
        }).when(storage).createOrUpdateSchema(any(), any(), anyList(), any(), any());
        return storage;
    }

    private static DataSourceConnectionDto connection(String id, String databaseType) {
        DataSourceConnectionDto connection = new DataSourceConnectionDto();
        connection.setId(new ObjectId(id));
        connection.setDatabase_type(databaseType);
        connection.setName(databaseType);
        return connection;
    }

    private static Map<String, Object> jsNode() {
        Map<String, Object> js = new LinkedHashMap<>();
        js.put("id", JS_NODE);
        js.put("name", "标准JS");
        js.put("script", "function process(record){ record.cdc_create_time = new Date(); record.cdc_update_time = new Date(); record.is_delete = 0; return record; }");
        return js;
    }

    private DAG migrateDag() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("id", SOURCE_NODE);
        source.put("type", "database");
        source.put("name", "R6信创-测试环境-XDMS-afterservice_db");
        source.put("connectionId", sourceConnectionId);
        source.put("databaseType", "Dameng");
        source.put("migrateTableSelectType", "custom");
        source.put("tableNames", new ArrayList<>(Collections.singletonList(SOURCE_TABLE)));

        Map<String, Object> renameRule = new LinkedHashMap<>();
        renameRule.put("originTableName", SOURCE_TABLE);
        renameRule.put("previousTableName", SOURCE_TABLE);
        renameRule.put("currentTableName", TARGET_TABLE);
        Map<String, Object> rename = new LinkedHashMap<>();
        rename.put("id", RENAME_NODE);
        rename.put("type", "table_rename_processor");
        rename.put("name", "表编辑");
        rename.put("tableNames", Collections.singletonList(renameRule));

        Map<String, Object> js = jsNode();
        js.put("type", "standard_migrate_js_processor");

        Map<String, Object> target = new LinkedHashMap<>();
        target.put("id", TARGET_NODE);
        target.put("type", "database");
        target.put("name", "Doris-Dev");
        target.put("connectionId", targetConnectionId);
        target.put("databaseType", "Doris");

        return build(Arrays.asList(source, rename, js, target),
                Arrays.asList(edge(SOURCE_NODE, RENAME_NODE), edge(RENAME_NODE, JS_NODE), edge(JS_NODE, TARGET_NODE)),
                TaskDto.SYNC_TYPE_MIGRATE);
    }

    private DAG syncDag() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("id", SOURCE_NODE);
        source.put("type", "table");
        source.put("name", SOURCE_TABLE);
        source.put("tableName", SOURCE_TABLE);
        source.put("connectionId", sourceConnectionId);
        source.put("databaseType", "Dameng");

        Map<String, Object> js = jsNode();
        js.put("type", "standard_js_processor");

        Map<String, Object> target = new LinkedHashMap<>();
        target.put("id", TARGET_NODE);
        target.put("type", "table");
        target.put("name", TARGET_TABLE);
        target.put("tableName", TARGET_TABLE);
        target.put("connectionId", targetConnectionId);
        target.put("databaseType", "Doris");

        return build(Arrays.asList(source, js, target),
                Arrays.asList(edge(SOURCE_NODE, JS_NODE), edge(JS_NODE, TARGET_NODE)),
                TaskDto.SYNC_TYPE_SYNC);
    }

    private DAG build(List<Map<String, Object>> nodes, List<Map<String, Object>> edges, String type) {
        String json = JsonUtil.toJsonUseJackson(Map.of("nodes", nodes, "edges", edges));
        DAG built = DAG.build(JsonUtil.parseJsonUseJackson(json, Dag.class));
        built.setTaskId(taskId);
        built.setSyncType(type);
        return built;
    }

    private static Map<String, Object> edge(String source, String target) {
        Map<String, Object> edge = new LinkedHashMap<>();
        edge.put("source", source);
        edge.put("target", target);
        return edge;
    }
}

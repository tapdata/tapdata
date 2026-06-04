package com.tapdata.tm.commons.dag.process;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import com.tapdata.tm.commons.util.SqlParserUtil;
import io.tapdata.entity.schema.TapTable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@NodeType("duckdb_sql_processor")
@Getter
@Setter
@Slf4j
public class DuckDbSqlNode extends ProcessorNode {

    /** 默认批大小 */
    public static final int DEFAULT_BATCH_SIZE = 1000;
    
    /** 默认查询 SQL */
    public static final String DEFAULT_QUERY_SQL = "SELECT * FROM %s";

    @EqField
    @JsonAlias({"querySql"})
    private String querySql = DEFAULT_QUERY_SQL; // 保持兼容 (serialize as "sqlQuery", accept "querySql" too)

    @EqField
    private String wideTableName;
    
    @EqField
    private Integer batchSize = DEFAULT_BATCH_SIZE;
    
    @EqField
    private Boolean executeQueryOnFullSyncComplete = true;
    
    @EqField
    private Integer memoryLimitMB = 1024;
    
    @EqField
    private Integer queryTimeoutMs = 5000;
    
    @EqField
    private Integer maxActiveSources = 50;
    
    @EqField
    private Integer commitIntervalMs = 5000;
    
    @EqField
    private Integer errorThresholdCount = 100;
    
    @EqField
    private Double errorThresholdRate = 0.01;
    
    @EqField
    private Boolean duckLakeEnabled = false;
    
    @EqField
    private String duckLakeStorageType = "LOCAL";
    
    @EqField
    private String duckLakeStoragePath = "/tmp/ducklake";
    
    @EqField
    private String duckLakeMetadataDbUrl = null;

    // ========== 新增: 实时增量物化视图配置 ==========
    
    /** 宽表主键字段名（必填） */
    @EqField
    private String wideTablePrimaryKey;
    
    /** 是否启用宽表 CDC 输出（默认 false） */
    @EqField
    private Boolean outputChangelogEnabled = true;
    
    /** 主表名 */
    @EqField
    private String mainTableName;
    
    /** 主表主键字段 */
    @EqField
    private String mainTablePrimaryKey;
    
    /** 从表配置列表 */
    @EqField
    private List<FromTableConfig> fromTables = new ArrayList<>();
    
    /** 自定义 JOIN 查询（当 SQL 自动解析失败时使用） */
    @EqField
    private Map<String, String> customJoinQueries = new HashMap<>();

    /** 前置节点 Schema 列表（用于传递到 Engine 端，不需要持久化） */
    @EqField
    private List<TapTable> preNodeTapTables = new ArrayList<>();

    public DuckDbSqlNode() {
        super("duckdb_sql_processor");
    }

    /**
     * 解析主表信息和默认值
     * 1. 确定主表名（如果未配置，从第一个 fromTable 配置中获取）
     * 2. 确定主表主键（如果未配置，从第一个 fromTable 的 schema 中获取）
     * 3. 确定宽表名（如果未配置，使用 wide_ + 主表名）
     * 4. 确定宽表主键（如果未配置，使用主表主键）
     */
    private void resolveMainTableInfo(List<Schema> inputSchemas) {
        // 1. 确定主表
        if (StringUtils.isBlank(mainTableName)) {
            if (CollectionUtils.isNotEmpty(fromTables)) {
                FromTableConfig firstFromTable = fromTables.get(0);
                if (firstFromTable == null) {
                    throw new IllegalStateException("First fromTableConfig is null, cannot resolve mainTableName");
                }
                if (StringUtils.isBlank(firstFromTable.getTableNameInSql())) {
                    throw new IllegalStateException("Cannot resolve mainTableName, no tableNameInSql in first fromTableConfig");
                }
                mainTableName = firstFromTable.getTableNameInSql();
                log.info("Resolved mainTableName: {}", mainTableName);
            }
        }

        // 2. 确定主表主键（如果未配置，从第一个 fromTable 配置中获取）
        if (StringUtils.isBlank(mainTablePrimaryKey) && CollectionUtils.isNotEmpty(inputSchemas)) {
            Schema schema = findFirstFromTableSchema(inputSchemas);
            if (schema == null) {
                throw new IllegalStateException("Cannot resolve mainTablePrimaryKey, no schema found for mainTableName: " + mainTableName);
            }
            List<Field> primaryKeyFields = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(schema.getFields())) {
                for (Field field : schema.getFields()) {
                    if (Boolean.TRUE.equals(field.getPrimaryKey())) {
                        primaryKeyFields.add(field);
                    }
                }
            }
            if (!primaryKeyFields.isEmpty()) {
                // 组合主键处理：目前先取第一个，后续可以考虑支持复合主键
                mainTablePrimaryKey = primaryKeyFields.get(0).getFieldName();
            } else {
                throw new IllegalStateException("Cannot resolve mainTablePrimaryKey, no primary keys defined in schema for mainTableName: " + mainTableName);
            }
            if (StringUtils.isBlank(mainTablePrimaryKey)) {
                throw new IllegalStateException("mainTablePrimaryKey is blank after resolution, this should not happen");
            }
            log.info("Resolved mainTablePrimaryKey: {}", mainTablePrimaryKey);
        }

        // 3. 确定宽表名
        if (StringUtils.isBlank(wideTableName)) {
            if (StringUtils.isNotBlank(mainTableName)) {
                wideTableName = "wide_" + mainTableName;
            } else {
                throw new IllegalStateException("Cannot resolve wideTableName: mainTableName is blank");
            }
            log.info("Resolved wideTableName: {}", wideTableName);
        }

        // 4. 确定宽表主键
        if (StringUtils.isBlank(wideTablePrimaryKey)) {
            wideTablePrimaryKey = mainTablePrimaryKey;
            log.info("Resolved wideTablePrimaryKey from mainTablePrimaryKey: {}", wideTablePrimaryKey);
        }

        // 验证关键配置
        if (StringUtils.isBlank(wideTablePrimaryKey)) {
            throw new IllegalStateException("wideTablePrimaryKey is blank after resolution, this should not happen");
        }
    }

    /**
     * 根据 fromTables 配置找到第一个表对应的 schema
     */
    private Schema findFirstFromTableSchema(List<Schema> inputSchemas) {
        if (CollectionUtils.isEmpty(fromTables) || CollectionUtils.isEmpty(inputSchemas)) {
            return null;
        }
        FromTableConfig firstFromTable = fromTables.get(0);
        String preNodeId = firstFromTable.getPreNodeId();
        String tableNameInSql = firstFromTable.getTableNameInSql();
        
        // 先尝试匹配 preNodeId（如果有的话）
        if (StringUtils.isNotBlank(preNodeId)) {
            for (Schema schema : inputSchemas) {
                if (preNodeId.equals(schema.getNodeId())) {
                    return schema;
                }
            }
        }
        
        // 然后尝试匹配表名
        if (StringUtils.isNotBlank(tableNameInSql)) {
            for (Schema schema : inputSchemas) {
                if (tableNameInSql.equals(schema.getName()) || 
                    tableNameInSql.equals(schema.getOriginalName()) || 
                    tableNameInSql.equals(schema.getQualifiedName())) {
                    return schema;
                }
            }
        }
        
        // 如果都没找到，返回第一个 schema 作为 fallback
        return inputSchemas.get(0);
    }

    /**
     * 合并输入的 Schema 到输出 Schema 中
     * 关键工作流程
     * 1. 获取输入 ： inputSchemas 提供所有上游表的结构，包括主表、从表、视图等
     * 2. 解析 SQL ：使用 SqlParserUtil 从 querySql 中提取选择的字段、表名、别名等信息
     * 3. 构建输出 ：根据解析结果，创建新的 Schema 并设置宽表信息
     * 4. 解析 SQL ：使用 SqlParserUtil 从 querySql 中提取选择的字段
     * 5. 构建输出 ：创建新的 Schema 并设置宽表信息
     * 6. 主键设置 ：根据 wideTablePrimaryKey 配置设置主键
     */
    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        log.info("DuckDbSqlNode.mergeSchema() called");
        log.info("  inputSchemas: {}", CollectionUtils.isEmpty(inputSchemas) ? "empty" : inputSchemas.size());
        log.info("  querySql: {}", querySql);
        log.info("  fromTables: {}", CollectionUtils.isEmpty(fromTables) ? "empty" : fromTables.size());
        
        // ========== 严格校验：输入参数不能为空 ==========
        if (CollectionUtils.isEmpty(inputSchemas)) {
            throw new IllegalStateException("DuckDbSqlNode.mergeSchema() failed: inputSchemas is empty or null");
        }
        
        // ========== 严格校验：必须配置 querySql ==========
        if (StringUtils.isBlank(querySql)) {
            throw new IllegalStateException("DuckDbSqlNode.mergeSchema() failed: querySql is blank or null");
        }
        
        // ========== 严格执行：解析主表信息 ==========
        resolveMainTableInfo(inputSchemas);
        log.info("  resolved mainTableInfo: mainTableName={}, mainTablePrimaryKey={}, wideTableName={}, wideTablePrimaryKey={}", 
            mainTableName, mainTablePrimaryKey, wideTableName, wideTablePrimaryKey);
        
        // 校验主表信息
        if (StringUtils.isBlank(mainTableName)) {
            throw new IllegalStateException("DuckDbSqlNode.mergeSchema() failed: mainTableName is blank after resolution");
        }
        if (StringUtils.isBlank(wideTableName)) {
            throw new IllegalStateException("DuckDbSqlNode.mergeSchema() failed: wideTableName is blank after resolution");
        }
        
        // ========== 严格执行：解析 SQL 查询 ==========
        log.info("  Parsing SQL query to generate schema...");
        List<Field> fields;
        try {
            fields = SqlParserUtil.parseSelectFields(querySql, fromTables, inputSchemas, this);
        } catch (Exception e) {
            throw new RuntimeException("DuckDbSqlNode.mergeSchema() failed to parse SQL query: " + querySql, e);
        }
        
        // ========== 严格校验：必须解析出字段 ==========
        if (CollectionUtils.isEmpty(fields)) {
            throw new IllegalStateException("DuckDbSqlNode.mergeSchema() failed: no fields parsed from SQL query: " + querySql);
        }
        
        log.info("  Parsed {} fields from SQL", fields.size());
        
        // ========== 构建输出 Schema ==========
        Schema resultSchema = new Schema();
        
        // 复制输入 schema 的基本属性
        Schema firstInputSchema = inputSchemas.get(0);
        if (schema != null) {
            resultSchema.setMetaType(schema.getMetaType());
            resultSchema.setSourceType(schema.getSourceType());
        } else {
            resultSchema.setMetaType(firstInputSchema.getMetaType());
            resultSchema.setSourceType(firstInputSchema.getSourceType());
        }
        
        // 关键修复：设置 databaseId，用于后续 Schema 保存
        if (firstInputSchema.getDatabaseId() != null) {
            resultSchema.setDatabaseId(firstInputSchema.getDatabaseId());
            log.info("  Set databaseId from input schema: {}", firstInputSchema.getDatabaseId());
        }
        
        resultSchema.setCreateSource("job_analyze");
        
        // 设置宽表名称
        resultSchema.setName(wideTableName);
        resultSchema.setOriginalName(mainTableName);
        
        resultSchema.setFields(fields);
        
        // 设置主键、设置字段并填充详细信息
        if (StringUtils.isNotBlank(wideTablePrimaryKey)) {
            boolean foundPrimaryKey = false;
            for (Field field : fields) {
                if (wideTablePrimaryKey.equals(field.getFieldName())) {
                    field.setPrimaryKey(true);
                    field.setPrimaryKeyPosition(1);
                    foundPrimaryKey = true;
                }
                // 设置字段来源
                field.setSource("job_analyze");

                // 设置字段所属表名
                field.setTableName(mainTableName);

                // 设置字段ID（参考 MergeTableNode 和 JoinProcessorNode 的做法）
                String fieldId = MetaDataBuilderUtils.generateFieldId(this.getId(), mainTableName, field.getFieldName());
                field.setId(fieldId);
                field.setColumnPosition(1);

                // 如果没有设置原始字段名，则使用字段名
                if (StringUtils.isBlank(field.getOriginalFieldName())) {
                    field.setOriginalFieldName(field.getFieldName());
                }
            }
            if (!foundPrimaryKey) {
                throw new IllegalStateException("DuckDbSqlNode.mergeSchema() failed: wideTablePrimaryKey '" + wideTablePrimaryKey + 
                    "' not found in parsed fields: " + fields.stream().map(Field::getFieldName).toList());
            }
        }
        
        // 设置 Schema 的 qualifiedName（参考 MergeTableNode 的做法）
        String taskIdStr = taskId() != null ? taskId().toHexString() : null;
        String qualifiedName = MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), this.getId(), null, taskIdStr);
        resultSchema.setQualifiedName(qualifiedName);
        
        // 设置 Schema 的 taskId 和 nodeId
        resultSchema.setTaskId(taskIdStr);
        resultSchema.setNodeId(this.getId());
        
        log.info("  Successfully generated schema: name={}, qualifiedName={}, fields={}", 
            resultSchema.getName(), resultSchema.getQualifiedName(), resultSchema.getFields().size());
        
        return super.mergeSchema(Lists.newArrayList(resultSchema), schema, options);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof DuckDbSqlNode) {
            Class<?> className = DuckDbSqlNode.class;
            for (; className != Object.class; className = className.getSuperclass()) {
                java.lang.reflect.Field[] declaredFields = className.getDeclaredFields();
                for (java.lang.reflect.Field declaredField : declaredFields) {
                    EqField annotation = declaredField.getAnnotation(EqField.class);
                    if (annotation != null) {
                        try {
                            Object f2 = declaredField.get(o);
                            Object f1 = declaredField.get(this);
                            boolean b = fieldEq(f1, f2);
                            if (!b) {
                                return false;
                            }
                        } catch (IllegalAccessException e) {
                        }
                    }
                }
            }
            return true;
        }
        return false;
    }
}

package com.tapdata.tm.commons.dag.process;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.util.SqlParserUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

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

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        // 解析主表信息和默认值
        if (CollectionUtils.isNotEmpty(inputSchemas)) {
            resolveMainTableInfo(inputSchemas);
        }
        
        // 对于 DuckDB SQL 节点，输出 Schema 取决于查询结果
        if (StringUtils.isNotBlank(querySql)) {
            try {
                // 使用 SqlParserUtil 解析 SQL 并生成宽表字段
                List<Field> fields = SqlParserUtil.parseSelectFields(querySql, fromTables, inputSchemas, this);
                
                if (CollectionUtils.isNotEmpty(fields)) {
                    // 创建新的 Schema
                    Schema resultSchema = new Schema();
                    
                    // 复制输入 schema 的基本属性
                    if (schema != null) {
                        resultSchema.setMetaType(schema.getMetaType());
                        resultSchema.setSourceType(schema.getSourceType());
                        resultSchema.setCreateSource("job_analyze");
                    } else if (CollectionUtils.isNotEmpty(inputSchemas)) {
                        Schema firstSchema = inputSchemas.get(0);
                        resultSchema.setMetaType(firstSchema.getMetaType());
                        resultSchema.setSourceType(firstSchema.getSourceType());
                        resultSchema.setCreateSource("job_analyze");
                    }
                    
                    // 设置宽表名称（已经在 resolveMainTableInfo() 中处理）
                    resultSchema.setName(wideTableName);
                    resultSchema.setOriginalName(mainTableName);
                    
                    // 设置字段
                    resultSchema.setFields(fields);
                    
                    // 设置主键
                    if (StringUtils.isNotBlank(wideTablePrimaryKey)) {
                        for (Field field : fields) {
                            if (wideTablePrimaryKey.equals(field.getFieldName())) {
                                field.setPrimaryKey(true);
                                field.setPrimaryKeyPosition(1);
                                break;
                            }
                        }
                    }
                    
                    return resultSchema;
                }
            } catch (Exception e) {
                log.warn("Failed to parse SQL to generate schema: {}", e.getMessage(), e);
                // 解析失败时，回退到原始行为
            }
        }
        
        // 解析失败或没有查询 SQL 时，回退到原始行为
        if (inputSchemas != null && !inputSchemas.isEmpty()) {
            return inputSchemas.get(0);
        }
        return super.mergeSchema(inputSchemas, schema, options);
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

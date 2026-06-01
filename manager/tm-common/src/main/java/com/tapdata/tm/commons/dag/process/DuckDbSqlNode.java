package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NodeType("duckdb_sql_processor")
@Getter
@Setter
@Slf4j
public class DuckDbSqlNode extends ProcessorNode {

    /** 默认批大小 */
    public static final int DEFAULT_BATCH_SIZE = 1000;
    
    /** 默认输出表名 */
    public static final String DEFAULT_OUTPUT_TABLE_NAME = "duckdb_output";
    
    /** 默认查询 SQL */
    public static final String DEFAULT_QUERY_SQL = "SELECT * FROM %s";

    @EqField
    @JsonAlias({"querySql"})
    private String querySql = DEFAULT_QUERY_SQL; // 保持兼容 (serialize as "sqlQuery", accept "querySql" too)

    @EqField
    private String outputTableName = DEFAULT_OUTPUT_TABLE_NAME;
    
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
    private Boolean outputChangelogEnabled = false;
    
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

    // ========== 内部类: 从表配置 ==========
    @Getter
    @Setter
    public static class FromTableConfig {
        /** 前置节点 ID（用于查找对应的 NodeSchemaInfo） */
        private String preNodeId;
        
        /** SQL 中使用的表别名（如 t1, t2, users_alias） */
        private String tableNameInSql;
        
        public FromTableConfig() {}
        
        public FromTableConfig(String preNodeId, String tableNameInSql) {
            if (preNodeId == null || preNodeId.isBlank()) {
                throw new IllegalArgumentException("preNodeId must not be blank");
            }
            if (tableNameInSql == null || tableNameInSql.isBlank()) {
                throw new IllegalArgumentException("tableNameInSql must not be blank");
            }
            this.preNodeId = preNodeId;
            this.tableNameInSql = tableNameInSql;
        }
    }

    public DuckDbSqlNode() {
        super("duckdb_sql_processor");
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema, DAG.Options options) {
        // 对于 DuckDB SQL 节点，输出 Schema 取决于查询结果
        // 这里暂时返回输入的第一个 Schema，实际应根据查询 SQL 动态生成
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

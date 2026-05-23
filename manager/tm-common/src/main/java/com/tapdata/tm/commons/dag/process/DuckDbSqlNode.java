package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@NodeType("duckdb_sql")
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
    private String sqlQuery = DEFAULT_QUERY_SQL;
    
    @EqField
    private String querySql = DEFAULT_QUERY_SQL; // 保持兼容
    
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

    public DuckDbSqlNode() {
        super("duckdb_sql");
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

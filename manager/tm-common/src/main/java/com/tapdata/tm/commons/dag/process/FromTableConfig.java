package com.tapdata.tm.commons.dag.process;


import lombok.Data;

import java.io.Serializable;

/**
 * duckdb-sql 节点的 from 表配置项
 * 包含前置节点 ID 和 SQL 中使用的表别名
 * @author luke
 */
@Data
public class FromTableConfig implements Serializable {
    /**
     * 前置节点 ID（用于查找对应的 NodeSchemaInfo）
     */
    private String preNodeId;

    /**
     * SQL 中使用的表别名（如 t1, t2, users_alias）
     */
    private String tableNameInSql;

    public FromTableConfig() {
    }

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
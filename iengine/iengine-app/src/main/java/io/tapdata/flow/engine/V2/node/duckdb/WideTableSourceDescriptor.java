package io.tapdata.flow.engine.V2.node.duckdb;

/**
 * 描述宽表参与源表的运行时元信息。
 */
public class WideTableSourceDescriptor {
    private final String sourceTableName;
    private final String sqlAlias;
    private final boolean mainTable;
    private final NodeSchemaInfo schemaInfo;

    public WideTableSourceDescriptor(String sourceTableName,
                                     String sqlAlias,
                                     boolean mainTable,
                                     NodeSchemaInfo schemaInfo) {
        this.sourceTableName = sourceTableName;
        this.sqlAlias = sqlAlias;
        this.mainTable = mainTable;
        this.schemaInfo = schemaInfo;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public String getSqlAlias() {
        return sqlAlias;
    }

    public boolean isMainTable() {
        return mainTable;
    }

    public NodeSchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}

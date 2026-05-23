package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.NodeType;
import lombok.Getter;
import lombok.Setter;

@NodeType("duckdb_sql")
@Getter
@Setter
public class DuckDbSqlNode extends ProcessorNode {
    public DuckDbSqlNode() {
        super("duckdb_sql");
    }
}

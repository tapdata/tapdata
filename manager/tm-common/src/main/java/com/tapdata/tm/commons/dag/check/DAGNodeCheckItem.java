package com.tapdata.tm.commons.dag.check;

public enum DAGNodeCheckItem {
    CHECK_JOIN_NODE("DAG.JonNode"),
    DAG_NOT_NODES("DAG.NotNodes"),
    DAG_NOT_EDGES("DAG.NotEdges"),
    DAG_NODE_TOO_FEW("DAG.NodeTooFew"),
    DAG_NODE_ISOLATED("DAG.NodeIsolated"),
    DAG_EDGE_NOT_LINK("DAG.EdgeNotLink"),
    DAG_IS_CYCLIC("DAG.IsCyclic"),
    DAG_SOURCE_IS_NOT_DATA("DAG.SourceIsNotData"),
    DAG_MIGRATE_TASK_NOT_CONTAINS_TABLE("DAG.MigrateTaskNotContainsTable"),
    DAG_TAIL_IS_NOT_DATA("DAG.TailIsNotData"),

            ;
    String code;

    DAGNodeCheckItem(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}

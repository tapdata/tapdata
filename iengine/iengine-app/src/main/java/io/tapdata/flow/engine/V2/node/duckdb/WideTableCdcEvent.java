package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.Map;

/**
 * 宽表 CDC 事件
 * 表示宽表的一次变更操作（INSERT/UPDATE/DELETE）
 */
public class WideTableCdcEvent {

    public enum OpType { INSERT, UPDATE, DELETE }

    private final OpType opType;
    private final Object primaryKey;
    private final Map<String, Object> data;

    public WideTableCdcEvent(OpType opType, Object primaryKey, Map<String, Object> data) {
        this.opType = opType;
        this.primaryKey = primaryKey;
        this.data = data;
    }

    public OpType getOpType() {
        return opType;
    }

    public Object getPrimaryKey() {
        return primaryKey;
    }

    public Map<String, Object> getData() {
        return data;
    }

    @Override
    public String toString() {
        return "WideTableCdcEvent{op=" + opType + ", pk=" + primaryKey + "}";
    }
}

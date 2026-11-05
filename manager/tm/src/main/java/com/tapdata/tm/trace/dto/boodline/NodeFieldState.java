package com.tapdata.tm.trace.dto.boodline;

import lombok.Data;

import java.util.Objects;

@Data
public final class NodeFieldState {
    private final String nodeId;
    private final String tableName;
    private final String fieldName;

    public NodeFieldState(String nodeId, String tableName, String fieldName) {
        this.nodeId = nodeId;
        this.tableName = tableName;
        this.fieldName = fieldName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeFieldState that = (NodeFieldState) o;
        return Objects.equals(nodeId, that.nodeId)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, tableName, fieldName);
    }
}
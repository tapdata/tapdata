package io.tapdata.observable.metric.throttle;

import com.tapdata.tm.commons.throttle.DmlType;

import java.util.Objects;

/**
 * Immutable cache key isolating CDC byte sampling by endpoint, normalized table, DML operation, and schema generation.
 */
public final class CdcSampleGroupKey {

    private final String endpointId;
    private final String normalizedTable;
    private final DmlType dmlType;
    private final long schemaGeneration;

    public CdcSampleGroupKey(String endpointId, String normalizedTable, DmlType dmlType, long schemaGeneration) {
        this.endpointId = endpointId != null ? endpointId : "";
        this.normalizedTable = normalizedTable != null ? normalizedTable : "";
        if (dmlType == null) {
            throw new IllegalArgumentException("DmlType must not be null");
        }
        this.dmlType = dmlType;
        this.schemaGeneration = schemaGeneration;
    }

    public String getEndpointId() {
        return endpointId;
    }

    public String getNormalizedTable() {
        return normalizedTable;
    }

    public DmlType getDmlType() {
        return dmlType;
    }

    public long getSchemaGeneration() {
        return schemaGeneration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CdcSampleGroupKey that)) return false;
        return schemaGeneration == that.schemaGeneration
                && Objects.equals(endpointId, that.endpointId)
                && Objects.equals(normalizedTable, that.normalizedTable)
                && dmlType == that.dmlType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpointId, normalizedTable, dmlType, schemaGeneration);
    }

    @Override
    public String toString() {
        return endpointId + "|" + normalizedTable + "|" + dmlType + "|g" + schemaGeneration;
    }
}

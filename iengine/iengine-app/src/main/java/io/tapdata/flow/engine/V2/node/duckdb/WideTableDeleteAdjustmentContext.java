package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 删除语义调整的上下文对象。
 */
public final class WideTableDeleteAdjustmentContext {
    private String sourceTableName;
    private List<Map<String, Object>> beforeKeys;
    private List<Map<String, Object>> afterResults;
    private List<SmartMerger.MergedRecord> mergedRecords;
    private String wideTableName;
    private List<String> wideTablePrimaryKey;
    private DuckDbOperator duckDbOperator;
    private WideTableSourceRegistry sourceRegistry;
    private WideTableFieldOwnershipResolver fieldOwnershipResolver;

    public WideTableDeleteAdjustmentContext() {

    }

    public WideTableDeleteAdjustmentContext sourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
        return this;
    }

    public WideTableDeleteAdjustmentContext wideTableName(String wideTableName) {
        this.wideTableName = wideTableName;
        return this;
    }

    public WideTableDeleteAdjustmentContext wideTablePrimaryKey(List<String> wideTablePrimaryKey) {
        this.wideTablePrimaryKey = wideTablePrimaryKey;
        return this;
    }

    public WideTableDeleteAdjustmentContext sourceRegistry(WideTableSourceRegistry sourceRegistry) {
        this.sourceRegistry = sourceRegistry;
        return this;
    }

    public WideTableDeleteAdjustmentContext fieldOwnershipResolver(WideTableFieldOwnershipResolver fieldOwnershipResolver) {
        this.fieldOwnershipResolver = fieldOwnershipResolver;
        return this;
    }

    public WideTableDeleteAdjustmentContext duckDbOperator(DuckDbOperator duckDbOperator) {
        this.duckDbOperator = duckDbOperator;
        return this;
    }

    public WideTableDeleteAdjustmentContext beforeKeys(List<Map<String, Object>> beforeKeys) {
        this.beforeKeys = beforeKeys;
        return this;
    }

    public WideTableDeleteAdjustmentContext afterResults(List<Map<String, Object>> afterResults) {
        this.afterResults = afterResults == null ? new ArrayList<>() : afterResults;
        return this;
    }

    public WideTableDeleteAdjustmentContext mergedRecords(List<SmartMerger.MergedRecord> mergedRecords) {
        this.mergedRecords = mergedRecords == null ? new ArrayList<>() : mergedRecords;
        return this;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public List<Map<String, Object>> getBeforeKeys() {
        return beforeKeys;
    }

    public List<Map<String, Object>> getAfterResults() {
        return afterResults;
    }

    public List<SmartMerger.MergedRecord> getMergedRecords() {
        return mergedRecords;
    }

    public String getWideTableName() {
        return wideTableName;
    }

    public List<String> getWideTablePrimaryKey() {
        return wideTablePrimaryKey;
    }

    public DuckDbOperator getDuckDbOperator() {
        return duckDbOperator;
    }

    public WideTableSourceRegistry getSourceRegistry() {
        return sourceRegistry;
    }

    public WideTableFieldOwnershipResolver getFieldOwnershipResolver() {
        return fieldOwnershipResolver;
    }
}

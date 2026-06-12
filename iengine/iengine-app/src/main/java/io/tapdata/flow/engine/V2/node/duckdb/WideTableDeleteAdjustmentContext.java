package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 删除语义调整的上下文对象。
 */
public class WideTableDeleteAdjustmentContext {
    private final String sourceTableName;
    private final Set<Object> beforeKeys;
    private final List<Map<String, Object>> afterResults;
    private final List<SmartMerger.MergedRecord> mergedRecords;
    private final String wideTableName;
    private final String wideTablePrimaryKey;
    private final DuckDbOperator duckDbOperator;
    private final WideTableSourceRegistry sourceRegistry;
    private final WideTableFieldOwnershipResolver fieldOwnershipResolver;

    public WideTableDeleteAdjustmentContext(String sourceTableName,
                                            Set<Object> beforeKeys,
                                            List<Map<String, Object>> afterResults,
                                            List<SmartMerger.MergedRecord> mergedRecords,
                                            String wideTableName,
                                            String wideTablePrimaryKey,
                                            DuckDbOperator duckDbOperator,
                                            WideTableSourceRegistry sourceRegistry,
                                            WideTableFieldOwnershipResolver fieldOwnershipResolver) {
        this.sourceTableName = sourceTableName;
        this.beforeKeys = beforeKeys;
        this.afterResults = afterResults == null ? Collections.emptyList() : afterResults;
        this.mergedRecords = mergedRecords == null ? Collections.emptyList() : mergedRecords;
        this.wideTableName = wideTableName;
        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.duckDbOperator = duckDbOperator;
        this.sourceRegistry = sourceRegistry;
        this.fieldOwnershipResolver = fieldOwnershipResolver;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public Set<Object> getBeforeKeys() {
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

    public String getWideTablePrimaryKey() {
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

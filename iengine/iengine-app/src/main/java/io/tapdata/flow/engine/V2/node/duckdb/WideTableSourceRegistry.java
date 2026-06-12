package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 宽表源表注册表，统一维护主表/子表元信息。
 */
public class WideTableSourceRegistry {
    private final Map<String, WideTableSourceDescriptor> descriptorsBySourceTableName;
    private final String mainSourceTableName;

    private WideTableSourceRegistry(Map<String, WideTableSourceDescriptor> descriptorsBySourceTableName,
                                    String mainSourceTableName) {
        this.descriptorsBySourceTableName = descriptorsBySourceTableName;
        this.mainSourceTableName = mainSourceTableName;
    }

    public static WideTableSourceRegistry empty() {
        return new WideTableSourceRegistry(Collections.emptyMap(), null);
    }

    public static WideTableSourceRegistry from(String mainTableAlias,
                                               List<FromTableConfig> fromTables,
                                               Map<String, NodeSchemaInfo> nodeSchemaCache) {
        if (fromTables == null || fromTables.isEmpty() || nodeSchemaCache == null || nodeSchemaCache.isEmpty()) {
            return empty();
        }

        Map<String, WideTableSourceDescriptor> descriptors = new LinkedHashMap<>();
        String resolvedMainSourceTableName = null;
        boolean matchedConfiguredMain = false;

        for (int index = 0; index < fromTables.size(); index++) {
            FromTableConfig fromTable = fromTables.get(index);
            if (fromTable == null) {
                continue;
            }
            NodeSchemaInfo schemaInfo = nodeSchemaCache.get(fromTable.getPreNodeId());
            if (schemaInfo == null) {
                continue;
            }
            String sourceTableName = schemaInfo.getTargetTableName();
            boolean isMainTable = false;
            if (mainTableAlias != null && !mainTableAlias.isBlank()) {
                isMainTable = mainTableAlias.equalsIgnoreCase(fromTable.getTableNameInSql())
                        || mainTableAlias.equalsIgnoreCase(schemaInfo.getTableName())
                        || mainTableAlias.equalsIgnoreCase(sourceTableName);
            } else if (index == 0) {
                isMainTable = true;
            }
            if (isMainTable) {
                matchedConfiguredMain = true;
                resolvedMainSourceTableName = sourceTableName;
            }
            descriptors.put(normalize(sourceTableName), new WideTableSourceDescriptor(
                    sourceTableName,
                    fromTable.getTableNameInSql(),
                    isMainTable,
                    schemaInfo
            ));
        }

        if (!matchedConfiguredMain && !descriptors.isEmpty()) {
            WideTableSourceDescriptor first = descriptors.values().iterator().next();
            descriptors.put(normalize(first.getSourceTableName()), new WideTableSourceDescriptor(
                    first.getSourceTableName(),
                    first.getSqlAlias(),
                    true,
                    first.getSchemaInfo()
            ));
            resolvedMainSourceTableName = first.getSourceTableName();
        }

        return new WideTableSourceRegistry(descriptors, resolvedMainSourceTableName);
    }

    public boolean isEmpty() {
        return descriptorsBySourceTableName.isEmpty();
    }

    public boolean containsSourceTableName(String sourceTableName) {
        return descriptorsBySourceTableName.containsKey(normalize(sourceTableName));
    }

    public WideTableSourceDescriptor getDescriptor(String sourceTableName) {
        return descriptorsBySourceTableName.get(normalize(sourceTableName));
    }

    public Collection<WideTableSourceDescriptor> getDescriptors() {
        return descriptorsBySourceTableName.values();
    }

    public String getMainSourceTableName() {
        return mainSourceTableName;
    }

    private static String normalize(String value) {
        if (value == null) {
            return "";
        }
        return stripQuotes(value).toLowerCase(Locale.ROOT);
    }

    static String stripQuotes(String value) {
        if (value == null) {
            return null;
        }
        return value.replace("\"", "").replace("`", "");
    }
}

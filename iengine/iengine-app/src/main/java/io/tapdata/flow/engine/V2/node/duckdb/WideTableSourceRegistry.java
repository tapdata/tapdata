package io.tapdata.flow.engine.V2.node.duckdb;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

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
        return from(mainTableAlias, fromTables, nodeSchemaCache, null);
    }

    public static WideTableSourceRegistry from(String mainTableAlias,
                                               List<FromTableConfig> fromTables,
                                               Map<String, NodeSchemaInfo> nodeSchemaCache,
                                               String querySql) {
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
                    schemaInfo,
                    !isMainTable
            ));
        }

        if (!matchedConfiguredMain && !descriptors.isEmpty()) {
            WideTableSourceDescriptor first = descriptors.values().iterator().next();
            descriptors.put(normalize(first.getSourceTableName()), new WideTableSourceDescriptor(
                    first.getSourceTableName(),
                    first.getSqlAlias(),
                    true,
                    first.getSchemaInfo(),
                    false
            ));
            resolvedMainSourceTableName = first.getSourceTableName();
        }

        WideTableSourceRegistry registry = new WideTableSourceRegistry(descriptors, resolvedMainSourceTableName);
        if (querySql == null || querySql.isBlank()) {
            return registry;
        }
        return registry.withDeleteRetainSemantics(querySql);
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

    private WideTableSourceRegistry withDeleteRetainSemantics(String querySql) {
        Set<String> retainableSourceTables = resolveRetainableSourceTables(querySql);
        Map<String, WideTableSourceDescriptor> adjusted = new LinkedHashMap<>();
        descriptorsBySourceTableName.forEach((key, descriptor) -> adjusted.put(key, new WideTableSourceDescriptor(
                descriptor.getSourceTableName(),
                descriptor.getSqlAlias(),
                descriptor.isMainTable(),
                descriptor.getSchemaInfo(),
                !descriptor.isMainTable() && retainableSourceTables.contains(normalize(descriptor.getSourceTableName()))
        )));
        return new WideTableSourceRegistry(adjusted, mainSourceTableName);
    }

    private Set<String> resolveRetainableSourceTables(String querySql) {
        try {
            Statement statement = CCJSqlParserUtil.parse(querySql);
            if (!(statement instanceof Select select)) {
                return Collections.emptySet();
            }
            SelectBody selectBody = select.getSelectBody();
            if (selectBody instanceof SetOperationList setOperationList && !setOperationList.getSelects().isEmpty()) {
                selectBody = setOperationList.getSelects().get(0);
            }
            if (!(selectBody instanceof PlainSelect plainSelect)) {
                return Collections.emptySet();
            }

            Set<String> retainable = new LinkedHashSet<>();
            Set<String> joinedSources = resolveSourceTables(plainSelect.getFromItem());
            if (plainSelect.getJoins() == null) {
                return retainable;
            }

            for (Join join : plainSelect.getJoins()) {
                Set<String> rightSources = resolveSourceTables(join.getRightItem());
                JoinKind joinKind = resolveJoinKind(join, querySql);
                if (joinKind == JoinKind.LEFT) {
                    retainable.addAll(rightSources);
                } else if (joinKind == JoinKind.RIGHT) {
                    retainable.addAll(joinedSources);
                } else if (joinKind == JoinKind.FULL) {
                    retainable.addAll(joinedSources);
                    retainable.addAll(rightSources);
                }
                joinedSources.addAll(rightSources);
            }
            retainable.addAll(resolveRetainableSourceTablesBySqlText(querySql));
            return retainable;
        } catch (Exception ignored) {
            return resolveRetainableSourceTablesBySqlText(querySql);
        }
    }

    private Set<String> resolveRetainableSourceTablesBySqlText(String querySql) {
        if (querySql == null || querySql.isBlank()) {
            return Collections.emptySet();
        }
        Set<String> retainable = new LinkedHashSet<>();
        for (WideTableSourceDescriptor descriptor : descriptorsBySourceTableName.values()) {
            if (descriptor == null || descriptor.isMainTable()) {
                continue;
            }
            if (containsOuterJoinClauseForDescriptor(querySql, descriptor)) {
                retainable.add(normalize(descriptor.getSourceTableName()));
            }
        }
        return retainable;
    }

    private boolean containsOuterJoinClauseForDescriptor(String querySql, WideTableSourceDescriptor descriptor) {
        return containsJoinClauseForName(querySql, descriptor.getSourceTableName(), "LEFT")
                || containsJoinClauseForName(querySql, descriptor.getSqlAlias(), "LEFT")
                || containsSchemaJoinClause(querySql, descriptor, "LEFT")
                || containsJoinClauseForName(querySql, descriptor.getSourceTableName(), "FULL")
                || containsJoinClauseForName(querySql, descriptor.getSqlAlias(), "FULL")
                || containsSchemaJoinClause(querySql, descriptor, "FULL");
    }

    private boolean containsSchemaJoinClause(String querySql, WideTableSourceDescriptor descriptor, String joinKeyword) {
        NodeSchemaInfo schemaInfo = descriptor.getSchemaInfo();
        return schemaInfo != null && (containsJoinClauseForName(querySql, schemaInfo.getTableName(), joinKeyword)
                || containsJoinClauseForName(querySql, schemaInfo.getTargetTableName(), joinKeyword));
    }

    private Set<String> resolveSourceTables(FromItem fromItem) {
        if (!(fromItem instanceof Table table)) {
            return Collections.emptySet();
        }
        WideTableSourceDescriptor descriptor = findDescriptor(table.getName());
        if (descriptor == null && table.getAlias() != null) {
            descriptor = findDescriptor(table.getAlias().getName());
        }
        if (descriptor == null) {
            return Collections.emptySet();
        }
        return Collections.singleton(normalize(descriptor.getSourceTableName()));
    }

    private enum JoinKind {
        LEFT,
        RIGHT,
        FULL,
        OTHER
    }

    private JoinKind resolveJoinKind(Join join, String querySql) {
        if (join == null) {
            return JoinKind.OTHER;
        }
        String joinText = joinText(join);
        if (join.isLeft() || joinText.startsWith("LEFT ") || containsJoinClause(querySql, join.getRightItem(), "LEFT")) {
            return JoinKind.LEFT;
        }
        if (join.isRight() || joinText.startsWith("RIGHT ") || containsJoinClause(querySql, join.getRightItem(), "RIGHT")) {
            return JoinKind.RIGHT;
        }
        if (join.isFull() || joinText.startsWith("FULL ") || containsJoinClause(querySql, join.getRightItem(), "FULL")) {
            return JoinKind.FULL;
        }
        return JoinKind.OTHER;
    }

    private String joinText(Join join) {
        return join == null ? "" : join.toString().trim().toUpperCase(Locale.ROOT);
    }

    private boolean containsJoinClause(String querySql, FromItem rightItem, String joinKeyword) {
        if (querySql == null || querySql.isBlank() || !(rightItem instanceof Table table)) {
            return false;
        }
        return containsJoinClauseForName(querySql, table.getName(), joinKeyword)
                || (table.getAlias() != null && containsJoinClauseForName(querySql, table.getAlias().getName(), joinKeyword));
    }

    private boolean containsJoinClauseForName(String querySql, String tableName, String joinKeyword) {
        String normalizedName = stripQuotes(tableName);
        if (normalizedName == null || normalizedName.isBlank()) {
            return false;
        }
        Pattern pattern = Pattern.compile(
                "(?is).*\\b" + Pattern.quote(joinKeyword) + "\\s+(?:OUTER\\s+)?JOIN\\s+[`\"]?"
                        + Pattern.quote(normalizedName) + "[`\"]?(?:\\s|$).*"
        );
        return pattern.matcher(querySql).matches();
    }

    private WideTableSourceDescriptor findDescriptor(String tableNameOrAlias) {
        String normalized = normalize(tableNameOrAlias);
        WideTableSourceDescriptor descriptor = descriptorsBySourceTableName.get(normalized);
        if (descriptor != null) {
            return descriptor;
        }
        for (WideTableSourceDescriptor candidate : descriptorsBySourceTableName.values()) {
            if (normalized.equals(normalize(candidate.getSqlAlias()))) {
                return candidate;
            }
            NodeSchemaInfo schemaInfo = candidate.getSchemaInfo();
            if (schemaInfo != null && (normalized.equals(normalize(schemaInfo.getTableName()))
                    || normalized.equals(normalize(schemaInfo.getTargetTableName())))) {
                return candidate;
            }
        }
        return null;
    }

    static String stripQuotes(String value) {
        if (value == null) {
            return null;
        }
        return value.replace("\"", "").replace("`", "");
    }
}

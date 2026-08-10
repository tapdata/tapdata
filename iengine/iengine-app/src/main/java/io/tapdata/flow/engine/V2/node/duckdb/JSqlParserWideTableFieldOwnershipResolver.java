package io.tapdata.flow.engine.V2.node.duckdb;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 基于 JSqlParser 解析 querySql 中字段归属关系。
 */
public class JSqlParserWideTableFieldOwnershipResolver implements WideTableFieldOwnershipResolver {
    private static final Logger logger = LoggerFactory.getLogger(JSqlParserWideTableFieldOwnershipResolver.class);

    private final Map<String, Set<String>> ownedFieldsBySourceTable = new LinkedHashMap<>();

    public JSqlParserWideTableFieldOwnershipResolver(String querySql, WideTableSourceRegistry sourceRegistry) {
        if (querySql == null || querySql.isBlank() || sourceRegistry == null || sourceRegistry.isEmpty()) {
            return;
        }
        try {
            parse(querySql, sourceRegistry);
        } catch (Exception e) {
            logger.warn("Failed to resolve wide table field ownership: {}", e.getMessage());
        }
    }

    @Override
    public Set<String> resolveOwnedFields(String sourceTableName) {
        return ownedFieldsBySourceTable.getOrDefault(normalize(sourceTableName), Collections.emptySet());
    }

    private void parse(String querySql, WideTableSourceRegistry sourceRegistry) throws Exception {
        Statement statement = CCJSqlParserUtil.parse(querySql);
        if (!(statement instanceof Select select)) {
            return;
        }

        SelectBody selectBody = select.getSelectBody();
        if (selectBody instanceof SetOperationList setOperationList && !setOperationList.getSelects().isEmpty()) {
            selectBody = setOperationList.getSelects().get(0);
        }
        if (!(selectBody instanceof PlainSelect plainSelect)) {
            return;
        }

        Map<String, String> aliasToSourceTable = buildAliasMapping(plainSelect, sourceRegistry);
        List<String> aliases = new ArrayList<>(aliasToSourceTable.keySet());
        aliases.sort((left, right) -> Integer.compare(right.length(), left.length()));

        for (SelectItem selectItem : plainSelect.getSelectItems()) {
            if (selectItem instanceof AllColumns) {
                continue;
            }
            if (selectItem instanceof AllTableColumns allTableColumns) {
                String owner = aliasToSourceTable.get(normalize(allTableColumns.getTable().getName()));
                if (owner == null) {
                    continue;
                }
                WideTableSourceDescriptor descriptor = sourceRegistry.getDescriptor(owner);
                if (descriptor == null || descriptor.getSchemaInfo() == null) {
                    continue;
                }
                ownedFieldsBySourceTable
                        .computeIfAbsent(normalize(owner), key -> new LinkedHashSet<>())
                        .addAll(descriptor.getSchemaInfo().getFieldNames());
                continue;
            }
            if (selectItem instanceof SelectExpressionItem expressionItem) {
                String outputField = resolveOutputFieldName(expressionItem);
                if (outputField == null || outputField.isBlank()) {
                    continue;
                }
                Set<String> owners = resolveOwners(expressionItem.toString(), aliasToSourceTable, aliases);
                if (owners.size() == 1) {
                    String owner = owners.iterator().next();
                    ownedFieldsBySourceTable
                            .computeIfAbsent(normalize(owner), key -> new LinkedHashSet<>())
                            .add(outputField);
                }
            }
        }
    }

    private Map<String, String> buildAliasMapping(PlainSelect plainSelect, WideTableSourceRegistry sourceRegistry) {
        Map<String, String> aliasToSourceTable = new LinkedHashMap<>();
        registerFromItem(plainSelect.getFromItem(), sourceRegistry, aliasToSourceTable);
        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                registerFromItem(join.getRightItem(), sourceRegistry, aliasToSourceTable);
            }
        }
        return aliasToSourceTable;
    }

    private void registerFromItem(FromItem fromItem,
                                  WideTableSourceRegistry sourceRegistry,
                                  Map<String, String> aliasToSourceTable) {
        if (!(fromItem instanceof Table table)) {
            return;
        }
        String tableName = normalize(table.getName());
        if (!sourceRegistry.containsSourceTableName(tableName)) {
            return;
        }
        String sourceTableName = sourceRegistry.getDescriptor(tableName).getSourceTableName();
        aliasToSourceTable.put(tableName, sourceTableName);
        if (table.getAlias() != null && table.getAlias().getName() != null) {
            aliasToSourceTable.put(normalize(table.getAlias().getName()), sourceTableName);
        }
    }

    private Set<String> resolveOwners(String expression,
                                      Map<String, String> aliasToSourceTable,
                                      List<String> aliases) {
        Set<String> owners = new LinkedHashSet<>();
        String loweredExpression = expression.toLowerCase(Locale.ROOT);
        for (String alias : aliases) {
            Pattern pattern = Pattern.compile("(^|[^a-zA-Z0-9_])" + Pattern.quote(alias) + "\\s*\\.", Pattern.CASE_INSENSITIVE);
            if (pattern.matcher(loweredExpression).find()) {
                owners.add(aliasToSourceTable.get(alias));
            }
        }
        return owners;
    }

    private String resolveOutputFieldName(SelectExpressionItem expressionItem) {
        if (expressionItem.getAlias() != null && expressionItem.getAlias().getName() != null) {
            return expressionItem.getAlias().getName();
        }
        if (expressionItem.getExpression() instanceof net.sf.jsqlparser.schema.Column column) {
            return column.getColumnName();
        }
        return null;
    }

    private String normalize(String value) {
        return WideTableSourceRegistry.stripQuotes(value).toLowerCase(Locale.ROOT);
    }
}

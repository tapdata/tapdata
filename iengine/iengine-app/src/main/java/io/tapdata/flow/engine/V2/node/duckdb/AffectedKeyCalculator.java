package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.flow.engine.V2.node.duckdb.utils.TablePkUtils;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Calculator for determining affected wide table primary keys from CDC events.
 *
 * Features:
 * - Calculate affected main table PKs from main table events
 * - Find related main table PKs from secondary table events via JOIN queries
 * - Support automatic SQL JOIN parsing and user-defined JOIN queries
 */
public class AffectedKeyCalculator {

    private static final Logger logger = LogManager.getLogger(AffectedKeyCalculator.class);

    private final List<String> wideTablePrimaryKey;
    private final String wideTableName;
    private final String mainTableName;
    private final DuckDbOperator operator;
    private final WithCteSqlGenerator withCteSqlGenerator;
    private final Map<String, NodeSchemaInfo> nodeSchemaMap;
    private final String resolvedQuerySql;
    private final Map<String, String> mainTableFieldToWideField;

    /**
     * Full constructor with all required dependencies including NodeSchemaInfo and resolved query SQL.
     *
     * <p>This is the only supported constructor. All dependencies are injected via constructor
     * to ensure immutability and explicit dependency declaration.</p>
     *
     * @param wideTablePrimaryKey Wide table primary key field name
     * @param mainTableName Main table name (alias used in SQL)
     * @param fromTables List of predecessor node configurations
     * @param customJoinQueries Custom JOIN query mappings
     * @param operator DuckDB operator instance
     * @param nodeSchemaMap Predecessor node schema information mapping (preNodeId → NodeSchemaInfo)
     * @param resolvedQuerySql Resolved SQL statement where table aliases have been replaced with actual table names
     * @throws IllegalArgumentException if required parameters are null or blank
     */
    public AffectedKeyCalculator(
            List<String> wideTablePrimaryKey,
            String mainTableName,
            List<FromTableConfig> fromTables,
            Map<String, String> customJoinQueries,
            DuckDbOperator operator,
            Map<String, NodeSchemaInfo> nodeSchemaMap,
            String resolvedQuerySql
    ) {
        this(wideTablePrimaryKey, null, mainTableName, fromTables, customJoinQueries, operator, nodeSchemaMap, resolvedQuerySql);
    }

    public AffectedKeyCalculator(
            List<String> wideTablePrimaryKey,
            String wideTableName,
            String mainTableName,
            List<FromTableConfig> fromTables,
            Map<String, String> customJoinQueries,
            DuckDbOperator operator,
            Map<String, NodeSchemaInfo> nodeSchemaMap,
            String resolvedQuerySql
    ) {
        if (CollectionUtils.isEmpty(wideTablePrimaryKey)) {
            throw new IllegalArgumentException("wideTablePrimaryKey must not be null or blank");
        }

        Objects.requireNonNull(operator, "operator must not be null");
        Objects.requireNonNull(nodeSchemaMap, "nodeSchemaMap must not be null");
        Objects.requireNonNull(resolvedQuerySql, "resolvedQuerySql must not be null");

        if (resolvedQuerySql.isBlank()) {
            throw new IllegalArgumentException("resolvedQuerySql must not be blank");
        }

        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.wideTableName = wideTableName;
        this.mainTableName = mainTableName;
        this.operator = operator;
        this.nodeSchemaMap = nodeSchemaMap;
        this.resolvedQuerySql = resolvedQuerySql;
        this.withCteSqlGenerator = new WithCteSqlGenerator();
        this.mainTableFieldToWideField = resolveMainTableFieldToWideField(resolvedQuerySql, mainTableName, fromTables, nodeSchemaMap);
    }

    /**
     * 使用 WITH CTE SQL 查询宽表主键和完整结果
     * @param tableName 子表名
     * @param dataRows 数据行
     * @param fields 字段列表
     * @return AffectedKeysResult 包含宽表主键集合和查询结果
     */
    private AffectedKeysResult queryWideTablePksWithCte(String tableName, 
                                                        List<Map<String, Object>> dataRows, 
                                                        List<String> fields) throws SQLException {
        if (dataRows == null || dataRows.isEmpty()) {
            return new AffectedKeysResult(new ArrayList<>(), Collections.emptyList(), dataRows);
        }
        String querySql = getQuerySqlForTable(tableName);
        if (withCteSqlGenerator == null) {
            logger.warn("WithCteSqlGenerator not configured for table {}", tableName);
            return new AffectedKeysResult(new ArrayList<>(), Collections.emptyList(), dataRows);
        }
        
        // 生成 WITH CTE SQL
        String withSql = withCteSqlGenerator.generateBatch(querySql, tableName, dataRows, fields);
        // 执行查询
        List<Map<String, Object>> results = operator.executeQuery(withSql);
        
        // 提取宽表主键
        List<Map<String, Object>> wideTablePks = TablePkUtils.pkValues(results, wideTablePrimaryKey);
        return new AffectedKeysResult(wideTablePks, results != null ? results : Collections.emptyList(), dataRows);
    }

    /**
     * 获取表对应的 querySql
     * 从 fromTables 中查找匹配的表，返回其 querySql
     * TODO: querySql 已从 FromTableConfig 移除，需要从 HazelcastDuckDbSqlNode.querySql 获取
     */
    /**
     * Get the resolved query SQL.
     *
     * <p>Returns the pre-resolved SQL from HazelcastDuckDbSqlNode where
     * table aliases have been replaced with actual target table names.</p>
     *
     * @param tableName Table name (parameter kept for interface compatibility, but not used in lookup)
     * @return The resolved SQL statement
     * @throws IllegalStateException if resolvedQuerySql is blank (should never happen after constructor validation)
     */
    private String getQuerySqlForTable(String tableName) {
        if (resolvedQuerySql == null || resolvedQuerySql.isBlank()) {
            throw new IllegalStateException(
                "resolvedQuerySql must not be null or blank. " +
                "Ensure HazelcastDuckDbSqlNode.resolveSqlTableAliases() was called before using AffectedKeyCalculator.");
        }

        logger.debug("Returning resolved querySql for table {}: {}",
                    tableName,
                    resolvedQuerySql.substring(0, Math.min(50, resolvedQuerySql.length())));

        return resolvedQuerySql;
    }

    /**
     * 获取表的字段列表
     * TODO: fields 已从 FromTableConfig 移除，需要从 NodeSchemaInfo 获取
     */
    /**
     * Get field name list for a table.
     *
     * <p>Retrieves complete field information from NodeSchemaInfo.</p>
     *
     * @param tableName Table alias as used in SQL
     * @return List of field names, falls back to empty list if schema not found
     */
    private List<String> getTableFields(String tableName) {
        NodeSchemaInfo schemaInfo = findSchemaInfoByTableName(tableName);

        if (schemaInfo == null) {
            logger.warn("Cannot find NodeSchemaInfo for tableNameInSql={}, available nodeIds: {}",
                       tableName,
                       nodeSchemaMap.keySet().stream().limit(10).collect(Collectors.joining(", ")));

            logger.info("Returning empty field list for table {} (schema not found)", tableName);
            return Collections.emptyList();
        }

        List<String> fieldNames = schemaInfo.getFieldNames();

        logger.debug("Retrieved {} fields for table {}: {}",
                    fieldNames.size(),
                    tableName,
                    fieldNames.stream().limit(5).collect(Collectors.joining(", ")));

        return fieldNames;
    }

    private NodeSchemaInfo findSchemaInfoByTableName(String tableName) {
        if (nodeSchemaMap == null || nodeSchemaMap.isEmpty()) {
            return null;
        }

        if (tableName == null || tableName.isBlank()) {
            return null;
        }

        NodeSchemaInfo schemaInfo = nodeSchemaMap.get(tableName);
        if (schemaInfo != null) {
            return schemaInfo;
        }

        for (NodeSchemaInfo info : nodeSchemaMap.values()) {
            if (info == null) {
                continue;
            }
            if (tableName.equalsIgnoreCase(info.getTableName()) || tableName.equalsIgnoreCase(info.getTargetTableName())) {
                return info;
            }
        }
        return null;
    }

    /**
     * Overloaded: Calculate affected before keys from merged records
     */
    public List<Map<String, Object>> calculateAffectedBeforeKeys(List<SmartMerger.MergedRecord> mergedRecords, String tableName) throws SQLException {
        if (mergedRecords == null) {
            return new ArrayList<>();
        }
        List<Map<String, Object>> affectedBeforeKeys = new ArrayList<>();
        //logger.info("Processing main table before merged records from {}", tableName);

        List<Map<String, Object>> beforeRows = mergedRecords.stream()
                .map(SmartMerger.MergedRecord::getBeforeRows)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        if (beforeRows.isEmpty()) {
            return affectedBeforeKeys;
        }

        List<String> fields = getTableFields(tableName);
        AffectedKeysResult result = queryWideTablePksWithCte(tableName, beforeRows, fields);
        List<Map<String, Object>> wideTablePks = result.getWideTablePks();
        if (isMainTable(tableName) && CollectionUtils.isEmpty(wideTablePks)) {
            List<Map<String, Object>> existingWideTablePks = queryExistingWideTablePksByMainRows(tableName, beforeRows);
            if (!CollectionUtils.isEmpty(existingWideTablePks)) {
                //logger.info("Main table before PKs for {} resolved from existing wide table: {}", tableName, existingWideTablePks);
                return existingWideTablePks;
            }
        }
        logger.info("Wide table before PKs for {}: {}", tableName, wideTablePks);
        return wideTablePks;
    }

    /**
     * Calculate affected after keys from merged records.
     * 
     * @return AffectedKeysResult containing wideTablePks, queryResults, and afterRows
     */
    public AffectedKeysResult calculateAffectedAfterKeys(List<SmartMerger.MergedRecord> mergedRecords, 
                                                         String tableName) throws SQLException {
        if (mergedRecords == null || mergedRecords.isEmpty()) {
            return new AffectedKeysResult(new ArrayList<>(), Collections.emptyList(), Collections.emptyList());
        }
        
        logger.info("Processing main table after merged records from {}", tableName);
        
        // 收集 afterRows（主表和子表都需要）
        List<Map<String, Object>> afterRows = mergedRecords.stream()
                .map(SmartMerger.MergedRecord::getAfterRows)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        
        if (afterRows.isEmpty()) {
            return new AffectedKeysResult(new ArrayList<>(), Collections.emptyList(), Collections.emptyList());
        }

        List<String> fields = getTableFields(tableName);
        AffectedKeysResult result = queryWideTablePksWithCte(tableName, afterRows, fields);
        logger.info("Wide table after PKs for {}: {}", tableName, result.getWideTablePks());
        return result;
    }

    private boolean isMainTable(String tableName) {
        if (mainTableName == null || tableName == null) {
            return false;
        }
        if (mainTableName.equalsIgnoreCase(tableName)) {
            return true;
        }
        NodeSchemaInfo tableSchemaInfo = findSchemaInfoByTableName(tableName);
        if (tableSchemaInfo == null) {
            return false;
        }
        return mainTableName.equalsIgnoreCase(tableSchemaInfo.getTableName())
                || mainTableName.equalsIgnoreCase(tableSchemaInfo.getTargetTableName());
    }

    private List<Map<String, Object>> queryExistingWideTablePksByMainRows(String tableName,
                                                                          List<Map<String, Object>> mainRows) throws SQLException {
        if (wideTableName == null || wideTableName.isBlank() || CollectionUtils.isEmpty(mainRows)
                || mainTableFieldToWideField.isEmpty()) {
            logger.info("Skip existing wide table lookup for {}, wideTableName={}, mainRows={}, fieldMapping={}",
                    tableName,
                    wideTableName,
                    mainRows == null ? 0 : mainRows.size(),
                    mainTableFieldToWideField);
            return Collections.emptyList();
        }

        List<String> sourceFields = selectMainSourceFieldsForFallback(tableName);
        if (sourceFields.isEmpty()) {
            logger.info("Skip existing wide table lookup for {}, no source fields selected from mapping {}",
                    tableName, mainTableFieldToWideField);
            return Collections.emptyList();
        }

        String where = buildExistingWideTableWhere(sourceFields, mainRows);
        if (where.isBlank()) {
            logger.info("Skip existing wide table lookup for {}, empty where clause, sourceFields={}",
                    tableName, sourceFields);
            return Collections.emptyList();
        }

        String columns = wideTablePrimaryKey.stream()
                .map(WideTableDdlGenerator::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String sql = String.format("SELECT %s FROM %s WHERE %s",
                columns,
                WideTableDdlGenerator.quoteIdentifier(wideTableName),
                where);
        logger.info("Query existing wide table PKs for main table {}: {}", tableName, sql);
        List<Map<String, Object>> existingRows = operator.executeQuery(sql);
        return TablePkUtils.pkValues(existingRows, wideTablePrimaryKey);
    }

    private List<String> selectMainSourceFieldsForFallback(String tableName) {
        NodeSchemaInfo schemaInfo = findSchemaInfoByTableName(tableName);
        if (schemaInfo == null) {
            schemaInfo = findSchemaInfoByTableName(mainTableName);
        }
        if (schemaInfo != null && !CollectionUtils.isEmpty(schemaInfo.getPrimaryKeys())) {
            List<String> primaryKeys = schemaInfo.getPrimaryKeys().stream()
                    .filter(mainTableFieldToWideField::containsKey)
                    .collect(Collectors.toList());
            if (!primaryKeys.isEmpty()) {
                return primaryKeys;
            }
        }
        return new ArrayList<>(mainTableFieldToWideField.keySet());
    }

    private String buildExistingWideTableWhere(List<String> sourceFields, List<Map<String, Object>> rows) {
        StringJoiner or = new StringJoiner(" OR ");
        for (Map<String, Object> row : rows) {
            StringJoiner and = new StringJoiner(" AND ");
            for (String sourceField : sourceFields) {
                if (!containsKeyIgnoreCase(row, sourceField)) {
                    continue;
                }
                String wideField = mainTableFieldToWideField.get(sourceField);
                if (wideField == null || wideField.isBlank()) {
                    continue;
                }
                Object value = getValueIgnoreCase(row, sourceField);
                if (value == null) {
                    and.add(WideTableDdlGenerator.quoteIdentifier(wideField) + " IS NULL");
                } else {
                    and.add(WideTableDdlGenerator.quoteIdentifier(wideField) + " = " + DuckDbSqlValueFormatter.format(value));
                }
            }
            String andSql = and.toString();
            if (!andSql.isBlank()) {
                or.add("( " + andSql + " )");
            }
        }
        return or.toString();
    }

    private boolean containsKeyIgnoreCase(Map<String, Object> row, String key) {
        if (row == null || key == null) {
            return false;
        }
        if (row.containsKey(key)) {
            return true;
        }
        return row.keySet().stream().anyMatch(k -> key.equalsIgnoreCase(k));
    }

    private Object getValueIgnoreCase(Map<String, Object> row, String key) {
        if (row.containsKey(key)) {
            return row.get(key);
        }
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (key.equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private Map<String, String> resolveMainTableFieldToWideField(String querySql,
                                                                 String mainTableName,
                                                                 List<FromTableConfig> fromTables,
                                                                 Map<String, NodeSchemaInfo> nodeSchemaMap) {
        if (querySql == null || querySql.isBlank()) {
            return Collections.emptyMap();
        }
        try {
            Statement statement = CCJSqlParserUtil.parse(querySql);
            if (!(statement instanceof Select select)) {
                return Collections.emptyMap();
            }
            SelectBody selectBody = select.getSelectBody();
            if (selectBody instanceof SetOperationList setOperationList && !setOperationList.getSelects().isEmpty()) {
                selectBody = setOperationList.getSelects().get(0);
            }
            if (!(selectBody instanceof PlainSelect plainSelect)) {
                return Collections.emptyMap();
            }

            Set<String> mainTableCandidates = resolveMainTableCandidates(mainTableName, fromTables, nodeSchemaMap);
            Set<String> mainAliases = resolveMainAliases(plainSelect, mainTableCandidates);
            if (mainAliases.isEmpty() && mainTableName != null) {
                mainAliases.add(normalize(mainTableName));
            }

            Map<String, String> mapping = new LinkedHashMap<>();
            NodeSchemaInfo mainSchemaInfo = findSchemaInfoByTableName(mainTableName);
            for (SelectItem selectItem : plainSelect.getSelectItems()) {
                if (selectItem instanceof AllTableColumns allTableColumns) {
                    String qualifier = normalize(allTableColumns.getTable().getName());
                    if (mainAliases.contains(qualifier) && mainSchemaInfo != null) {
                        for (String field : mainSchemaInfo.getFieldNames()) {
                            mapping.put(field, field);
                        }
                    }
                    continue;
                }
                if (!(selectItem instanceof SelectExpressionItem expressionItem)) {
                    continue;
                }
                String outputField = resolveOutputFieldName(expressionItem);
                if (outputField == null || outputField.isBlank()) {
                    continue;
                }
                String sourceField = resolveMainSourceField(expressionItem, mainAliases, mainSchemaInfo);
                if (sourceField != null && !sourceField.isBlank()) {
                    mapping.put(sourceField, outputField);
                }
            }
            return mapping;
        } catch (Exception e) {
            logger.warn("Failed to resolve main table field mapping from querySql: {}", e.getMessage());
            return Collections.emptyMap();
        }
    }

    private Set<String> resolveMainTableCandidates(String mainTableName,
                                                   List<FromTableConfig> fromTables,
                                                   Map<String, NodeSchemaInfo> nodeSchemaMap) {
        Set<String> candidates = new LinkedHashSet<>();
        if (mainTableName != null && !mainTableName.isBlank()) {
            candidates.add(normalize(mainTableName));
        }
        NodeSchemaInfo mainSchemaInfo = findSchemaInfoByTableName(mainTableName);
        addSchemaCandidates(candidates, mainSchemaInfo);
        if (fromTables != null) {
            for (int index = 0; index < fromTables.size(); index++) {
                FromTableConfig fromTable = fromTables.get(index);
                if (fromTable == null) {
                    continue;
                }
                NodeSchemaInfo schemaInfo = null;
                if (nodeSchemaMap != null) {
                    schemaInfo = nodeSchemaMap.get(fromTable.getPreNodeId());
                    if (schemaInfo == null) {
                        schemaInfo = findSchemaInfoByTableName(fromTable.getTableNameInSql());
                    }
                }
                boolean matchedMain = mainTableName != null && (
                        mainTableName.equalsIgnoreCase(fromTable.getTableNameInSql())
                                || (schemaInfo != null && (mainTableName.equalsIgnoreCase(schemaInfo.getTableName())
                                || mainTableName.equalsIgnoreCase(schemaInfo.getTargetTableName()))));
                if (matchedMain || (mainTableName == null && index == 0)) {
                    candidates.add(normalize(fromTable.getTableNameInSql()));
                    addSchemaCandidates(candidates, schemaInfo);
                }
            }
        }
        return candidates;
    }

    private void addSchemaCandidates(Set<String> candidates, NodeSchemaInfo schemaInfo) {
        if (schemaInfo == null) {
            return;
        }
        candidates.add(normalize(schemaInfo.getTableName()));
        candidates.add(normalize(schemaInfo.getTargetTableName()));
    }

    private Set<String> resolveMainAliases(PlainSelect plainSelect, Set<String> mainTableCandidates) {
        Set<String> aliases = new LinkedHashSet<>();
        registerMainAlias(plainSelect.getFromItem(), mainTableCandidates, aliases);
        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                registerMainAlias(join.getRightItem(), mainTableCandidates, aliases);
            }
        }
        return aliases;
    }

    private void registerMainAlias(FromItem fromItem, Set<String> mainTableCandidates, Set<String> aliases) {
        if (!(fromItem instanceof Table table)) {
            return;
        }
        String tableName = normalize(table.getName());
        if (!mainTableCandidates.contains(tableName)) {
            return;
        }
        aliases.add(tableName);
        if (table.getAlias() != null && table.getAlias().getName() != null) {
            aliases.add(normalize(table.getAlias().getName()));
        }
    }

    private String resolveOutputFieldName(SelectExpressionItem expressionItem) {
        if (expressionItem.getAlias() != null && expressionItem.getAlias().getName() != null) {
            return expressionItem.getAlias().getName();
        }
        if (expressionItem.getExpression() instanceof Column column) {
            return column.getColumnName();
        }
        return null;
    }

    private String resolveMainSourceField(SelectExpressionItem expressionItem,
                                          Set<String> mainAliases,
                                          NodeSchemaInfo mainSchemaInfo) {
        if (!(expressionItem.getExpression() instanceof Column column)) {
            return null;
        }
        String columnName = column.getColumnName();
        if (column.getTable() == null || column.getTable().getName() == null || column.getTable().getName().isBlank()) {
            if (mainSchemaInfo != null && mainSchemaInfo.getFieldNames().stream().anyMatch(columnName::equalsIgnoreCase)) {
                return columnName;
            }
            return null;
        }
        String qualifier = normalize(column.getTable().getName());
        return mainAliases.contains(qualifier) ? columnName : null;
    }

    private String normalize(String value) {
        if (value == null) {
            return "";
        }
        String stripped = WideTableSourceRegistry.stripQuotes(value);
        return stripped == null ? "" : stripped.toLowerCase(Locale.ROOT);
    }

    /**
     * 受影响主键计算结果容器
     * 
     * <p>包含计算受影响主键过程中的中间结果，避免后续重复计算：</p>
     * <ul>
     *   <li>wideTablePks: 宽表待写入的主键集合</li>
     *   <li>wideTableQueryResults: 宽表待写入结果集（WITH CTE 查询结果）</li>
     *   <li>afterRows: 子表待写入的数据行</li>
     * </ul>
     */
    public static class AffectedKeysResult {
        
        /** 宽表待写入的主键集合 */
        private final List<Map<String, Object>> wideTablePks;
        
        /** 宽表待写入结果集（WITH CTE 查询结果） */
        private final List<Map<String, Object>> wideTableQueryResults;
        
        /** 子表待写入的数据行 */
        private final List<Map<String, Object>> afterRows;
        
        public AffectedKeysResult(List<Map<String, Object>> wideTablePks,
                                  List<Map<String, Object>> wideTableQueryResults,
                                  List<Map<String, Object>> afterRows) {
            this.wideTablePks = wideTablePks;
            this.wideTableQueryResults = wideTableQueryResults;
            this.afterRows = afterRows;
        }
        
        public List<Map<String, Object>> getWideTablePks() {
            return wideTablePks;
        }
        
        public List<Map<String, Object>> getWideTableQueryResults() {
            return wideTableQueryResults;
        }
        
        public List<Map<String, Object>> getAfterRows() {
            return afterRows;
        }
        
        /**
         * 检查是否为空结果
         */
        public boolean isEmpty() {
            return (wideTablePks == null || wideTablePks.isEmpty())
                && (wideTableQueryResults == null || wideTableQueryResults.isEmpty())
                && (afterRows == null || afterRows.isEmpty());
        }
    }
}

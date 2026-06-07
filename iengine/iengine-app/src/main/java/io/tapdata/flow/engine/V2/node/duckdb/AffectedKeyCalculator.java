package io.tapdata.flow.engine.V2.node.duckdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.*;
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

    private static final int MAX_PK_VALUES_PER_QUERY = 1000;

    private final String wideTablePrimaryKey;
    private final String mainTableName;
    private final String mainTablePrimaryKey;
    private final List<FromTableConfig> fromTables;
    private final Map<String, String> customJoinQueries;
    private final DuckDbOperator operator;
    private final WithCteSqlGenerator withCteSqlGenerator;
    private final Map<String, NodeSchemaInfo> nodeSchemaMap;
    private final String resolvedQuerySql;

    /**
     * Full constructor with all required dependencies including NodeSchemaInfo and resolved query SQL.
     *
     * <p>This is the only supported constructor. All dependencies are injected via constructor
     * to ensure immutability and explicit dependency declaration.</p>
     *
     * @param wideTablePrimaryKey Wide table primary key field name
     * @param mainTableName Main table name (alias used in SQL)
     * @param mainTablePrimaryKey Main table primary key field name
     * @param fromTables List of predecessor node configurations
     * @param customJoinQueries Custom JOIN query mappings
     * @param operator DuckDB operator instance
     * @param nodeSchemaMap Predecessor node schema information mapping (preNodeId → NodeSchemaInfo)
     * @param resolvedQuerySql Resolved SQL statement where table aliases have been replaced with actual table names
     * @throws IllegalArgumentException if required parameters are null or blank
     */
    public AffectedKeyCalculator(
            String wideTablePrimaryKey,
            String mainTableName,
            String mainTablePrimaryKey,
            List<FromTableConfig> fromTables,
            Map<String, String> customJoinQueries,
            DuckDbOperator operator,
            Map<String, NodeSchemaInfo> nodeSchemaMap,
            String resolvedQuerySql
    ) {
        if (wideTablePrimaryKey == null || wideTablePrimaryKey.isBlank()) {
            throw new IllegalArgumentException("wideTablePrimaryKey must not be null or blank");
        }

        Objects.requireNonNull(operator, "operator must not be null");
        Objects.requireNonNull(nodeSchemaMap, "nodeSchemaMap must not be null");
        Objects.requireNonNull(resolvedQuerySql, "resolvedQuerySql must not be null");

        if (resolvedQuerySql.isBlank()) {
            throw new IllegalArgumentException("resolvedQuerySql must not be blank");
        }

        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.mainTableName = mainTableName;
        this.mainTablePrimaryKey = mainTablePrimaryKey;
        this.fromTables = fromTables != null ? fromTables : Collections.emptyList();
        this.customJoinQueries = customJoinQueries != null ? customJoinQueries : Collections.emptyMap();
        this.operator = operator;
        this.nodeSchemaMap = nodeSchemaMap;
        this.resolvedQuerySql = resolvedQuerySql;
        this.withCteSqlGenerator = new WithCteSqlGenerator();
    }


    /**
     * 从 SmartMerger 合并结果中提取 after 数据行。
     *
     * <p>Refactored per 2026-06-07 design: now uses {@link SmartMerger.MergedRecord#getAfterRows()}
     * directly instead of calling {@code getFinalState()}.</p>
     */
    private List<Map<String, Object>> extractAfterDataRowsFromEvents(List<SmartMerger.MergedRecord> mergedRecords) {
        List<Map<String, Object>> afterRows = new ArrayList<>();
        for (SmartMerger.MergedRecord record : mergedRecords) {
            afterRows.addAll(record.getAfterRows());
        }
        return afterRows;
    }

    /**
     * 使用 WITH CTE SQL 查询宽表主键
     * @param tableName 子表名
     * @param dataRows 数据行
     * @param fields 字段列表
     * @return 宽表主键集合
     */
    private Set<Object> queryWideTablePksWithCte(String tableName, List<Map<String, Object>> dataRows, List<String> fields) throws SQLException {
        if (dataRows == null || dataRows.isEmpty()) {
            return Collections.emptySet();
        }
        
        // 获取 querySql
        String querySql = getQuerySqlForTable(tableName);
        if (querySql == null) {
            logger.warn("No querySql found for table {}", tableName);
            return Collections.emptySet();
        }
        
        if (withCteSqlGenerator == null) {
            logger.warn("WithCteSqlGenerator not configured for table {}", tableName);
            return Collections.emptySet();
        }
        
        // 生成 WITH CTE SQL
        String withSql = withCteSqlGenerator.generateBatch(querySql, tableName, dataRows, fields);
        
        // 执行查询
        List<Map<String, Object>> results = operator.executeQuery(withSql);
        
        // 提取宽表主键
        Set<Object> wideTablePks = new LinkedHashSet<>();
        if (results != null) {
            for (Map<String, Object> row : results) {
                Object pk = row.get(wideTablePrimaryKey);
                if (pk != null) {
                    wideTablePks.add(pk);
                }
            }
        }
        
        return wideTablePks;
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

    /**
     * 获取表的主键字段名
     */
    private String getTablePrimaryKey(String tableName) {
        if (mainTableName != null && mainTableName.equalsIgnoreCase(tableName)) {
            return mainTablePrimaryKey;
        }
        return getSourceTablePrimaryKey(tableName);
    }

    /**
     * Find NodeSchemaInfo by tableNameInSql.
     *
     * <p>Lookup flow:</p>
     * <ol>
     *   <li>Iterate fromTables to find matching tableNameInSql</li>
     *   <li>Get corresponding preNodeId</li>
     *   <li>Look up NodeSchemaInfo from nodeSchemaMap</li>
     * </ol>
     *
     * @param tableNameInSql Table alias as used in SQL queries
     * @return NodeSchemaInfo if found, null otherwise
     */
    private NodeSchemaInfo findSchemaInfoByTableNameInSql(String tableNameInSql) {
        if (nodeSchemaMap == null || nodeSchemaMap.isEmpty()) {
            return null;
        }

        if (tableNameInSql == null || tableNameInSql.isBlank()) {
            return null;
        }

        for (FromTableConfig config : fromTables) {
            if (config != null &&
                config.getTableNameInSql() != null &&
                config.getTableNameInSql().equalsIgnoreCase(tableNameInSql)) {

                String preNodeId = config.getPreNodeId();

                if (preNodeId != null && !preNodeId.isBlank()) {
                    return nodeSchemaMap.get(preNodeId);
                }
            }
        }

        return null;
    }

    private NodeSchemaInfo findSchemaInfoByTableName(String tableName) {
        if (nodeSchemaMap == null || nodeSchemaMap.isEmpty()) {
            return null;
        }

        if (tableName == null || tableName.isBlank()) {
            return null;
        }

        return nodeSchemaMap.get(tableName);
    }

    /**
     * Get primary key field name for a source table.
     */
    /**
     * Get primary key field name for a source table.
     *
     * <p>Uses three-layer fallback strategy:</p>
     * <ol>
     *   <li>Explicit primary keys from NodeSchemaInfo</li>
     *   <li>Common primary key name detection (id, ID, _id, pk, Id)</li>
     *   <li>Throw explicit exception with available fields</li>
     * </ol>
     *
     * @param tableName Table alias as used in SQL
     * @return Primary key field name
     * @throws IllegalStateException if cannot determine primary key
     */
    private String getSourceTablePrimaryKey(String tableName) {
        NodeSchemaInfo schemaInfo = findSchemaInfoByTableName(tableName);

        if (schemaInfo == null) {
            logger.error("Cannot find NodeSchemaInfo for table={}, cannot determine primary key. " +
                        "Available schemas: {}",
                        tableName,
                        nodeSchemaMap.keySet());
            throw new IllegalStateException(
                "Failed to find schema info for table: " + tableName + ". " +
                "Ensure nodeSchemaMap is properly initialized in HazelcastDuckDbSqlNode.initNodeSchemaCache()");
        }

        List<String> primaryKeys = schemaInfo.getPrimaryKeys();

        if (primaryKeys == null || primaryKeys.isEmpty()) {
            logger.warn("No primary keys defined for table={}, attempting common PK name detection", tableName);

            String[] commonPkNames = {"id", "ID", "_id", "pk", "Id"};

            for (String commonPk : commonPkNames) {
                if (schemaInfo.getFieldMap() != null && schemaInfo.getFieldMap().containsKey(commonPk)) {
                    logger.info("Using fallback primary key '{}' for table {} (no explicit PK defined)",
                               commonPk, tableName);
                    return commonPk;
                }
            }

            throw new IllegalStateException(
                "No primary key found for table: " + tableName + ". " +
                "Available fields: " + schemaInfo.getFieldNames() + ". " +
                "Please define primary keys in the source table schema.");
        }

        String primaryKey = primaryKeys.get(0);

        if (primaryKeys.size() > 1) {
            logger.debug("Table {} has composite primary keys ({}), using first key '{}')",
                        tableName, primaryKeys, primaryKey);
        } else {
            logger.debug("Found single primary key '{}' for table {}", primaryKey, tableName);
        }

        return primaryKey;
    }

    /**
     * Query main table primary keys related to given source table PKs.
     */
    private Set<Object> queryRelatedMainTablePks(String tableName, Set<Object> sourceTablePks) throws SQLException {
        // First check for custom join query (case-insensitive lookup)
        String matchedTableKey = null;
        for (String key : customJoinQueries.keySet()) {
            if (key.equalsIgnoreCase(tableName)) {
                matchedTableKey = key;
                break;
            }
        }
        
        if (matchedTableKey != null) {
            return executeCustomJoinQuery(matchedTableKey, sourceTablePks);
        }

        // TODO: Implement automatic SQL parsing from user's SELECT query
        // For now, use a generic approach that assumes simple JOIN
        logger.error("No custom join query found for table {}, cannot determine affected primary keys", tableName);
        throw new SQLException("No custom join query configured for table: " + tableName);
    }

    /**
     * Execute a custom JOIN query to find related main table PKs.
     */
    private Set<Object> executeCustomJoinQuery(String tableName, Set<Object> sourceTablePks) throws SQLException {
        String queryTemplate = customJoinQueries.get(tableName);
        if (queryTemplate == null) {
            return Collections.emptySet();
        }

        // Using LinkedHashSet to preserve insertion order (for consistent debugging)
        Set<Object> relatedPks = new LinkedHashSet<>();
        
        // Split into batches to avoid too long SQL statements
        List<List<Object>> batches = partitionList(new ArrayList<>(sourceTablePks), MAX_PK_VALUES_PER_QUERY);
        
        for (List<Object> batch : batches) {
            // Replace ${pkValues} placeholder with CSV of PKs
            String pkCsv = batch.stream()
                    .map(pk -> DuckDbSqlValueFormatter.format(pk))
                    .collect(Collectors.joining(","));

            String query = queryTemplate.replace("${pkValues}", pkCsv);
            logger.debug("Executing custom join query (batch size: {}): {}", batch.size(), query);

            List<Map<String, Object>> results = operator.executeQuery(query);

            for (Map<String, Object> row : results) {
                Object pk = row.get(mainTablePrimaryKey);
                if (pk == null) {
                    pk = row.get(wideTablePrimaryKey);
                }
                if (pk != null) {
                    relatedPks.add(pk);
                }
            }
        }

        return relatedPks;
    }

    /**
     * Partition a list into batches of the given maximum size.
     */
    private <T> List<List<T>> partitionList(List<T> list, int size) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            batches.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return batches;
    }



    /**
     * Overloaded: Calculate affected before keys from merged records
     */
    public Set<Object> calculateAffectedBeforeKeys(List<SmartMerger.MergedRecord> mergedRecords,
                                                   String tableName) throws SQLException {
        if (mergedRecords == null) {
            return Collections.emptySet();
        }
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>();
        // 主表优化路径：直接使用 MergedRecord.mainTableBeforePks
        logger.info("Processing main table before merged records from {}", tableName);
        // 检查是否为主表
        if (mainTableName != null && mainTableName.equalsIgnoreCase(tableName)) {
            affectedBeforeKeys.addAll(
                    mergedRecords.stream()
                            .map(SmartMerger.MergedRecord::getMainTableBeforePks)
                            .filter(Objects::nonNull)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet())
            );
            logger.info("Main table before PKs for {}: {}", tableName, affectedBeforeKeys);
        } else {
            // 原有路径
            List<Map<String, Object>> beforeRows = mergedRecords.stream()
                    .map(SmartMerger.MergedRecord::getBeforeRows)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            if (beforeRows.isEmpty()) {
                return affectedBeforeKeys;
            }

            List<String> fields = getTableFields(tableName);
            affectedBeforeKeys.addAll(queryWideTablePksWithCte(tableName, beforeRows, fields));
            logger.info("Main table before PKs for Sub table {}: {}", tableName, affectedBeforeKeys);
        }
        return affectedBeforeKeys;
    }

    /**
     * Overloaded: Calculate affected after keys from merged records
     */
    public Set<Object> calculateAffectedAfterKeys(Iterable<SmartMerger.MergedRecord> mergedRecords)
            throws SQLException {
        if (mergedRecords == null) {
            return Collections.emptySet();
        }

        List<SmartMerger.MergedRecord> mergedList = new ArrayList<>();
        for (SmartMerger.MergedRecord r : mergedRecords) mergedList.add(r);
        if (mergedList.isEmpty()) {
            return Collections.emptySet();
        }

        Map<String, List<SmartMerger.MergedRecord>> recordsByTable = groupMergedRecordsByTable(mergedList);
        Set<Object> affectedAfterKeys = new LinkedHashSet<>();

        for (Map.Entry<String, List<SmartMerger.MergedRecord>> entry : recordsByTable.entrySet()) {
            String tableName = entry.getKey();
            List<SmartMerger.MergedRecord> recordList = entry.getValue();
            if (recordList == null || recordList.isEmpty()) {
                continue;
            }

            // 检查是否为主表
            if (mainTableName != null && mainTableName.equalsIgnoreCase(tableName)) {
                // 主表优化路径：直接使用 MergedRecord.mainTableAfterPks
                logger.debug("Processing main table after merged records from {}, using optimized path", tableName);

                Set<Object> mainTablePks = new LinkedHashSet<>();
                for (SmartMerger.MergedRecord record : recordList) {
                    mainTablePks.addAll(record.getMainTableAfterPks());
                }

                logger.debug("Main table after PKs for {}: {}", tableName, mainTablePks);
                affectedAfterKeys.addAll(mainTablePks);
            } else {
                // 原有路径
                List<Map<String, Object>> afterRows = extractAfterDataRowsFromEvents(recordList);
                if (afterRows.isEmpty()) {
                    continue;
                }

                List<String> fields = getTableFields(tableName);
                affectedAfterKeys.addAll(queryWideTablePksWithCte(tableName, afterRows, fields));
            }
        }

        return affectedAfterKeys;
    }

    private Map<String, List<SmartMerger.MergedRecord>> groupMergedRecordsByTable(
            Iterable<SmartMerger.MergedRecord> mergedRecords) {
        Map<String, List<SmartMerger.MergedRecord>> recordsByTable = new LinkedHashMap<>();
        for (SmartMerger.MergedRecord record : mergedRecords) {
            String tableName = resolveTableName(record);
            if (tableName != null) {
                recordsByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(record);
            }
        }
        return recordsByTable;
    }

    /**
     * Resolve table name from MergedRecord.
     *
     * <p>Refactored per 2026-06-07 design: now uses {@link SmartMerger.MergedRecord#getTableName()}
     * directly instead of traversing operations.</p>
     */
    private String resolveTableName(SmartMerger.MergedRecord record) {
        String tableName = record.getTableName();
        if (tableName != null && !tableName.isEmpty()) {
            return tableName;
        }
        logger.warn("MergedRecord without tableName set, skipping for table grouping");
        return null;
    }

}

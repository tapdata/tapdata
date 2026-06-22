package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.flow.engine.V2.node.duckdb.utils.TablePkUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private final String mainTableName;
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
        this.mainTableName = mainTableName;
        this.operator = operator;
        this.nodeSchemaMap = nodeSchemaMap;
        this.resolvedQuerySql = resolvedQuerySql;
        this.withCteSqlGenerator = new WithCteSqlGenerator();
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
        
        // 获取 querySql
        String querySql = getQuerySqlForTable(tableName);
        if (querySql == null) {
            logger.warn("No querySql found for table {}", tableName);
            return new AffectedKeysResult(new ArrayList<>(), Collections.emptyList(), dataRows);
        }
        
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

        return nodeSchemaMap.get(tableName);
    }

    /**
     * Overloaded: Calculate affected before keys from merged records
     */
    public List<Map<String, Object>> calculateAffectedBeforeKeys(List<SmartMerger.MergedRecord> mergedRecords, String tableName) throws SQLException {
        if (mergedRecords == null) {
            return new ArrayList<>();
        }
        List<Map<String, Object>> affectedBeforeKeys = new ArrayList<>();
        // 主表优化路径：直接使用 MergedRecord.mainTableBeforePks
        logger.info("Processing main table before merged records from {}", tableName);
        // 检查是否为主表
        if (mainTableName != null && mainTableName.equalsIgnoreCase(tableName)) {
            logger.info("Main table before PKs for {}: {}", tableName, affectedBeforeKeys);
            return mergedRecords.stream()
                    .map(SmartMerger.MergedRecord::getMainTableBeforePks)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
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
            AffectedKeysResult result = queryWideTablePksWithCte(tableName, beforeRows, fields);
            logger.info("Main table before PKs for Sub table {}: {}", tableName, affectedBeforeKeys);
            return result.getWideTablePks();
        }
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
        
        // 主表优化路径：直接使用 MergedRecord.mainTableAfterPks
        logger.info("Processing main table after merged records from {}", tableName);
        
        // 收集 afterRows（主表和子表都需要）
        List<Map<String, Object>> afterRows = mergedRecords.stream()
                .map(SmartMerger.MergedRecord::getAfterRows)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        
        // 检查是否为主表
        if (mainTableName != null && mainTableName.equalsIgnoreCase(tableName)) {
            List<Map<String, Object>> affectedKeys = mergedRecords.stream()
                    .map(SmartMerger.MergedRecord::getMainTableAfterPks)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            
            logger.info("Main table after PKs for {}: {}", tableName, affectedKeys);
            
            // 主表：需要通过 WITH CTE 查询获取完整结果
            if (!afterRows.isEmpty()) {
                List<String> fields = getTableFields(tableName);
                AffectedKeysResult result = queryWideTablePksWithCte(tableName, afterRows, fields);
                // 使用主表PK覆盖（更准确）
                return new AffectedKeysResult(affectedKeys, result.getWideTableQueryResults(), afterRows);
            }
            return new AffectedKeysResult(affectedKeys, Collections.emptyList(), afterRows);
        } else {
            // 子表路径：使用 WITH CTE 查询
            if (afterRows.isEmpty()) {
                return new AffectedKeysResult(new ArrayList<>(), Collections.emptyList(), Collections.emptyList());
            }

            List<String> fields = getTableFields(tableName);
            AffectedKeysResult result = queryWideTablePksWithCte(tableName, afterRows, fields);
            logger.info("Main table after PKs for Sub table {}: {}", tableName, result.getWideTablePks());
            return result;
        }
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

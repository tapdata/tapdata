package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.common.TapEventUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
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

    public AffectedKeyCalculator(
            String wideTablePrimaryKey,
            String mainTableName,
            String mainTablePrimaryKey,
            List<FromTableConfig> fromTables,
            Map<String, String> customJoinQueries,
            DuckDbOperator operator
    ) {
        this(wideTablePrimaryKey, mainTableName, mainTablePrimaryKey, fromTables, customJoinQueries, operator, null);
    }

    public AffectedKeyCalculator(
            String wideTablePrimaryKey,
            String mainTableName,
            String mainTablePrimaryKey,
            List<FromTableConfig> fromTables,
            Map<String, String> customJoinQueries,
            DuckDbOperator operator,
            WithCteSqlGenerator withCteSqlGenerator
    ) {
        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.mainTableName = mainTableName;
        this.mainTablePrimaryKey = mainTablePrimaryKey;
        this.fromTables = fromTables != null ? fromTables : Collections.emptyList();
        this.customJoinQueries = customJoinQueries != null ? customJoinQueries : Collections.emptyMap();
        this.operator = operator;
        this.withCteSqlGenerator = withCteSqlGenerator;
    }

    /**
     * Calculate affected wide table primary keys from CDC events.
     *
     * @param tableName The table name that the events came from
     * @param events    The CDC events to process
     * @return Set of affected wide table primary keys
     */
    public Set<Object> calculateAffectedKeys(String tableName, List<Map<String, Object>> events) throws SQLException {
        if (events == null || events.isEmpty()) {
            return Collections.emptySet();
        }

        // Using LinkedHashSet to preserve insertion order (for consistent debugging)
        Set<Object> affectedPks = new LinkedHashSet<>();

        if (mainTableName != null && mainTableName.equalsIgnoreCase(tableName)) {
            // Main table event: directly extract primary keys
            logger.debug("Processing main table events from {}", tableName);
            for (Map<String, Object> event : events) {
                Object pk = extractPrimaryKey(event, mainTablePrimaryKey);
                if (pk != null) {
                    affectedPks.add(pk);
                }
            }
        } else {
            // Check if table is configured as a source table
            boolean isKnownSourceTable = false;
            for (FromTableConfig config : fromTables) {
                if (config.getTableName().equalsIgnoreCase(tableName)) {
                    isKnownSourceTable = true;
                    break;
                }
            }
            // Also check if there is a custom join query for this table
            if (!isKnownSourceTable) {
                for (String key : customJoinQueries.keySet()) {
                    if (key.equalsIgnoreCase(tableName)) {
                        isKnownSourceTable = true;
                        break;
                    }
                }
            }

            if (!isKnownSourceTable) {
                // Unknown table: return empty set
                logger.debug("Unknown table {}, skipping processing", tableName);
                return Collections.emptySet();
            }

            // Secondary table event: find related main table PKs
            logger.debug("Processing secondary table events from {}", tableName);
            // Using LinkedHashSet to preserve insertion order (for consistent debugging)
            Set<Object> sourceTablePks = new LinkedHashSet<>();
            String sourcePkField = getSourceTablePrimaryKey(tableName);

            if (sourcePkField != null) {
                for (Map<String, Object> event : events) {
                    Object pk = extractPrimaryKey(event, sourcePkField);
                    if (pk != null) {
                        sourceTablePks.add(pk);
                    }
                }
            }

            if (!sourceTablePks.isEmpty()) {
                // Query main table PKs related to these source PKs
                Set<Object> relatedMainPks = queryRelatedMainTablePks(tableName, sourceTablePks);
                affectedPks.addAll(relatedMainPks);
            }
        }

        logger.debug("Calculated {} affected wide table PKs for {} events from {}",
                affectedPks.size(), events.size(), tableName);

        return affectedPks;
    }

    /**
     * Extract primary key value from an event.
     * @deprecated Use extractBeforePrimaryKey or extractAfterPrimaryKey instead
     */
    @Deprecated
    private Object extractPrimaryKey(Map<String, Object> event, String pkField) {
        // Try direct field access first
        Object pk = event.get(pkField);
        if (pk != null) {
            return pk;
        }

        // Try from "after" field (for INSERT/UPDATE)
        Object after = event.get("after");
        if (after instanceof Map) {
            pk = ((Map<?, ?>) after).get(pkField);
            if (pk != null) {
                return pk;
            }
        }

        // Try from "before" field (for DELETE/UPDATE)
        Object before = event.get("before");
        if (before instanceof Map) {
            pk = ((Map<?, ?>) before).get(pkField);
            if (pk != null) {
                return pk;
            }
        }

        // Try from "o2" or "o" fields (for MongoDB style updates/deletes)
        Object o2 = event.get("o2");
        if (o2 instanceof Map) {
            pk = ((Map<?, ?>) o2).get(pkField);
            if (pk != null) {
                return pk;
            }
        }
        Object o = event.get("o");
        if (o instanceof Map) {
            pk = ((Map<?, ?>) o).get(pkField);
            if (pk != null) {
                return pk;
            }
        }

        logger.debug("Failed to extract primary key '{}' from event (tried direct, after, before, o2, o)", pkField);
        return null;
    }

    /**
     * 从 CDC 事件提取 before 主键（宽表改动前）
     * @param event CDC 事件
     * @param pkField 主键字段名
     * @return Optional 包含 before 主键值，如果不存在则返回 empty
     */
    public Optional<Object> extractBeforePrimaryKey(Map<String, Object> event, String pkField) {
        // 1. 优先从 before 字段提取（DELETE/UPDATE 事件）
        Object before = event.get("before");
        if (before instanceof Map) {
            Object pk = ((Map<?, ?>) before).get(pkField);
            if (pk != null) {
                return Optional.of(pk);
            }
        }
        
        // 2. 回退到顶层直接访问（某些 CDC 格式）
        Object pk = event.get(pkField);
        if (pk != null) {
            return Optional.of(pk);
        }
        
        // 3. MongoDB 风格 o2/o 字段
        Object o2 = event.get("o2");
        if (o2 instanceof Map) {
            Object pk2 = ((Map<?, ?>) o2).get(pkField);
            if (pk2 != null) {
                return Optional.of(pk2);
            }
        }
        
        Object o = event.get("o");
        if (o instanceof Map) {
            Object pk3 = ((Map<?, ?>) o).get(pkField);
            if (pk3 != null) {
                return Optional.of(pk3);
            }
        }
        
        return Optional.empty();
    }

    /**
     * 从 CDC 事件提取 after 主键（宽表改动后）
     * @param event CDC 事件
     * @param pkField 主键字段名
     * @return Optional 包含 after 主键值，如果不存在则返回 empty
     */
    public Optional<Object> extractAfterPrimaryKey(Map<String, Object> event, String pkField) {
        // 1. 优先从 after 字段提取（INSERT/UPDATE 事件）
        Object after = event.get("after");
        if (after instanceof Map) {
            Object pk = ((Map<?, ?>) after).get(pkField);
            if (pk != null) {
                return Optional.of(pk);
            }
        }
        
        // 2. 回退到顶层直接访问
        Object pk = event.get(pkField);
        if (pk != null) {
            return Optional.of(pk);
        }
        
        return Optional.empty();
    }

    /**
     * 检测主键是否更新
     * @param event CDC 事件
     * @param pkField 主键字段名
     * @return true 表示主键更新（beforePk ≠ afterPk）
     */
    public boolean isPrimaryKeyUpdated(Map<String, Object> event, String pkField) {
        Optional<Object> beforePk = extractBeforePrimaryKey(event, pkField);
        Optional<Object> afterPk = extractAfterPrimaryKey(event, pkField);
        
        return beforePk.isPresent() && afterPk.isPresent() 
                && !Objects.equals(beforePk.get(), afterPk.get());
    }

    /**
     * 批量计算所有事件的 before 受影响主键集合
     * 使用 SmartMerger 合并事件，提取所有历史状态的 before 数据，拼接 WITH SQL 查询宽表主键
     * 注意：需要在调用 SmartMerger 之前先收集 DELETE 事件的 before 数据，因为 SmartMerger 会移除 DELETE 记录
     * @param eventsByTable 按表名分组的 CDC 事件
     * @return 所有 before 主键集合（用于 DELETE 宽表记录）
     */
    /**
     * 批量计算所有事件的before受影响主键集合
     * 从TapdataEvent中提取数据进行计算
     * 
     * @param events TapdataEvent列表
     * @return 所有before主键集合
     */
    public Set<Object> calculateAffectedBeforeKeys(List<TapdataEvent> events) throws SQLException {
        if (events == null || events.isEmpty()) {
            return Collections.emptySet();
        }
        
        // 按表名分组并转换为Map格式
        Map<String, List<Map<String, Object>>> eventsByTable = new LinkedHashMap<>();
        for (TapdataEvent event : events) {
            TapEvent tapEvent = event.getTapEvent();
            if (tapEvent instanceof TapRecordEvent) {
                TapRecordEvent recordEvent = (TapRecordEvent) tapEvent;
                String tableName = TapEventUtil.getTableId(recordEvent);
                if (tableName == null) continue;
                
                Map<String, Object> mapEvent = convertTapdataEventToMap(recordEvent);
                eventsByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(mapEvent);
            }
        }
        
        // 复用原有逻辑
        return calculateAffectedBeforeKeysInternal(eventsByTable);
    }

    /**
     * 批量计算所有事件的after受影响主键集合
     * 从TapdataEvent中提取数据进行计算
     * 
     * @param events TapdataEvent列表
     * @return 所有after主键集合
     */
    public Set<Object> calculateAffectedAfterKeys(List<TapdataEvent> events) throws SQLException {
        if (events == null || events.isEmpty()) {
            return Collections.emptySet();
        }
        
        // 按表名分组并转换为Map格式
        Map<String, List<Map<String, Object>>> eventsByTable = new LinkedHashMap<>();
        for (TapdataEvent event : events) {
            TapEvent tapEvent = event.getTapEvent();
            if (tapEvent instanceof TapRecordEvent) {
                TapRecordEvent recordEvent = (TapRecordEvent) tapEvent;
                String tableName = TapEventUtil.getTableId(recordEvent);
                if (tableName == null) continue;
                
                Map<String, Object> mapEvent = convertTapdataEventToMap(recordEvent);
                eventsByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(mapEvent);
            }
        }
        
        // 复用原有逻辑
        return calculateAffectedAfterKeysInternal(eventsByTable);
    }

    /**
     * 将TapRecordEvent转换为Map格式
     * 
     * @param recordEvent 记录事件
     * @return Map格式的事件数据
     */
    private Map<String, Object> convertTapdataEventToMap(TapRecordEvent recordEvent) {
        Map<String, Object> mapEvent = new HashMap<>();
        
        if (recordEvent instanceof TapInsertRecordEvent) {
            mapEvent.put("op", "INSERT");
            mapEvent.put("value", TapEventUtil.getAfter(recordEvent));
        } else if (recordEvent instanceof TapUpdateRecordEvent) {
            mapEvent.put("op", "UPDATE");
            mapEvent.put("value", TapEventUtil.getAfter(recordEvent));
            mapEvent.put("old_value", TapEventUtil.getBefore(recordEvent));
        } else if (recordEvent instanceof TapDeleteRecordEvent) {
            mapEvent.put("op", "DELETE");
            mapEvent.put("value", TapEventUtil.getBefore(recordEvent));
        }
        
        return mapEvent;
    }

    /**
     * 内部方法：批量计算before受影响主键集合（复用原有逻辑）
     */
    private Set<Object> calculateAffectedBeforeKeysInternal(Map<String, List<Map<String, Object>>> eventsByTable) throws SQLException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>();
        
        for (Map.Entry<String, List<Map<String, Object>>> entry : eventsByTable.entrySet()) {
            String tableName = entry.getKey();
            List<Map<String, Object>> events = entry.getValue();
            
            if (events == null || events.isEmpty()) {
                continue;
            }
            
            // 先收集 DELETE 事件的 before 数据（在 SmartMerger 移除记录之前）
            List<Map<String, Object>> deleteBeforeRows = extractDeleteBeforeRows(events, tableName);
            
            // 使用 SmartMerger 合并事件
            List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events);
            
            // 提取所有 before 数据行（不包括 DELETE，因为已经提前收集了）
            List<String> fields = getTableFields(tableName);
            List<Map<String, Object>> beforeRows = extractBeforeDataRows(mergedRecords, tableName);
            
            // 合并 DELETE 的 before 数据
            beforeRows.addAll(deleteBeforeRows);
            
            if (beforeRows.isEmpty()) {
                continue;
            }
            
            // 使用 WITH CTE SQL 查询宽表主键
            Set<Object> wideTablePks = queryWideTablePksWithCte(tableName, beforeRows, fields);
            affectedBeforeKeys.addAll(wideTablePks);
        }
        
        return affectedBeforeKeys;
    }

    /**
     * 从原始事件中提取 DELETE 事件的 before 数据
     * 在 SmartMerger 处理之前调用，因为 SmartMerger 会移除 DELETE 记录
     */
    private List<Map<String, Object>> extractDeleteBeforeRows(List<Map<String, Object>> events, String tableName) {
        List<Map<String, Object>> deleteBeforeRows = new ArrayList<>();
        
        for (Map<String, Object> event : events) {
            String opType = (String) event.get("op");
            if ("DELETE".equals(opType)) {
                // DELETE 事件的 before 数据从 value 或 o/o2 中提取
                Object value = event.get("value");
                if (value instanceof Map) {
                    deleteBeforeRows.add(new HashMap<>((Map<String, Object>) value));
                } else {
                    // 尝试从 o 或 o2 中提取
                    Object filter = event.get("o");
                    if (filter == null) {
                        filter = event.get("o2");
                    }
                    if (filter instanceof Map) {
                        deleteBeforeRows.add(new HashMap<>((Map<String, Object>) filter));
                    }
                }
            }
        }
        
        return deleteBeforeRows;
    }

    /**
     * 从 SmartMerger 合并结果中提取所有 before 数据行
     * 收集 operations 中所有操作的 before 状态，保证历史错误数据全被删除
     * 注意：INSERT 事件只有在后面还有操作时才需要收集 before 数据
     * 对于 JOIN KEY 多次更新场景，需要收集所有中间状态
     */
    private List<Map<String, Object>> extractBeforeDataRows(List<SmartMerger.MergedRecord> mergedRecords, String tableName) {
        List<Map<String, Object>> beforeRows = new ArrayList<>();
        String pkField = getSourceTablePrimaryKey(tableName);
        
        for (SmartMerger.MergedRecord record : mergedRecords) {
            List<Map<String, Object>> operations = record.getOperations();
            if (operations.isEmpty()) {
                continue;
            }
            
            int opCount = operations.size();
            // 收集所有操作的 before 状态
            for (int i = 0; i < opCount; i++) {
                Map<String, Object> op = operations.get(i);
                String opType = (String) op.get("op");
                boolean isLastOp = (i == opCount - 1);
                
                if ("INSERT".equals(opType)) {
                    // INSERT 事件：只有后面还有操作时才需要收集 before 数据
                    if (!isLastOp) {
                        Object value = op.get("value");
                        if (value instanceof Map) {
                            beforeRows.add(new HashMap<>((Map<String, Object>) value));
                        }
                    }
                } else if ("UPDATE".equals(opType)) {
                    // UPDATE 事件的 before 数据
                    Object oldPk = op.get("old_pk");
                    Map<String, Object> beforeRow = new HashMap<>();
                    if (oldPk != null) {
                        // 有主键变更，使用 old_pk
                        beforeRow.put(pkField, oldPk);
                    } else {
                        // 没有主键变更，使用当前主键
                        beforeRow.put(pkField, record.getCurrentPk());
                    }
                    // 复制其他字段（如果有）
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fields = (Map<String, Object>) op.get("fields");
                    if (fields != null) {
                        beforeRow.putAll(fields);
                    }
                    beforeRows.add(beforeRow);
                }
                // DELETE 操作不在这里处理，DELETE 的 before 数据由 finalState 提供
            }
            
            // 如果最后一个操作是 DELETE，需要添加 finalState 作为 before 数据
            Map<String, Object> lastOp = operations.get(opCount - 1);
            String lastOpType = (String) lastOp.get("op");
            if ("DELETE".equals(lastOpType)) {
                Map<String, Object> beforeRow = new HashMap<>(record.getFinalState());
                if (!beforeRow.isEmpty()) {
                    beforeRows.add(beforeRow);
                }
            }
        }
        
        return beforeRows;
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
        for (Map<String, Object> row : results) {
            Object pk = row.get(wideTablePrimaryKey);
            if (pk != null) {
                wideTablePks.add(pk);
            }
        }
        
        return wideTablePks;
    }

    /**
     * 获取表对应的 querySql
     * 从 fromTables 中查找匹配的表，返回其 querySql
     */
    private String getQuerySqlForTable(String tableName) {
        // 遍历 fromTables 查找匹配的表
        for (FromTableConfig config : fromTables) {
            if (config.getTableName().equalsIgnoreCase(tableName)) {
                return config.getQuerySql();
            }
        }
        return null;
    }

    /**
     * 获取表的字段列表
     * 从 FromTableConfig 中提取字段，如果没有配置则返回主键字段
     */
    private List<String> getTableFields(String tableName) {
        for (FromTableConfig config : fromTables) {
            if (config.getTableName().equalsIgnoreCase(tableName)) {
                List<String> fields = config.getFields();
                if (fields != null && !fields.isEmpty()) {
                    return fields;
                }
            }
        }
        // 回退：返回主键字段
        return Collections.singletonList(getSourceTablePrimaryKey(tableName));
    }

    /**
     * 批量计算所有事件的 after 受影响主键集合
     * 使用 SmartMerger 合并事件，提取 finalState 数据，拼接 WITH SQL 查询宽表主键
     * @param eventsByTable 按表名分组的 CDC 事件
     * @return 所有 after 主键集合（用于 INSERT/UPDATE 宽表记录）
     */
    /**
     * 内部方法：批量计算after受影响主键集合（复用原有逻辑）
     */
    private Set<Object> calculateAffectedAfterKeysInternal(Map<String, List<Map<String, Object>>> eventsByTable) throws SQLException {
        Set<Object> affectedAfterKeys = new LinkedHashSet<>();
        
        for (Map.Entry<String, List<Map<String, Object>>> entry : eventsByTable.entrySet()) {
            String tableName = entry.getKey();
            List<Map<String, Object>> events = entry.getValue();
            
            if (events == null || events.isEmpty()) {
                continue;
            }
            
            // 使用 SmartMerger 合并事件
            List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(events);
            if (mergedRecords.isEmpty()) {
                continue;
            }
            
            // 提取所有 after 数据行（finalState）
            List<String> fields = getTableFields(tableName);
            List<Map<String, Object>> afterRows = extractAfterDataRows(mergedRecords);
            
            if (afterRows.isEmpty()) {
                continue;
            }
            
            // 使用 WITH CTE SQL 查询宽表主键
            Set<Object> wideTablePks = queryWideTablePksWithCte(tableName, afterRows, fields);
            affectedAfterKeys.addAll(wideTablePks);
        }
        
        return affectedAfterKeys;
    }

    /**
     * 从 SmartMerger 合并结果中提取所有 after 数据行
     * 只收集 finalState 数据，保证最终正确数据被插入
     */
    private List<Map<String, Object>> extractAfterDataRows(List<SmartMerger.MergedRecord> mergedRecords) {
        List<Map<String, Object>> afterRows = new ArrayList<>();
        
        for (SmartMerger.MergedRecord record : mergedRecords) {
            Map<String, Object> finalState = record.getFinalState();
            if (finalState != null && !finalState.isEmpty()) {
                afterRows.add(new HashMap<>(finalState));
            }
        }
        
        return afterRows;
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
     * Get primary key field name for a source table.
     */
    private String getSourceTablePrimaryKey(String tableName) {
        for (FromTableConfig config : fromTables) {
            if (config.getTableName().equalsIgnoreCase(tableName)) {
                return config.getPrimaryKey();
            }
        }
        // Default: try common PK field name "id"
        logger.warn("No primary key configured for table {}, using default 'id'", tableName);
        return "id";
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
                    .map(pk -> {
                        if (pk instanceof String) {
                            return "'" + pk.toString().replace("'", "''") + "'";
                        }
                        return pk.toString();
                    })
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

    public String getWideTablePrimaryKey() {
        return wideTablePrimaryKey;
    }

    public String getMainTableName() {
        return mainTableName;
    }

    public String getMainTablePrimaryKey() {
        return mainTablePrimaryKey;
    }
}

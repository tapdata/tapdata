package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
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
    public Set<Object> calculateAffectedKeysFromEvents(String tableName, List<TapdataEvent> events) throws SQLException {
        if (events == null || events.isEmpty()) {
            return Collections.emptySet();
        }

        // Using LinkedHashSet to preserve insertion order (for consistent debugging)
        Set<Object> affectedPks = new LinkedHashSet<>();

        if (mainTableName != null && mainTableName.equalsIgnoreCase(tableName)) {
            // Main table event: directly extract primary keys
            logger.debug("Processing main table events from {}", tableName);
            for (TapdataEvent event : events) {
                TapEvent tapEvent = event.getTapEvent();
                if (tapEvent instanceof TapRecordEvent) {
                    TapRecordEvent recordEvent = (TapRecordEvent) tapEvent;
                    Object pk = extractPrimaryKey(recordEvent, mainTablePrimaryKey);
                    if (pk != null) {
                        affectedPks.add(pk);
                    }
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
                for (TapdataEvent event : events) {
                    TapEvent tapEvent = event.getTapEvent();
                    if (tapEvent instanceof TapRecordEvent) {
                        TapRecordEvent recordEvent = (TapRecordEvent) tapEvent;
                        Object pk = extractPrimaryKey(recordEvent, sourcePkField);
                        if (pk != null) {
                            sourceTablePks.add(pk);
                        }
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
     * Extract primary key value from a TapRecordEvent.
     */
    private Object extractPrimaryKey(TapRecordEvent recordEvent, String pkField) {
        // Try after field first
        Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
        if (after != null) {
            Object pk = after.get(pkField);
            if (pk != null) {
                return pk;
            }
        }

        // Try before field
        Map<String, Object> before = TapEventUtil.getBefore(recordEvent);
        if (before != null) {
            Object pk = before.get(pkField);
            if (pk != null) {
                return pk;
            }
        }

        logger.debug("Failed to extract primary key '{}' from event", pkField);
        return null;
    }

    /**
     * 从 TapRecordEvent 提取 before 主键（宽表改动前）
     * @param recordEvent 记录事件
     * @param pkField 主键字段名
     * @return Optional 包含 before 主键值，如果不存在则返回 empty
     */
    public Optional<Object> extractBeforePrimaryKey(TapRecordEvent recordEvent, String pkField) {
        Map<String, Object> before = TapEventUtil.getBefore(recordEvent);
        if (before != null) {
            Object pk = before.get(pkField);
            if (pk != null) {
                return Optional.of(pk);
            }
        }
        
        return Optional.empty();
    }

    /**
     * 从 TapRecordEvent 提取 after 主键（宽表改动后）
     * @param recordEvent 记录事件
     * @param pkField 主键字段名
     * @return Optional 包含 after 主键值，如果不存在则返回 empty
     */
    public Optional<Object> extractAfterPrimaryKey(TapRecordEvent recordEvent, String pkField) {
        Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
        if (after != null) {
            Object pk = after.get(pkField);
            if (pk != null) {
                return Optional.of(pk);
            }
        }
        
        return Optional.empty();
    }

    /**
     * 检测主键是否更新
     * @param recordEvent 记录事件
     * @param pkField 主键字段名
     * @return true 表示主键更新（beforePk ≠ afterPk）
     */
    public boolean isPrimaryKeyUpdated(TapRecordEvent recordEvent, String pkField) {
        Optional<Object> beforePk = extractBeforePrimaryKey(recordEvent, pkField);
        Optional<Object> afterPk = extractAfterPrimaryKey(recordEvent, pkField);
        
        return beforePk.isPresent() && afterPk.isPresent() 
                && !Objects.equals(beforePk.get(), afterPk.get());
    }

    /**
     * 批量计算所有事件的before受影响主键集合
     * 使用 SmartMerger 合并事件，提取所有历史状态的 before 数据，拼接 WITH SQL 查询宽表主键
     *
     * @param events    TapdataEvent列表
     * @return 所有before主键集合
     */
    public Set<Object> calculateAffectedBeforeKeys(List<TapdataEvent> events) throws SQLException {
        return calculateAffectedBeforeKeys(events, null);
    }
    
    /**
     * 批量计算所有事件的before受影响主键集合
     * 使用 SmartMerger 合并事件，提取所有历史状态的 before 数据，拼接 WITH SQL 查询宽表主键
     *
     * @param events    TapdataEvent列表
     * @param sourceTableName 可选的单表名，如果提供则只处理该表的事件，否则按事件中的表名分组处理
     * @return 所有before主键集合
     */
    public Set<Object> calculateAffectedBeforeKeys(List<TapdataEvent> events, String sourceTableName) throws SQLException {
        if (events == null || events.isEmpty()) {
            return Collections.emptySet();
        }
        
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>();

        // 是单表处理还是多表处理：
        Map<String, List<TapdataEvent>> eventsByTable = new LinkedHashMap<>();
        if (sourceTableName != null) {
            eventsByTable.put(sourceTableName, events);
        } else {
            // 按表名分组
            for (TapdataEvent tapEvent : events) {
                TapEvent tapEventInner = tapEvent.getTapEvent();
                if (tapEventInner instanceof TapRecordEvent recordEvent) {
                    String everyTableName = TapEventUtil.getTableId(recordEvent);
                    if (everyTableName != null) {
                        eventsByTable.computeIfAbsent(everyTableName, k -> new ArrayList<>()).add(tapEvent);
                    }
                }
            }
        }

        for (Map.Entry<String, List<TapdataEvent>> entry : eventsByTable.entrySet()) {
            String tableName = entry.getKey();
            List<TapdataEvent> eventsList = entry.getValue();
            
            if (eventsList == null || eventsList.isEmpty()) {
                continue;
            }
            
            // 1. 先收集 DELETE 事件的 before 数据（在 SmartMerger 移除记录之前）
            List<Map<String, Object>> deleteBeforeRows = extractDeleteBeforeRowsFromEvents(eventsList);
            
            // 2. 使用 SmartMerger 合并事件
            List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(eventsList);
            
            // 3. 提取所有 before 数据行（不包括 DELETE，因为已经提前收集了）
            List<String> fields = getTableFields(tableName);
            List<Map<String, Object>> beforeRows = extractBeforeDataRowsFromEvents(mergedRecords, tableName);
            
            // 4. 合并 DELETE 的 before 数据
            beforeRows.addAll(deleteBeforeRows);
            
            if (beforeRows.isEmpty()) {
                continue;
            }
            
            // 5. 使用 WITH CTE SQL 查询宽表主键
            Set<Object> wideTablePks = queryWideTablePksWithCte(tableName, beforeRows, fields);
            affectedBeforeKeys.addAll(wideTablePks);
        }
        
        return affectedBeforeKeys;
    }

    /**
     * 批量计算所有事件的after受影响主键集合
     * 使用 SmartMerger 合并事件，提取 finalState 数据，拼接 WITH SQL 查询宽表主键
     * 
     * @param events TapdataEvent列表
     * @return 所有after主键集合
     */
    public Set<Object> calculateAffectedAfterKeys(List<TapdataEvent> events) throws SQLException {
        if (events == null || events.isEmpty()) {
            return Collections.emptySet();
        }
        
        Set<Object> affectedAfterKeys = new LinkedHashSet<>();
        
        // 按表名分组
        Map<String, List<TapdataEvent>> eventsByTable = new LinkedHashMap<>();
        for (TapdataEvent tapEvent : events) {
            TapEvent tapEventInner = tapEvent.getTapEvent();
            if (tapEventInner instanceof TapRecordEvent) {
                TapRecordEvent recordEvent = (TapRecordEvent) tapEventInner;
                String tableName = TapEventUtil.getTableId(recordEvent);
                if (tableName != null) {
                    eventsByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(tapEvent);
                }
            }
        }
        
        for (Map.Entry<String, List<TapdataEvent>> entry : eventsByTable.entrySet()) {
            String tableName = entry.getKey();
            List<TapdataEvent> eventsList = entry.getValue();
            
            if (eventsList == null || eventsList.isEmpty()) {
                continue;
            }
            
            // 1. 使用 SmartMerger 合并事件
            List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(eventsList);
            if (mergedRecords.isEmpty()) {
                continue;
            }
            
            // 2. 提取所有 after 数据行（finalState）
            List<String> fields = getTableFields(tableName);
            List<Map<String, Object>> afterRows = extractAfterDataRowsFromEvents(mergedRecords);
            
            if (afterRows.isEmpty()) {
                continue;
            }
            
            // 3. 使用 WITH CTE SQL 查询宽表主键
            Set<Object> wideTablePks = queryWideTablePksWithCte(tableName, afterRows, fields);
            affectedAfterKeys.addAll(wideTablePks);
        }
        
        return affectedAfterKeys;
    }

    /**
     * 从 TapdataEvent 列表中提取 DELETE 事件的 before 数据
     */
    private List<Map<String, Object>> extractDeleteBeforeRowsFromEvents(List<TapdataEvent> events) {
        List<Map<String, Object>> deleteBeforeRows = new ArrayList<>();
        
        for (TapdataEvent tapEvent : events) {
            TapEvent tapEventInner = tapEvent.getTapEvent();
            if (tapEventInner instanceof TapDeleteRecordEvent) {
                TapDeleteRecordEvent deleteEvent = (TapDeleteRecordEvent) tapEventInner;
                Map<String, Object> before = TapEventUtil.getBefore(deleteEvent);
                if (before != null) {
                    deleteBeforeRows.add(new HashMap<>(before));
                }
            }
        }
        
        return deleteBeforeRows;
    }

    /**
     * 从 SmartMerger 合并结果中提取 before 数据行
     */
    private List<Map<String, Object>> extractBeforeDataRowsFromEvents(List<SmartMerger.MergedRecord> mergedRecords, String tableName) {
        List<Map<String, Object>> beforeRows = new ArrayList<>();
        String pkField = getSourceTablePrimaryKey(tableName);
        
        for (SmartMerger.MergedRecord record : mergedRecords) {
            List<TapdataEvent> operations = record.getOperations();
            if (operations.isEmpty()) {
                continue;
            }
            
            int opCount = operations.size();
            // 收集所有操作的 before 状态
            for (int i = 0; i < opCount; i++) {
                TapdataEvent tapEvent = operations.get(i);
                TapEvent tapEventInner = tapEvent.getTapEvent();
                
                if (!(tapEventInner instanceof TapRecordEvent recordEvent)) {
                    continue;
                }
                
                boolean isLastOp = (i == opCount - 1);
                
                if (tapEventInner instanceof TapInsertRecordEvent) {
                    // INSERT 事件：只有后面还有操作时才需要收集 before 数据
                    if (!isLastOp) {
                        Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
                        if (after != null) {
                            beforeRows.add(new HashMap<>(after));
                        }
                    }
                } else if (tapEventInner instanceof TapUpdateRecordEvent) {
                    // UPDATE 事件的 before 数据
                    Map<String, Object> beforeRow = new HashMap<>();
                    
                    // 首先尝试从 before 字段获取完整数据
                    Map<String, Object> beforeData = TapEventUtil.getBefore(recordEvent);
                    if (beforeData != null) {
                        beforeRow.putAll(beforeData);
                    }
                    
                    // 如果没有 before 数据，尝试构建
                    if (beforeRow.isEmpty()) {
                        // 使用 record 信息构建
                        beforeRow.put(pkField, record.getInitialPk());
                    }
                    
                    beforeRows.add(beforeRow);
                }
                // DELETE 操作不在这里处理
            }
            
            // 如果最后一个操作是 DELETE，需要添加 finalState 作为 before 数据
            if (opCount > 0) {
                TapdataEvent lastEvent = operations.get(opCount - 1);
                TapEvent lastEventInner = lastEvent.getTapEvent();
                
                if (lastEventInner instanceof TapDeleteRecordEvent) {
                    Map<String, Object> beforeRow = new HashMap<>(record.getFinalState());
                    if (!beforeRow.isEmpty()) {
                        beforeRows.add(beforeRow);
                    }
                }
            }
        }
        
        return beforeRows;
    }

    /**
     * 从 SmartMerger 合并结果中提取 after 数据行
     */
    private List<Map<String, Object>> extractAfterDataRowsFromEvents(List<SmartMerger.MergedRecord> mergedRecords) {
        List<Map<String, Object>> afterRows = new ArrayList<>();
        
        for (SmartMerger.MergedRecord record : mergedRecords) {
            Map<String, Object> finalState = record.getFinalState();
            if (finalState != null && !finalState.isEmpty()) {
                afterRows.add(finalState);
            }
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
    
    // ==================== 兼容旧 API 的方法 ====================
    
    /**
     * 兼容旧 API：从 Map 格式事件计算受影响主键
     */
    public Set<Object> calculateAffectedKeys(String tableName, List<Map<String, Object>> events) throws SQLException {
        if (events == null || events.isEmpty()) {
            return Collections.emptySet();
        }
        
        // 简单的实现：直接从 Map 中提取主键
        Set<Object> affectedPks = new LinkedHashSet<>();
        
        if (mainTableName != null && mainTableName.equalsIgnoreCase(tableName)) {
            for (Map<String, Object> event : events) {
                Object pk = extractPrimaryKeyFromMap(event, mainTablePrimaryKey);
                if (pk != null) {
                    affectedPks.add(pk);
                }
            }
        } else {
            String sourcePkField = getSourceTablePrimaryKey(tableName);
            if (sourcePkField != null) {
                Set<Object> sourcePks = new LinkedHashSet<>();
                for (Map<String, Object> event : events) {
                    Object pk = extractPrimaryKeyFromMap(event, sourcePkField);
                    if (pk != null) {
                        sourcePks.add(pk);
                    }
                }
                if (!sourcePks.isEmpty()) {
                    affectedPks.addAll(queryRelatedMainTablePks(tableName, sourcePks));
                }
            }
        }
        
        return affectedPks;
    }
    
    /**
     * 从 Map 格式事件中提取主键
     */
    private Object extractPrimaryKeyFromMap(Map<String, Object> event, String pkField) {
        if (event == null) {
            return null;
        }
        
        // 优先尝试 after 字段
        Object after = event.get("after");
        if (after instanceof Map) {
            Object pk = ((Map<String, Object>) after).get(pkField);
            if (pk != null) {
                return pk;
            }
        }
        
        // 然后尝试直接从顶级查找
        Object pk = event.get(pkField);
        if (pk != null) {
            return pk;
        }
        
        // 尝试 before 字段
        Object before = event.get("before");
        if (before instanceof Map) {
            pk = ((Map<String, Object>) before).get(pkField);
            if (pk != null) {
                return pk;
            }
        }
        
        // 尝试 o2 字段（MongoDB 风格）
        Object o2 = event.get("o2");
        if (o2 instanceof Map) {
            pk = ((Map<String, Object>) o2).get(pkField);
            if (pk != null) {
                return pk;
            }
        }
        
        // 尝试 o 字段（MongoDB 风格）
        Object o = event.get("o");
        if (o instanceof Map) {
            pk = ((Map<String, Object>) o).get(pkField);
            if (pk != null) {
                return pk;
            }
        }
        
        return null;
    }
    
    /**
     * 兼容旧 API：从 Map 格式事件中提取 before 主键
     * @deprecated 使用 extractBeforePrimaryKey(TapRecordEvent, String) 替代
     */
    @Deprecated
    public Optional<Object> extractBeforePrimaryKey(Map<String, Object> event, String pkField) {
        Object pk = extractPrimaryKeyFromBeforeMap(event, pkField);
        return Optional.ofNullable(pk);
    }
    
    /**
     * 从 Map 格式事件的 before 部分提取主键
     */
    private Object extractPrimaryKeyFromBeforeMap(Map<String, Object> event, String pkField) {
        if (event == null) {
            return null;
        }
        
        // 尝试 before 字段
        Object before = event.get("before");
        if (before instanceof Map) {
            Object pk = ((Map<String, Object>) before).get(pkField);
            if (pk != null) {
                return pk;
            }
        }
        
        // 尝试 o2 字段（MongoDB 风格）
        Object o2 = event.get("o2");
        if (o2 instanceof Map) {
            Object pk = ((Map<String, Object>) o2).get(pkField);
            if (pk != null) {
                return pk;
            }
        }
        
        // 尝试 o 字段（MongoDB 风格）
        Object o = event.get("o");
        if (o instanceof Map) {
            Object pk = ((Map<String, Object>) o).get(pkField);
            if (pk != null) {
                return pk;
            }
        }
        
        // 尝试顶级
        return event.get(pkField);
    }
    
    /**
     * 兼容旧 API：从 Map 格式事件中提取 after 主键
     * @deprecated 使用 extractAfterPrimaryKey(TapRecordEvent, String) 替代
     */
    @Deprecated
    public Optional<Object> extractAfterPrimaryKey(Map<String, Object> event, String pkField) {
        Object pk = extractPrimaryKeyFromAfterMap(event, pkField);
        return Optional.ofNullable(pk);
    }
    
    /**
     * 从 Map 格式事件的 after 部分提取主键
     */
    private Object extractPrimaryKeyFromAfterMap(Map<String, Object> event, String pkField) {
        if (event == null) {
            return null;
        }
        
        // 优先尝试 after 字段
        Object after = event.get("after");
        if (after instanceof Map) {
            Object pk = ((Map<String, Object>) after).get(pkField);
            if (pk != null) {
                return pk;
            }
        }
        
        // 然后尝试直接从顶级查找
        return event.get(pkField);
    }
    
    /**
     * 兼容旧 API：检测 Map 格式事件中的主键是否更新
     * @deprecated 使用 isPrimaryKeyUpdated(TapRecordEvent, String) 替代
     */
    @Deprecated
    public boolean isPrimaryKeyUpdated(Map<String, Object> event, String pkField) {
        Optional<Object> beforePk = extractBeforePrimaryKey(event, pkField);
        Optional<Object> afterPk = extractAfterPrimaryKey(event, pkField);
        
        return beforePk.isPresent() && afterPk.isPresent() 
                && !Objects.equals(beforePk.get(), afterPk.get());
    }
}

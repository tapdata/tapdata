package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.observable.logging.ObsLogger;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * 宽表增量更新器（重构版）
 * 
 * 核心功能：
 * 1. 四态判断：根据 before/after 主键集合判断 INSERT/UPDATE/DELETE/SKIP
 * 2. 可选事务：enableTransaction=true 时真实更新宽表，false 时仅生成事件
 * 3. Changelog 监听：通过 ChangelogListener 输出标准 TapdataEvent
 */
public class WideTableIncrementalUpdater {

    private ObsLogger logger;

    private final String wideTablePrimaryKey;
    private final String querySql;
    private final WithCteSqlGenerator withCteSqlGenerator;
    private final DuckDbOperator duckDbOperator;
    private final FourStateJudge fourStateJudge;
    private final boolean enableWriteWideTable;
    private final String wideTableName;
    private final List<ChangelogListener> changelogListeners = new ArrayList<>();
    
    /** 宽表的完整 NodeSchemaInfo 缓存（包含预计算的字段列表和类型信息） */
    private final NodeSchemaInfo tableSchemaInfoCache;
    private WideTableSourceRegistry sourceRegistry;
    private WideTableFieldOwnershipResolver fieldOwnershipResolver;
    private final WideTableDeleteAdjustmentService deleteAdjustmentService;
    
    /** 标记宽表是否已创建（避免重复解析 SQL） */
    private volatile boolean wideTableCreated = false;

    /**
     * Changelog 监听器接口
     */
    @FunctionalInterface
    public interface ChangelogListener {
        void onEvent(TapdataEvent event);
    }

    /**
     * 构造函数（完整）
     * @param enableWriteWideTable 是否启用事务模式（true=真实更新宽表，false=仅生成事件）
     */
    public WideTableIncrementalUpdater(String wideTableName, String wideTablePrimaryKey, String querySql,
                                       WithCteSqlGenerator withCteSqlGenerator,
                                       DuckDbOperator duckDbOperator, boolean enableWriteWideTable) {
        this(wideTableName, wideTableName, wideTablePrimaryKey, querySql, withCteSqlGenerator, duckDbOperator, enableWriteWideTable, null);
    }

    /**
     * 构造函数（完整，带 NodeSchemaInfo）
     * @param enableWriteWideTable 是否启用事务模式（true=真实更新宽表，false=仅生成事件）
     */
    public WideTableIncrementalUpdater(String wideTableName, String wideTablePrimaryKey, String querySql,
                                       WithCteSqlGenerator withCteSqlGenerator,
                                       DuckDbOperator duckDbOperator, boolean enableWriteWideTable,
                                       NodeSchemaInfo tableSchemaInfoCache) {
        this(wideTableName, wideTableName, wideTablePrimaryKey, querySql, withCteSqlGenerator, duckDbOperator, enableWriteWideTable, tableSchemaInfoCache);
    }

    public WideTableIncrementalUpdater log(ObsLogger obsLogger) {
        this.logger = obsLogger;
        return this;
    }

    /**
     * 构造函数（完整版，包含宽表名）
     * @param enableWriteWideTable 是否启用事务模式（true=真实更新宽表，false=仅生成事件）
     */
    public WideTableIncrementalUpdater(String tableId, String wideTableName, String wideTablePrimaryKey, 
                                       String querySql,
                                       WithCteSqlGenerator withCteSqlGenerator,
                                       DuckDbOperator duckDbOperator, boolean enableWriteWideTable) {
        this(tableId, wideTableName, wideTablePrimaryKey, querySql, withCteSqlGenerator, duckDbOperator, enableWriteWideTable, null);
    }

    /**
     * 构造函数（完整版，包含宽表名 + NodeSchemaInfo）
     * @param enableWriteWideTable 是否启用事务模式（true=真实更新宽表，false=仅生成事件）
     */
    public WideTableIncrementalUpdater(String tableId, String wideTableName, String wideTablePrimaryKey,
                                       String querySql,
                                       WithCteSqlGenerator withCteSqlGenerator,
                                       DuckDbOperator duckDbOperator, boolean enableWriteWideTable,
                                       NodeSchemaInfo tableSchemaInfoCache) {
        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.querySql = querySql;
        this.withCteSqlGenerator = withCteSqlGenerator;
        this.duckDbOperator = duckDbOperator;
        this.fourStateJudge = new FourStateJudge(tableId, wideTablePrimaryKey);
        this.enableWriteWideTable = enableWriteWideTable;
        this.wideTableName = wideTableName;
        this.tableSchemaInfoCache = tableSchemaInfoCache;
        this.sourceRegistry = WideTableSourceRegistry.empty();
        this.fieldOwnershipResolver = WideTableFieldOwnershipResolver.noop();
        this.deleteAdjustmentService = new WideTableDeleteAdjustmentService(Collections.singletonList(new ChildTableDeleteRetainStrategy()));
        
        // 注意：宽表创建已统一移到 HazelcastDuckDbSqlNode.manageDuckDbTables() 中
        // 这里不再重复创建
    }

    public WideTableIncrementalUpdater withDeleteSemantics(WideTableSourceRegistry sourceRegistry) {
        this.sourceRegistry = sourceRegistry == null ? WideTableSourceRegistry.empty() : sourceRegistry;
        this.fieldOwnershipResolver = this.sourceRegistry.isEmpty()
                ? WideTableFieldOwnershipResolver.noop()
                : new JSqlParserWideTableFieldOwnershipResolver(querySql, this.sourceRegistry);
        return this;
    }

    /**
     * 添加 Changelog 监听器
     */
    public void addChangelogListener(ChangelogListener listener) {
        if (listener != null) {
            changelogListeners.add(listener);
        }
    }

    /**
     * 批量更新宽表（唯一核心方法）
     * 
     * 事务模式：包裹 executeInTransaction + 真实更新宽表 + 生成事件
     * 非事务模式：仅生成事件，不更新宽表
     * 
     * @param affectedBeforeKeys before 受影响主键集合
     * @param afterKeysAndResults  after 受影响主键和查询结果的键值对
     * @param tableName 源表名（用于 WITH CTE 临时表名）
     * @return TapdataEvent 事件列表
     */
    public List<TapdataEvent> updateWideTableAsTapdataEvents(Set<Object> affectedBeforeKeys,
                                                             AbstractMap.SimpleEntry<Set<Object>, List<Map<String, Object>>> afterKeysAndResults,
                                                              String tableName, AtomicReference<BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult>> currentConsumer) throws SQLException, IOException {
        // 从 afterKeysAndResults 提取 afterRows（如果需要）
        // 注意：此重载不包含 afterRows，需要重新执行查询
        return executeAndUpdate(affectedBeforeKeys, afterKeysAndResults.getKey(), afterKeysAndResults.getValue(), null, tableName, currentConsumer);
    }

    /**
     * 批量更新宽表（向后兼容：4参数版本）
     * 
     * <p>此重载保持向后兼容，内部将 afterRows 作为数据行传递，不传递预计算结果，
     * 从而保持原有的查询执行行为。</p>
     * 
     * @param affectedBeforeKeys before 受影响主键集合
     * @param affectedAfterKeys after 受影响主键集合
     * @param afterRows after 数据行（用于执行查询）
     * @param tableName 源表名（用于 WITH CTE 临时表名）
     * @return TapdataEvent 事件列表
     * @deprecated 使用 {@link #updateWideTableAsTapdataEvents(Set, Set, List, List, String, AtomicReference)} 代替
     */
    @Deprecated
    public List<TapdataEvent> updateWideTableAsTapdataEvents(Set<Object> affectedBeforeKeys,
                                                             Set<Object> affectedAfterKeys,
                                                             List<Map<String, Object>> afterRows,
                                                              String tableName, AtomicReference<BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult>> currentConsumer) throws SQLException, IOException {
        // 向后兼容：不传递预计算结果，让方法执行查询
        return updateWideTableAsTapdataEvents(affectedBeforeKeys, affectedAfterKeys, null, afterRows, tableName, currentConsumer);
    }

    /**
     * 批量更新宽表（优化版：接受预计算的查询结果）
     * 
     * <p>此重载接受预计算的宽表查询结果，避免重复执行 WITH CTE 查询。</p>
     * 
     * @param affectedBeforeKeys   before 受影响主键集合
     * @param afterKeys            after 受影响主键集合
     * @param wideTableQueryResults 预计算的宽表查询结果（可为空，为空时自动执行查询）
     * @param afterRows            after 数据行（从 CDC 事件提取，用于备用查询）
     * @param tableName            源表名（用于 WITH CTE 临时表名）
     * @return TapdataEvent 事件列表
     */
    public List<TapdataEvent> updateWideTableAsTapdataEvents(Set<Object> affectedBeforeKeys,
                                                             Set<Object> afterKeys,
                                                             List<Map<String, Object>> wideTableQueryResults,
                                                             List<Map<String, Object>> afterRows,
                                                              String tableName, AtomicReference<BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult>> currentConsumer) throws SQLException, IOException {
        return executeAndUpdate(affectedBeforeKeys, afterKeys, wideTableQueryResults, afterRows, tableName, currentConsumer);
    }

    /**
     * 执行查询 + 四态判断 + 可选宽表更新
     * 
     * <p>优化说明：</p>
     * <ul>
     *   <li>使用预计算的查询结果，避免重复执行 WITH CTE 查询</li>
     *   <li>宽表更新直接使用 {@link #applyWideTableChanges(Set, List)}，避免 TapdataEvent 解析</li>
     *   <li>四态判断仅用于生成 Changelog 事件，不用于宽表更新</li>
     *   <li>事务模式：在 executeInTransaction 中执行宽表更新</li>
     * </ul>
     */
    private List<TapdataEvent> executeAndUpdate(Set<Object> affectedBeforeKeys,
                                                 Set<Object> afterKeys,
                                                 List<Map<String, Object>> wideTableQueryResults,
                                                 List<Map<String, Object>> afterRows,
                                                 String tableName, AtomicReference<BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult>> currentConsumer) throws SQLException, IOException {
        // 1. 使用预计算的查询结果，如果为空则尝试使用 afterRows 执行查询
        List<Map<String, Object>> results = wideTableQueryResults;
        if (results == null || results.isEmpty()) {
            // 预计算结果不可用，需要使用 afterRows 重新执行查询
            if (afterRows != null && !afterRows.isEmpty()) {
                logger.info("Pre-computed query results not available, executing WITH CTE query");
                String afterSql = withCteSqlGenerator.generateBatch(querySql, tableName, afterRows, 
                                                                   tableSchemaInfoCache != null ? tableSchemaInfoCache.getFieldNames() : Collections.emptyList());
                results = duckDbOperator.executeQuery(afterSql);
            } else {
                results = new ArrayList<>();
            }
        }
        
        results = deleteAdjustmentService.adjust(new WideTableDeleteAdjustmentContext(tableName, affectedBeforeKeys, results, (afterRows != null && !afterRows.isEmpty()) ? Collections.emptyList() : Collections.singletonList(new SmartMerger.MergedRecord() {{ setTableName(tableName); getBeforeRows().add(Collections.singletonMap(wideTablePrimaryKey, "marker")); }}), wideTableName, wideTablePrimaryKey, duckDbOperator, sourceRegistry, fieldOwnershipResolver));
        results = WideTableRowTypeNormalizer.normalizeRows(results, tableSchemaInfoCache);

        // 创建 final 副本以便在 lambda 中使用
        final List<Map<String, Object>> finalResults = results;
        final Set<Object> finalAffectedBeforeKeys = affectedBeforeKeys;

        // 2. 四态判断（用于生成 Changelog 事件）
        // 注意：即使 results 为空，fourStateJudge 也会生成 DELETE 事件
        List<TapdataEvent> events = fourStateJudge.judge(affectedBeforeKeys, results);

        // 3. 真实更新宽表（事务模式：在 executeInTransaction 中执行）
        if (enableWriteWideTable && !events.isEmpty()) {
            // 事务模式：在 executeInTransaction 中执行宽表更新
            applyWideTableChanges(finalAffectedBeforeKeys, finalResults);
        }

        // 4. 触发 ChangelogListener
        for (TapdataEvent e : events) {
            currentConsumer.get().accept(e, HazelcastProcessorBaseNode.ProcessResult.create());
        }
        //emitWideTableChangelogEvents(events);

        return events;
    }

    /**
     * 发射宽表 CDC 事件到下游
     */
    private void emitWideTableChangelogEvents(@MonotonicNonNull List<TapdataEvent> events) {
        for (TapdataEvent event : events) {
            for (ChangelogListener listener : changelogListeners) {
                try {
                    listener.onEvent(event);
                } catch (Exception e) {
                    logger.warn("Error notifying changelog listener", e);
                }
            }
        }
        logger.debug("emitWideTableChangelogEvents called - CDC event emission handled by WideTableUpdater");
    }

    /**
     * 将事件应用到宽表（批量模式：直接刷写，不走 buffer 缓存）
     * 
     * @deprecated 使用 {@link #applyWideTableChanges(Set, List)} 代替，避免 TapdataEvent 解析开销
     */
    @Deprecated
    private void applyEventsToWideTable(List<TapdataEvent> events) throws SQLException, IOException {
        // 0. 确保宽表存在（如果尚未创建）
        ensureWideTableExists();
        
        // 1. 收集 DELETE 主键和 INSERT 数据
        List<Object> deletePks = new ArrayList<>();
        List<Map<String, Object>> inserts = new ArrayList<>();

        // 获取主键字段的 TapType，用于类型转换
        Class<?> pkTargetType = getPkTargetType();
        logger.debug("PK target type for '{}': {}", wideTablePrimaryKey, pkTargetType.getSimpleName());

        for (TapdataEvent event : events) {
            TapEvent tapEvent = event.getTapEvent();
            if (tapEvent instanceof TapInsertRecordEvent) {
                inserts.add(((TapInsertRecordEvent) tapEvent).getAfter());
            } else if (tapEvent instanceof TapUpdateRecordEvent) {
                // UPDATE = DELETE old + INSERT new
                Map<String, Object> before = ((TapUpdateRecordEvent) tapEvent).getBefore();
                if (before != null && before.containsKey(wideTablePrimaryKey)) {
                    Object rawPk = before.get(wideTablePrimaryKey);
                    Object convertedPk = convertPkValue(rawPk, pkTargetType);
                    logger.debug("PK value conversion: {} ({}) → {} ({})", 
                            rawPk, rawPk.getClass().getSimpleName(),
                            convertedPk, convertedPk.getClass().getSimpleName());
                    deletePks.add(convertedPk);
                }
                inserts.add(((TapUpdateRecordEvent) tapEvent).getAfter());
            } else if (tapEvent instanceof TapDeleteRecordEvent) {
                Map<String, Object> before = ((TapDeleteRecordEvent) tapEvent).getBefore();
                if (before != null && before.containsKey(wideTablePrimaryKey)) {
                    Object rawPk = before.get(wideTablePrimaryKey);
                    Object convertedPk = convertPkValue(rawPk, pkTargetType);
                    logger.debug("PK value conversion: {} ({}) → {} ({})", 
                            rawPk, rawPk.getClass().getSimpleName(),
                            convertedPk, convertedPk.getClass().getSimpleName());
                    deletePks.add(convertedPk);
                }
            }
        }

        // 2. 批量删除（使用 deleteByIds，类型精准）
        if (!deletePks.isEmpty()) {
            if (tableSchemaInfoCache != null) {
                logger.debug("Batch delete by ids, count: {}", deletePks.size());
                duckDbOperator.deleteByIds(deletePks, tableSchemaInfoCache);
            } else {
                // 降级：tableSchemaInfoCache 为空时走旧路径
                String deleteSql = WideTableBatchSqlBuilder.buildDeleteSql(
                        wideTableName, wideTablePrimaryKey, deletePks, pkTargetType);
                logger.debug("Batch delete SQL: {}", deleteSql);
                duckDbOperator.executeUpdate(deleteSql);
            }
        }

        // 3. 批量插入（一条 SQL，直接刷写）
        if (!inserts.isEmpty()) {
            String insertSql = WideTableBatchSqlBuilder.buildInsertSql(
                    wideTableName, tableSchemaInfoCache.getFieldNames(), inserts);
            logger.debug("Batch insert SQL: {}", insertSql);
            duckDbOperator.executeUpdate(insertSql);
        }
    }

    /**
     * 高效批量更新宽表（优化版：直接接受主键和集合，避免 TapdataEvent 解析）
     * 
     * <p>核心优化：</p>
     * <ul>
     *   <li>使用 {@code affectedBeforeKeys} 批量删除旧数据</li>
     *   <li>使用 {@code wideTableQueryResults} 批量插入新数据（复用 DuckDbOperator.writeBatch Arrow 零拷贝）</li>
     *   <li>避免 TapdataEvent 解析和四态判断，直接操作数据</li>
     * </ul>
     *
     * @param affectedBeforeKeys  需要删除的主键集合（写入前的主键）
     * @param wideTableQueryResults 需要插入的数据集（WITH CTE 查询结果）
     * @throws SQLException 数据库操作异常
     * @throws IOException  IO 异常（Arrow 写入）
     */
    private void applyWideTableChanges(Set<Object> affectedBeforeKeys,
                                       List<Map<String, Object>> wideTableQueryResults) throws SQLException, IOException {
        // 0. 确保宽表存在
        ensureWideTableExists();

        // 1. 批量删除旧数据（使用 WHERE PK IN (...)）
        if (affectedBeforeKeys != null && !affectedBeforeKeys.isEmpty()) {
            logger.debug("Batch deleting {} records from wide table '{}'", 
                        affectedBeforeKeys.size(), wideTableName);
            
            // 批量删除旧数据（使用 deleteByIds，类型安全、支持复合主键）
            if (tableSchemaInfoCache != null) {
                List<Object> pkList = new ArrayList<>(affectedBeforeKeys);
                logger.debug("Batch delete by ids, count: {}", pkList.size());
                duckDbOperator.deleteByIds(pkList, tableSchemaInfoCache);
            } else {
                // 降级：tableSchemaInfoCache 为空时走旧路径
                Class<?> pkTargetType = getPkTargetType();
                List<Object> convertedKeys = affectedBeforeKeys.stream()
                        .map(pk -> convertPkValue(pk, pkTargetType))
                        .collect(java.util.stream.Collectors.toList());

                String deleteSql = WideTableBatchSqlBuilder.buildDeleteSql(
                        wideTableName, wideTablePrimaryKey,
                        convertedKeys, pkTargetType);
                logger.debug("Batch delete SQL: {}", deleteSql);
                duckDbOperator.executeUpdate(deleteSql);
            }
        }

        // 2. 批量插入新数据（优先使用 Arrow 零拷贝写入）
        if (wideTableQueryResults != null && !wideTableQueryResults.isEmpty()) {
            logger.debug("Batch inserting {} records into wide table '{}'", 
                        wideTableQueryResults.size(), wideTableName);
            
            // 复用 DuckDbOperator.writeBatch (Arrow 零拷贝，高性能)
            if (tableSchemaInfoCache != null) {
                // 使用 NodeSchemaInfo 优化路径（预计算 Schema）
                duckDbOperator.writeBatch(wideTableQueryResults, tableSchemaInfoCache);
            } else {
                // 降级到表名模式
                duckDbOperator.writeBatch(wideTableQueryResults, wideTableName);
            }
        }
    }

    /**
     * 确保宽表存在，如果不存在则根据 querySql 自动创建
     * 
     * <p>实现逻辑：</p>
     * <ol>
     *   <li>检查 wideTableCreated 标志，避免重复解析</li>
     *   <li>检查表是否已存在（通过查询 DuckDB 系统表）</li>
     *   <li>如果不存在，使用 CREATE TABLE ... AS SELECT ... WHERE 1=0 创建表结构</li>
     *   <li>如果 AS SELECT 方式失败，降级到传统字段列表方式</li>
     * </ol>
     * 
     * @throws SQLException 数据库操作异常
     */
    private void ensureWideTableExists() throws SQLException {
        // 快速检查：避免重复解析
        if (wideTableCreated) {
            return;
        }
        
        synchronized (this) {
            // 双重检查
            if (wideTableCreated) {
                return;
            }
            
            try {
                // 检查表是否已存在
                if (isTableExists(wideTableName)) {
                    logger.info("Wide table '{}' already exists, skipping creation", wideTableName);
                    wideTableCreated = true;
                    return;
                }
                
                // 表不存在，需要创建
                logger.info("Wide table '{}' does not exist, creating from querySql...", wideTableName);
                
                // 优先使用 CREATE TABLE ... AS SELECT ... WHERE 1=0 语法
                // 优势：自动推断字段类型，保持与查询结果一致
                boolean created = createTableUsingAsSelect(wideTableName, querySql);
                
                if (!created) {
                    // AS SELECT 方式失败，降级到传统字段列表方式
                    logger.warn("CREATE TABLE AS SELECT failed, falling back to traditional DDL");
                    createTableUsingFieldList(wideTableName, wideTablePrimaryKey);
                }
                
                logger.info("Successfully created wide table '{}'", wideTableName);
                wideTableCreated = true;
                
            } catch (Exception e) {
                logger.error("Failed to ensure wide table exists: {}", e.getMessage(), e);
                throw new SQLException("Failed to create wide table: " + wideTableName, e);
            }
        }
    }

    /**
     * 使用 CREATE TABLE ... AS SELECT ... WHERE 1=0 语法创建表
     * 
     * @param tableName 表名
     * @param querySql 原始查询SQL
     * @return true 表示创建成功，false 表示需要降级到其他方式
     */
    private boolean createTableUsingAsSelect(String tableName, String querySql) {
        try {
            // 生成 CREATE TABLE ... AS SELECT ... WHERE 1=0 语句
            String createDdl = WideTableDdlGenerator.generateCreateTableAsSelect(tableName, querySql);
            
            // 执行建表
            duckDbOperator.executeUpdate(createDdl);
            
            logger.info("Successfully created table '{}' using CREATE TABLE AS SELECT", tableName);
            return true;
            
        } catch (Exception e) {
            logger.warn("CREATE TABLE AS SELECT failed for table '{}': {}", tableName, e.getMessage());
            return false;
        }
    }

    /**
     * 使用传统字段列表方式创建表（降级方案）
     * 
     * @param tableName 表名
     * @param primaryKey 主键字段名
     */
    private void createTableUsingFieldList(String tableName, String primaryKey) throws SQLException {
        // ========== 优先使用 NodeSchemaInfo（如果有） ==========
        if (this.tableSchemaInfoCache != null && this.tableSchemaInfoCache.getFieldMap() != null && !this.tableSchemaInfoCache.getFieldMap().isEmpty()) {
            logger.info("Creating wide table using NodeSchemaInfo: {}", tableSchemaInfoCache.getTableName());
            String createDdl = WideTableDdlGenerator.generateCreateTableDdl(tableSchemaInfoCache, primaryKey);
            duckDbOperator.executeUpdate(createDdl);
            logger.info("Successfully created table '{}' from NodeSchemaInfo", tableName);
            return;
        }
        
        // ========== 降级：使用原有的字段列表方式 ==========
        // 从 querySql 解析字段
        List<String> selectFields = WideTableDdlGenerator.extractSelectFields(querySql);
        if (selectFields.isEmpty()) {
            // 如果解析失败，使用预定义的 fields 列表
            logger.warn("Failed to extract fields from querySql, using predefined fields: {}", tableSchemaInfoCache.getFieldNames());
            selectFields = tableSchemaInfoCache.getFieldNames();
        }
        
        // 生成传统 CREATE TABLE DDL
        String createDdl = WideTableDdlGenerator.generateCreateTableDdl(
                tableName, selectFields, primaryKey);
        
        // 执行建表
        duckDbOperator.executeUpdate(createDdl);
        
        logger.info("Successfully created table '{}' using traditional DDL with {} fields", 
                   tableName, selectFields.size());
    }

    /**
     * 检查表是否已存在
     * 
     * @param tableName 表名
     * @return true 表示表已存在，false 表示不存在
     * @throws SQLException 数据库操作异常
     */
    private boolean isTableExists(String tableName) throws SQLException {
        try {
            // DuckDB 查询系统表检查表是否存在
            String checkSql = String.format(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'",
                tableName
            );
            List<Map<String, Object>> result = duckDbOperator.executeQuery(checkSql);
            if (!result.isEmpty()) {
                Object count = result.get(0).values().iterator().next();
                return count instanceof Number && ((Number) count).intValue() > 0;
            }
            return false;
        } catch (Exception e) {
            logger.debug("Error checking table existence: {}", e.getMessage());
            // 如果查询失败，假设表不存在（需要创建）
            return false;
        }
    }

    /**
     * 获取主键字段的目标 Java 类型（用于 SQL 格式化时的类型转换）
     * 
     * <p>根据 NodeSchemaInfo 中的 TapType 判断主键字段类型：
     * <ul>
     *   <li>TapNumber → Long（DuckDB BIGINT）</li>
     *   <li>TapString → String（DuckDB VARCHAR）</li>
     *   <li>其他 → String（默认）</li>
     * </ul>
     * </p>
     * 
     * @return 主键字段对应的 Java 类型，如 Long.class、String.class 等
     */
    private Class<?> getPkTargetType() {
        if (tableSchemaInfoCache == null) {
            logger.warn("tableSchemaInfoCache is null, defaulting PK type to String");
            return String.class; // 无法判断类型，默认 String
        }
        
        TapType pkTapType = tableSchemaInfoCache.getFieldType(wideTablePrimaryKey);
        if (pkTapType == null) {
            logger.warn("PK field '{}' has no TapType, defaulting to String", wideTablePrimaryKey);
            return String.class; // 未知类型，默认 String
        }
        
        logger.debug("PK field '{}' TapType: {}", wideTablePrimaryKey, pkTapType.getClass().getSimpleName());
        
        // 根据 TapType 判断目标 Java 类型
        if (pkTapType instanceof io.tapdata.entity.schema.type.TapNumber) {
            // 数值类型 → Long（DuckDB BIGINT）
            return Long.class;
        } else if (pkTapType instanceof io.tapdata.entity.schema.type.TapString) {
            // 字符串类型 → String（DuckDB VARCHAR）
            return String.class;
        } else if (pkTapType instanceof io.tapdata.entity.schema.type.TapBoolean) {
            // 布尔类型 → Boolean
            return Boolean.class;
        } else if (pkTapType instanceof io.tapdata.entity.schema.type.TapDate) {
            // 日期类型 → String（DuckDB DATE，格式化后用字符串比较）
            return String.class;
        } else if (pkTapType instanceof io.tapdata.entity.schema.type.TapDateTime) {
            // 时间戳类型 → String（DuckDB TIMESTAMP，格式化后用字符串比较）
            return String.class;
        }
        
        // 默认返回 String
        logger.warn("Unknown TapType for PK field '{}': {}, defaulting to String", 
                wideTablePrimaryKey, pkTapType.getClass().getSimpleName());
        return String.class;
    }

    /**
     * 将 PK 值转换为目标类型
     * 
     * <p>确保 PK 值类型与数据库字段类型匹配，避免 SQL 中 VARCHAR 与 BIGINT 比较错误。</p>
     * 
     * @param pkValue PK 原始值（可能是 String、Number 等）
     * @param targetType 目标 Java 类型（由 getPkTargetType() 返回）
     * @return 转换后的 PK 值
     */
    private Object convertPkValue(Object pkValue, Class<?> targetType) {
        if (pkValue == null) {
            return null;
        }
        
        logger.debug("Converting PK value: {} (type: {}) to target type: {}", 
                pkValue, pkValue.getClass().getSimpleName(), targetType.getSimpleName());
        
        // 如果已经是目标类型，直接返回
        if (targetType.isInstance(pkValue)) {
            logger.debug("PK value already target type, returning as-is");
            return pkValue;
        }
        
        // String → 数值类型
        if (pkValue instanceof String && Number.class.isAssignableFrom(targetType)) {
            try {
                if (targetType == Long.class) {
                    Long result = Long.parseLong((String) pkValue);
                    logger.debug("Converted String '{}' to Long: {}", pkValue, result);
                    return result;
                } else if (targetType == Integer.class) {
                    return Integer.parseInt((String) pkValue);
                } else if (targetType == Double.class) {
                    return Double.parseDouble((String) pkValue);
                } else if (targetType == java.math.BigDecimal.class) {
                    return new java.math.BigDecimal((String) pkValue);
                }
            } catch (NumberFormatException e) {
                logger.warn("Failed to convert PK value '{}' to {}: {}", pkValue, targetType.getSimpleName(), e.getMessage());
                return pkValue; // 转换失败，返回原值
            }
        }
        
        // 数值类型 → String（较少见，但处理边界情况）
        if (pkValue instanceof Number && targetType == String.class) {
            return pkValue.toString();
        }
        
        // 其他情况：返回原值
        logger.warn("Cannot convert PK value {} (type: {}) to {}, returning as-is", 
                pkValue, pkValue.getClass().getSimpleName(), targetType.getSimpleName());
        return pkValue;
    }
}

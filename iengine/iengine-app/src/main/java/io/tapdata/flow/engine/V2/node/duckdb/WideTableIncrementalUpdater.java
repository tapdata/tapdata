package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * 宽表增量更新器（重构版）
 * 
 * 核心功能：
 * 1. 四态判断：根据 before/after 主键集合判断 INSERT/UPDATE/DELETE/SKIP
 * 2. 可选事务：enableTransaction=true 时真实更新宽表，false 时仅生成事件
 * 3. Changelog 监听：通过 ChangelogListener 输出标准 TapdataEvent
 */
public class WideTableIncrementalUpdater {

    private static final Logger logger = LoggerFactory.getLogger(WideTableIncrementalUpdater.class);

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
        
        // 注意：宽表创建已统一移到 HazelcastDuckDbSqlNode.manageDuckDbTables() 中
        // 这里不再重复创建
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
     * @param affectedAfterKeys after 受影响主键集合
     * @param afterRows after 数据行（从 CDC 事件提取）
     * @param tableName 源表名（用于 WITH CTE 临时表名）
     * @return TapdataEvent 事件列表
     */
    public List<TapdataEvent> updateWideTableAsTapdataEvents(Set<Object> affectedBeforeKeys,
                                                              Set<Object> affectedAfterKeys,
                                                              List<Map<String, Object>> afterRows,
                                                              String tableName) throws SQLException, IOException {
        return executeAndUpdate(affectedBeforeKeys, afterRows, tableName);

    }

    /**
     * 执行查询 + 四态判断 + 可选宽表更新
     */
    private List<TapdataEvent> executeAndUpdate(Set<Object> affectedBeforeKeys,
                                                 List<Map<String, Object>> afterRows,
                                                 String tableName) throws SQLException, IOException {
        // 1. 使用 WITH CTE 执行 after 查询
        List<Map<String, Object>> results = Collections.emptyList();
        if (afterRows != null && !afterRows.isEmpty()) {
            String afterSql = withCteSqlGenerator.generateBatch(querySql, tableName, afterRows, tableSchemaInfoCache.getFieldNames());
            results = duckDbOperator.executeQuery(afterSql);
        }

        // 2. 四态判断
        List<TapdataEvent> events = fourStateJudge.judge(affectedBeforeKeys, results);

        // 3. 真实更新宽表
        if (enableWriteWideTable && !events.isEmpty()) {
            applyEventsToWideTable(events);
        }

        // 4. 触发 ChangelogListener
        emitWideTableChangelogEvents(events);

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
     */
    private void applyEventsToWideTable(List<TapdataEvent> events) throws SQLException, IOException {
        // 0. 确保宽表存在（如果尚未创建）
        ensureWideTableExists();
        
        // 1. 收集 DELETE 主键和 INSERT 数据
        List<Object> deletePks = new ArrayList<>();
        List<Map<String, Object>> inserts = new ArrayList<>();

        for (TapdataEvent event : events) {
            TapEvent tapEvent = event.getTapEvent();
            if (tapEvent instanceof TapInsertRecordEvent) {
                inserts.add(((TapInsertRecordEvent) tapEvent).getAfter());
            } else if (tapEvent instanceof TapUpdateRecordEvent) {
                // UPDATE = DELETE old + INSERT new
                Map<String, Object> before = ((TapUpdateRecordEvent) tapEvent).getBefore();
                if (before != null && before.containsKey(wideTablePrimaryKey)) {
                    deletePks.add(before.get(wideTablePrimaryKey));
                }
                inserts.add(((TapUpdateRecordEvent) tapEvent).getAfter());
            } else if (tapEvent instanceof TapDeleteRecordEvent) {
                Map<String, Object> before = ((TapDeleteRecordEvent) tapEvent).getBefore();
                if (before != null && before.containsKey(wideTablePrimaryKey)) {
                    deletePks.add(before.get(wideTablePrimaryKey));
                }
            }
        }

        // 2. 批量删除（一条 SQL，直接刷写）
        if (!deletePks.isEmpty()) {
            String deleteSql = WideTableBatchSqlBuilder.buildDeleteSql(
                    wideTableName, wideTablePrimaryKey, deletePks);
            logger.debug("Batch delete SQL: {}", deleteSql);
            duckDbOperator.executeUpdate(deleteSql);
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
}

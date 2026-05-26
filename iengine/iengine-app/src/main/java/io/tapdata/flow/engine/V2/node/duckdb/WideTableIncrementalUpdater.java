package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
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
    private final List<String> fields;
    private final WithCteSqlGenerator withCteSqlGenerator;
    private final DuckDbOperator duckDbOperator;
    private final FourStateJudge fourStateJudge;
    private final boolean enableTransaction;
    private final List<ChangelogListener> changelogListeners = new ArrayList<>();

    /**
     * Changelog 监听器接口
     */
    @FunctionalInterface
    public interface ChangelogListener {
        void onEvent(TapdataEvent event);
    }

    /**
     * 构造函数（非事务模式）
     */
    public WideTableIncrementalUpdater(String tableId, String wideTablePrimaryKey, String querySql,
                                       List<String> fields, WithCteSqlGenerator withCteSqlGenerator,
                                       DuckDbOperator duckDbOperator) {
        this(tableId, wideTablePrimaryKey, querySql, fields, withCteSqlGenerator, duckDbOperator, false);
    }

    /**
     * 构造函数（完整）
     * @param enableTransaction 是否启用事务模式（true=真实更新宽表，false=仅生成事件）
     */
    public WideTableIncrementalUpdater(String tableId, String wideTablePrimaryKey, String querySql,
                                       List<String> fields, WithCteSqlGenerator withCteSqlGenerator,
                                       DuckDbOperator duckDbOperator, boolean enableTransaction) {
        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.querySql = querySql;
        this.fields = fields;
        this.withCteSqlGenerator = withCteSqlGenerator;
        this.duckDbOperator = duckDbOperator;
        this.fourStateJudge = new FourStateJudge(tableId, wideTablePrimaryKey);
        this.enableTransaction = enableTransaction;
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
        final List<TapdataEvent>[] resultHolder = new List[]{Collections.emptyList()};

        if (enableTransaction) {
            // 事务模式：真实更新宽表 + 生成事件
            duckDbOperator.executeInTransaction(() -> {
                resultHolder[0] = executeAndUpdate(affectedBeforeKeys, afterRows, tableName);
            });
        } else {
            // 非事务模式：仅生成事件，不更新宽表
            resultHolder[0] = executeAndUpdate(affectedBeforeKeys, afterRows, tableName);
        }

        return resultHolder[0];
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
            String afterSql = withCteSqlGenerator.generateBatch(querySql, tableName, afterRows, fields);
            results = duckDbOperator.executeQuery(afterSql);
        }

        // 2. 四态判断
        List<TapdataEvent> events = fourStateJudge.judge(affectedBeforeKeys, results);

        // 3. 事务模式下真实更新宽表
        if (enableTransaction && !events.isEmpty()) {
            applyEventsToWideTable(events);
        }

        // 4. 触发 ChangelogListener
        for (TapdataEvent event : events) {
            for (ChangelogListener listener : changelogListeners) {
                try {
                    listener.onEvent(event);
                } catch (Exception e) {
                    logger.warn("Error notifying changelog listener", e);
                }
            }
        }

        return events;
    }

    /**
     * 将事件应用到宽表（真实执行 INSERT/UPDATE/DELETE）
     */
    private void applyEventsToWideTable(List<TapdataEvent> events) throws SQLException, IOException {
        // 按操作类型分组，批量执行
        List<Map<String, Object>> inserts = new ArrayList<>();
        List<Object> deletePks = new ArrayList<>();

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

        // 批量删除
        for (Object pk : deletePks) {
            deleteRowByPk(pk);
        }

        // 批量插入
        if (!inserts.isEmpty()) {
            duckDbOperator.batchInsert("wide_table", inserts);
        }
    }

    /**
     * 按主键删除宽表记录
     */
    private void deleteRowByPk(Object pk) throws SQLException {
        String pkValue;
        if (pk instanceof String) {
            pkValue = "'" + pk.toString().replace("'", "''") + "'";
        } else {
            pkValue = pk.toString();
        }

        String deleteSql = String.format(
                "DELETE FROM wide_table WHERE %s = %s",
                wideTablePrimaryKey,
                pkValue
        );
        logger.debug("Deleting row: {}", deleteSql);
        duckDbOperator.executeUpdate(deleteSql);
    }
}

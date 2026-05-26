package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 宽表增量更新器
 * 基于 before/after 主键集合实现批量宽表更新
 * 
 * 核心逻辑：
 * 1. 计算纯 DELETE 主键（在 before 中但不在 after 中）
 * 2. 对 after 主键执行宽表查询获取最新数据
 * 3. 生成 DELETE/INSERT/UPDATE 事件
 */
public class WideTableIncrementalUpdater {

    private static final Logger logger = LoggerFactory.getLogger(WideTableIncrementalUpdater.class);

    private final String wideTablePrimaryKey;
    private final String querySql;
    private final List<String> fields;
    private final WithCteSqlGenerator withCteSqlGenerator;
    private final DuckDbOperator duckDbOperator;
    private final FourStateJudge fourStateJudge;

    /**
     * 构造函数（向后兼容，使用 IN 子句方式）
     * @deprecated Use constructor with WithCteSqlGenerator
     */
    @Deprecated
    public WideTableIncrementalUpdater(String wideTablePrimaryKey, String querySql,
                                       List<String> fields, DuckDbOperator duckDbOperator) {
        this(wideTablePrimaryKey, querySql, fields, new WithCteSqlGenerator(), duckDbOperator);
    }

    /**
     * 构造函数（使用 WITH CTE 方式）
     */
    public WideTableIncrementalUpdater(String wideTablePrimaryKey, String querySql,
                                       List<String> fields, WithCteSqlGenerator withCteSqlGenerator,
                                       DuckDbOperator duckDbOperator) {
        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.querySql = querySql;
        this.fields = fields;
        this.withCteSqlGenerator = withCteSqlGenerator;
        this.duckDbOperator = duckDbOperator;
        this.fourStateJudge = new FourStateJudge("wide_table", wideTablePrimaryKey);
    }

    /**
     * 构造函数（使用 WITH CTE 方式 + 自定义 tableId）
     */
    public WideTableIncrementalUpdater(String tableId, String wideTablePrimaryKey, String querySql,
                                       List<String> fields, WithCteSqlGenerator withCteSqlGenerator,
                                       DuckDbOperator duckDbOperator) {
        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.querySql = querySql;
        this.fields = fields;
        this.withCteSqlGenerator = withCteSqlGenerator;
        this.duckDbOperator = duckDbOperator;
        this.fourStateJudge = new FourStateJudge(tableId, wideTablePrimaryKey);
    }

    /**
     * 批量更新宽表
     * @param affectedBeforeKeys before 受影响主键集合（用于 DELETE 宽表记录）
     * @param affectedAfterKeys after 受影响主键集合（用于 INSERT/UPDATE 宽表记录）
     * @return 宽表 CDC 事件列表
     */
    public List<WideTableCdcEvent> updateWideTable(Set<Object> affectedBeforeKeys,
                                                    Set<Object> affectedAfterKeys) throws SQLException {
        List<WideTableCdcEvent> events = new ArrayList<>();

        // 1. 计算纯 DELETE 的主键（在 before 中但不在 after 中）
        Set<Object> pureDeleteKeys = new LinkedHashSet<>(affectedBeforeKeys);
        pureDeleteKeys.removeAll(affectedAfterKeys);

        // 2. 生成 DELETE 事件
        for (Object pk : pureDeleteKeys) {
            events.add(new WideTableCdcEvent(WideTableCdcEvent.OpType.DELETE, pk, null));
            logger.debug("Generated DELETE event for pk={}", pk);
        }

        // 3. 对 after 主键执行宽表查询，获取最新数据
        if (!affectedAfterKeys.isEmpty()) {
            String afterSql = generateAfterSql(affectedAfterKeys);
            List<Map<String, Object>> results = duckDbOperator.executeQuery(afterSql);

            // 4. 生成 INSERT/UPDATE 事件
            for (Map<String, Object> row : results) {
                Object pk = row.get(wideTablePrimaryKey);
                if (pk != null) {
                    // 判断是 INSERT 还是 UPDATE
                    if (affectedBeforeKeys.contains(pk)) {
                        events.add(new WideTableCdcEvent(WideTableCdcEvent.OpType.UPDATE, pk, row));
                        logger.debug("Generated UPDATE event for pk={}", pk);
                    } else {
                        events.add(new WideTableCdcEvent(WideTableCdcEvent.OpType.INSERT, pk, row));
                        logger.debug("Generated INSERT event for pk={}", pk);
                    }
                }
            }
        }

        logger.info("Generated {} wide table CDC events: {} DELETE, {} INSERT/UPDATE",
                events.size(), pureDeleteKeys.size(), events.size() - pureDeleteKeys.size());

        return events;
    }

    /**
     * 批量更新宽表（使用 WITH CTE）
     * @param affectedBeforeKeys before 受影响主键集合（用于 DELETE 宽表记录）
     * @param affectedAfterKeys after 受影响主键集合（用于 INSERT/UPDATE 宽表记录）
     * @param afterRows after 数据行（从 CDC 事件提取）
     * @param tableName 源表名（用于 WITH CTE 临时表名）
     * @return 宽表 CDC 事件列表
     */
    public List<WideTableCdcEvent> updateWideTable(Set<Object> affectedBeforeKeys,
                                                    Set<Object> affectedAfterKeys,
                                                    List<Map<String, Object>> afterRows,
                                                    String tableName) throws SQLException {
        List<WideTableCdcEvent> events = new ArrayList<>();

        // 1. 计算纯 DELETE 的主键（在 before 中但不在 after 中）
        Set<Object> pureDeleteKeys = new LinkedHashSet<>(affectedBeforeKeys);
        pureDeleteKeys.removeAll(affectedAfterKeys);

        // 2. 生成 DELETE 事件
        for (Object pk : pureDeleteKeys) {
            events.add(new WideTableCdcEvent(WideTableCdcEvent.OpType.DELETE, pk, null));
            logger.debug("Generated DELETE event for pk={}", pk);
        }

        // 3. 使用 WITH CTE 执行 after 查询
        if (afterRows != null && !afterRows.isEmpty()) {
            String afterSql = withCteSqlGenerator.generateBatch(querySql, tableName, afterRows, fields);
            List<Map<String, Object>> results = duckDbOperator.executeQuery(afterSql);

            // 4. 生成 INSERT/UPDATE 事件
            for (Map<String, Object> row : results) {
                Object pk = row.get(wideTablePrimaryKey);
                if (pk != null) {
                    if (affectedBeforeKeys.contains(pk)) {
                        events.add(new WideTableCdcEvent(WideTableCdcEvent.OpType.UPDATE, pk, row));
                        logger.debug("Generated UPDATE event for pk={}", pk);
                    } else {
                        events.add(new WideTableCdcEvent(WideTableCdcEvent.OpType.INSERT, pk, row));
                        logger.debug("Generated INSERT event for pk={}", pk);
                    }
                }
            }
        }

        logger.info("Generated {} wide table CDC events: {} DELETE, {} INSERT/UPDATE",
                events.size(), pureDeleteKeys.size(), events.size() - pureDeleteKeys.size());

        return events;
    }

    /**
     * 生成 after SQL（批量查询宽表数据）
     */
    private String generateAfterSql(Set<Object> affectedAfterKeys) {
        String inClause = buildInClause(affectedAfterKeys);
        return String.format("WITH affected AS (%s) SELECT * FROM (%s) WHERE %s IN %s",
                inClause, querySql, wideTablePrimaryKey, inClause);
    }

    /**
     * 构建 IN 子句
     */
    private String buildInClause(Set<Object> keys) {
        return "(" + keys.stream()
                .map(this::formatValue)
                .collect(Collectors.joining(", ")) + ")";
    }

    /**
     * 格式化 SQL 值
     */
    private String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        }
        return value.toString();
    }

    /**
     * 批量更新宽表（使用 FourStateJudge 输出 TapdataEvent）
     * @param affectedBeforeKeys before 受影响主键集合（用于 DELETE 宽表记录）
     * @param affectedAfterKeys after 受影响主键集合（用于 INSERT/UPDATE 宽表记录）
     * @param afterRows after 数据行（从 CDC 事件提取）
     * @param tableName 源表名（用于 WITH CTE 临时表名）
     * @return TapdataEvent 事件列表
     */
    public List<TapdataEvent> updateWideTableAsTapdataEvents(Set<Object> affectedBeforeKeys,
                                                              Set<Object> affectedAfterKeys,
                                                              List<Map<String, Object>> afterRows,
                                                              String tableName) throws SQLException {
        // 1. 计算纯 DELETE 的主键（在 before 中但不在 after 中）
        Set<Object> pureDeleteKeys = new LinkedHashSet<>(affectedBeforeKeys);
        pureDeleteKeys.removeAll(affectedAfterKeys);

        // 2. 使用 WITH CTE 执行 after 查询
        List<Map<String, Object>> results = Collections.emptyList();
        if (afterRows != null && !afterRows.isEmpty()) {
            String afterSql = withCteSqlGenerator.generateBatch(querySql, tableName, afterRows, fields);
            results = duckDbOperator.executeQuery(afterSql);
        }

        // 3. 使用 FourStateJudge 进行四态判断
        return fourStateJudge.judge(pureDeleteKeys, results);
    }
}

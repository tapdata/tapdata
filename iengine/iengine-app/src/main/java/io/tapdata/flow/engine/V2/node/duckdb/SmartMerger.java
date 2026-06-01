package io.tapdata.flow.engine.V2.node.duckdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.node.duckdb.NodeSchemaInfo;
import io.tapdata.flow.engine.V2.util.TapEventUtil;

import java.util.*;

/**
 * Advanced smart merger implementing full merge_events_smart (M3) from ADR D4.
 *
 * Features:
 * - Track record lifecycle by initial_pk
 * - Handle primary key changes (1→2→3 chains)
 * - Support ABA scenarios (pk returns to original)
 * - Output single final operation per record
 * - Merge last-wins for same-record operations
 *
 * @see <a href="docs/adr/decision-4-smart-merge.md">ADR D4</a>
 */
public class SmartMerger {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> PK_CANDIDATES = Arrays.asList("_id", "id", "pk", "PK", "ID");
    private static final String INSERT_OP = "INSERT";
    private static final String UPDATE_OP = "UPDATE";
    private static final String DELETE_OP = "DELETE";
    private static final String DELETE_INSERT_OP = "DELETE_INSERT";

    /**
     * Represents a single logical record's full lifecycle through REPLAY phase.
     */
    public static class MergedRecord {
        private final Object initialPk;           // Original pk from INSERT (never changes)
        private Object currentPk;                 // Current pk value (may change via UPDATE)
        private final List<TapdataEvent> operations; // All mutations (original TapdataEvents)
        private TapRecordEvent finalStateEvent;   // Final state event
        private String finalOp;                   // 'INSERT' | 'UPDATE' | 'DELETE' | 'DELETE_INSERT'

        public MergedRecord(Object initialPk, Object currentPk) {
            this.initialPk = initialPk;
            this.currentPk = currentPk;
            this.operations = new ArrayList<>();
            this.finalStateEvent = null;
            this.finalOp = INSERT_OP;
        }

        // 添加便捷方法获取 Map 格式数据
        public Map<String, Object> getFinalState() {
            return finalStateEvent != null ? TapEventUtil.getAfter(finalStateEvent) : null;
        }

        public Object getInitialPk() { return initialPk; }
        public Object getCurrentPk() { return currentPk; }
        public void setCurrentPk(Object currentPk) { this.currentPk = currentPk; }
        public List<TapdataEvent> getOperations() { return operations; }
        public TapRecordEvent getFinalStateEvent() { return finalStateEvent; }
        public void setFinalStateEvent(TapRecordEvent finalStateEvent) { this.finalStateEvent = finalStateEvent; }
        public String getFinalOp() { return finalOp; }
        public void setFinalOp(String finalOp) { this.finalOp = finalOp; }
    }

    /**
     * 按 initial_pk 追踪记录生命周期，输出每条记录的最终状态。
     *
     * @param tapEvents 仅包含 TapdataEvent 的列表
     * @return list of MergedRecord, one per unique initial_pk
     */
    public static List<MergedRecord> mergeEventsSmart(List<TapdataEvent> tapEvents) {
        if (tapEvents == null || tapEvents.isEmpty()) {
            return Collections.emptyList();
        }

        Map<Object, MergedRecord> mergedRecords = new LinkedHashMap<>();
        Map<Object, Object> pkMigration = new HashMap<>();

        for (TapdataEvent tapEvent : tapEvents) {
            TapEvent tapEventInner = tapEvent.getTapEvent();
            if (tapEventInner instanceof TapRecordEvent) {
                processEvent(tapEvent, (TapRecordEvent) tapEventInner, mergedRecords, pkMigration);
            }
        }

        return new ArrayList<>(mergedRecords.values());
    }

    /**
     * 按 initial_pk 追踪记录生命周期，输出每条记录的最终状态。
     * 使用 NodeSchemaInfo 获取真实主键信息。
     *
     * @param tapEvents 仅包含 TapdataEvent 的列表
     * @param tableName 表名
     * @param schema 表的 schema 信息
     * @return list of MergedRecord, one per unique initial_pk
     */
    public static List<MergedRecord> mergeEventsSmart(List<TapdataEvent> tapEvents,
                                                       String tableName,
                                                       NodeSchemaInfo schema) {
        if (tapEvents == null || tapEvents.isEmpty()) {
            return Collections.emptyList();
        }

        Map<Object, MergedRecord> mergedRecords = new LinkedHashMap<>();
        Map<Object, Object> pkMigration = new HashMap<>();

        for (TapdataEvent tapEvent : tapEvents) {
            TapEvent tapEventInner = tapEvent.getTapEvent();
            if (tapEventInner instanceof TapRecordEvent) {
                processEvent(tapEvent, (TapRecordEvent) tapEventInner, mergedRecords, pkMigration, tableName, schema);
            }
        }

        return new ArrayList<>(mergedRecords.values());
    }

    /**
     * 处理单条 TapdataEvent，并更新合并态。
     */
    private static void processEvent(TapdataEvent tapEvent,
                                     TapRecordEvent recordEvent,
                                     Map<Object, MergedRecord> mergedRecords,
                                     Map<Object, Object> pkMigration) {
        String opType;
        if (recordEvent instanceof TapInsertRecordEvent) {
            opType = INSERT_OP;
        } else if (recordEvent instanceof TapUpdateRecordEvent) {
            opType = UPDATE_OP;
        } else if (recordEvent instanceof TapDeleteRecordEvent) {
            opType = DELETE_OP;
        } else {
            return;
        }

        switch (opType) {
            case INSERT_OP:
                processInsert(tapEvent, recordEvent, mergedRecords, pkMigration);
                break;
            case UPDATE_OP:
                processUpdate(tapEvent, recordEvent, mergedRecords, pkMigration);
                break;
            case DELETE_OP:
                processDelete(tapEvent, recordEvent, mergedRecords, pkMigration);
                break;
        }
    }

    /**
     * 处理 INSERT：创建新生命周期记录，并初始化 finalState / finalOp。
     */
    private static void processInsert(TapdataEvent tapEvent,
                                      TapRecordEvent recordEvent,
                                      Map<Object, MergedRecord> mergedRecords,
                                      Map<Object, Object> pkMigration) {
        Object initialPk = extractPrimaryKey(recordEvent);
        if (initialPk == null) {
            String key = computeKey(recordEvent);
            mergedRecords.put(key, new MergedRecord(key, key));
            return;
        }

        MergedRecord record = new MergedRecord(initialPk, initialPk);
        // 直接保存原始 TapdataEvent
        record.getOperations().add(tapEvent);
        // 保存 finalStateEvent
        record.setFinalStateEvent(recordEvent);
        record.setFinalOp(INSERT_OP);

        mergedRecords.put(initialPk, record);
        pkMigration.put(initialPk, initialPk);
    }

    /**
     * 处理 UPDATE：记录字段变化；若主键变化，则把当前生命周期标记为 DELETE_INSERT。
     */
    private static void processUpdate(TapdataEvent tapEvent,
                                      TapRecordEvent recordEvent,
                                      Map<Object, MergedRecord> mergedRecords,
                                      Map<Object, Object> pkMigration) {
        Object oldPk = extractBeforePrimaryKey(recordEvent);
        if (oldPk == null) {
            oldPk = extractPrimaryKey(recordEvent);
        }

        if (oldPk == null || !pkMigration.containsKey(oldPk)) {
            return;
        }

        Object initialPk = pkMigration.get(oldPk);
        MergedRecord record = mergedRecords.get(initialPk);
        if (record == null) return;

        // 直接保存原始 TapdataEvent
        record.getOperations().add(tapEvent);
        // 更新 finalStateEvent
        record.setFinalStateEvent(recordEvent);

        Object newPk = extractAfterPrimaryKey(recordEvent);
        if (newPk == null) {
            newPk = oldPk;
        }

        if (!Objects.equals(oldPk, newPk)) {
            pkMigration.remove(oldPk);
            pkMigration.put(newPk, initialPk);
            record.setCurrentPk(newPk);
            record.setFinalOp(DELETE_INSERT_OP);
        } else {
            record.setFinalOp(UPDATE_OP);
        }
    }

    /**
     * 处理 DELETE：移除生命周期记录，表示这条记录最终消失。
     */
    private static void processDelete(TapdataEvent tapEvent,
                                      TapRecordEvent recordEvent,
                                      Map<Object, MergedRecord> mergedRecords,
                                      Map<Object, Object> pkMigration) {
        Object oldPk = extractPrimaryKey(recordEvent);
        if (oldPk == null) {
            oldPk = extractBeforePrimaryKey(recordEvent);
        }

        if (oldPk == null || !pkMigration.containsKey(oldPk)) {
            return;
        }

        Object initialPk = pkMigration.get(oldPk);
        MergedRecord record = mergedRecords.get(initialPk);
        if (record == null) return;

        // 直接保存原始 TapdataEvent
        record.getOperations().add(tapEvent);
        record.setFinalOp(DELETE_OP);

        pkMigration.remove(oldPk);
        mergedRecords.remove(initialPk);
    }

    private static void processEvent(TapdataEvent tapEvent,
                                     TapRecordEvent recordEvent,
                                     Map<Object, MergedRecord> mergedRecords,
                                     Map<Object, Object> pkMigration,
                                     String tableName,
                                     NodeSchemaInfo schema) {
        String opType;
        if (recordEvent instanceof TapInsertRecordEvent) {
            opType = INSERT_OP;
        } else if (recordEvent instanceof TapUpdateRecordEvent) {
            opType = UPDATE_OP;
        } else if (recordEvent instanceof TapDeleteRecordEvent) {
            opType = DELETE_OP;
        } else {
            return;
        }

        switch (opType) {
            case INSERT_OP:
                processInsert(tapEvent, recordEvent, mergedRecords, pkMigration, tableName, schema);
                break;
            case UPDATE_OP:
                processUpdate(tapEvent, recordEvent, mergedRecords, pkMigration, tableName, schema);
                break;
            case DELETE_OP:
                processDelete(tapEvent, recordEvent, mergedRecords, pkMigration, tableName, schema);
                break;
        }
    }

    private static void processInsert(TapdataEvent tapEvent,
                                      TapRecordEvent recordEvent,
                                      Map<Object, MergedRecord> mergedRecords,
                                      Map<Object, Object> pkMigration,
                                      String tableName,
                                      NodeSchemaInfo schema) {
        Object initialPk = extractPrimaryKey(recordEvent, tableName, schema);
        if (initialPk == null) {
            String key = computeKey(recordEvent);
            mergedRecords.put(key, new MergedRecord(key, key));
            return;
        }

        MergedRecord record = new MergedRecord(initialPk, initialPk);
        record.getOperations().add(tapEvent);
        record.setFinalStateEvent(recordEvent);
        record.setFinalOp(INSERT_OP);

        mergedRecords.put(initialPk, record);
        pkMigration.put(initialPk, initialPk);
    }

    private static void processUpdate(TapdataEvent tapEvent,
                                      TapRecordEvent recordEvent,
                                      Map<Object, MergedRecord> mergedRecords,
                                      Map<Object, Object> pkMigration,
                                      String tableName,
                                      NodeSchemaInfo schema) {
        Object oldPk = extractBeforePrimaryKey(recordEvent, tableName, schema);
        if (oldPk == null) {
            oldPk = extractPrimaryKey(recordEvent, tableName, schema);
        }

        if (oldPk == null || !pkMigration.containsKey(oldPk)) {
            return;
        }

        Object initialPk = pkMigration.get(oldPk);
        MergedRecord record = mergedRecords.get(initialPk);
        if (record == null) return;

        record.getOperations().add(tapEvent);
        record.setFinalStateEvent(recordEvent);

        Object newPk = extractAfterPrimaryKey(recordEvent, tableName, schema);
        if (newPk == null) {
            newPk = oldPk;
        }

        if (!Objects.equals(oldPk, newPk)) {
            pkMigration.remove(oldPk);
            pkMigration.put(newPk, initialPk);
            record.setCurrentPk(newPk);
            record.setFinalOp(DELETE_INSERT_OP);
        } else {
            record.setFinalOp(UPDATE_OP);
        }
    }

    private static void processDelete(TapdataEvent tapEvent,
                                      TapRecordEvent recordEvent,
                                      Map<Object, MergedRecord> mergedRecords,
                                      Map<Object, Object> pkMigration,
                                      String tableName,
                                      NodeSchemaInfo schema) {
        Object oldPk = extractPrimaryKey(recordEvent, tableName, schema);
        if (oldPk == null) {
            oldPk = extractBeforePrimaryKey(recordEvent, tableName, schema);
        }

        if (oldPk == null || !pkMigration.containsKey(oldPk)) {
            return;
        }

        Object initialPk = pkMigration.get(oldPk);
        MergedRecord record = mergedRecords.get(initialPk);
        if (record == null) return;

        record.getOperations().add(tapEvent);
        record.setFinalOp(DELETE_OP);

        pkMigration.remove(oldPk);
        mergedRecords.remove(initialPk);
    }

    /**
     * 从 TapRecordEvent 中提取主键，优先 after，其次 before。
     */
    private static Object extractPrimaryKey(TapRecordEvent recordEvent) {
        Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
        if (after != null) {
            for (String pkCandidate : PK_CANDIDATES) {
                Object pk = after.get(pkCandidate);
                if (pk != null) {
                    return pk;
                }
            }
        }
        Map<String, Object> before = TapEventUtil.getBefore(recordEvent);
        if (before != null) {
            for (String pkCandidate : PK_CANDIDATES) {
                Object pk = before.get(pkCandidate);
                if (pk != null) {
                    return pk;
                }
            }
        }
        return null;
    }

    /**
     * 从 before 数据中提取旧主键。
     */
    private static Object extractBeforePrimaryKey(TapRecordEvent recordEvent) {
        Map<String, Object> before = TapEventUtil.getBefore(recordEvent);
        if (before != null) {
            for (String pkCandidate : PK_CANDIDATES) {
                Object pk = before.get(pkCandidate);
                if (pk != null) {
                    return pk;
                }
            }
        }
        return null;
    }

    /**
     * 从 after 数据中提取新主键。
     */
    private static Object extractAfterPrimaryKey(TapRecordEvent recordEvent) {
        Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
        if (after != null) {
            for (String pkCandidate : PK_CANDIDATES) {
                Object pk = after.get(pkCandidate);
                if (pk != null) {
                    return pk;
                }
            }
        }
        return null;
    }

    /**
     * 当没有可识别主键时，为 TapRecordEvent 生成兜底 key。
     */
    private static String computeKey(TapRecordEvent recordEvent) {
        Map<String, Object> data = TapEventUtil.getAfter(recordEvent);
        if (data == null) {
            data = TapEventUtil.getBefore(recordEvent);
        }
        if (data == null) {
            return "_hash:" + System.identityHashCode(recordEvent);
        }
        for (String pk : PK_CANDIDATES) {
            if (data.containsKey(pk)) {
                Object v = data.get(pk);
                if (v != null) return pk + ":" + v;
            }
        }
        try {
            return "_full:" + MAPPER.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            return "_hash:" + System.identityHashCode(recordEvent);
        }
    }

    /**
     * 从 TapRecordEvent 中提取主键，使用 NodeSchemaInfo 获取真实主键信息。
     */
    private static Object extractPrimaryKey(TapRecordEvent recordEvent,
                                            String tableName,
                                            NodeSchemaInfo schema) {
        List<String> primaryKeys = schema.getPrimaryKeys();
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalStateException("No primary keys defined for table: " + tableName);
        }

        // 优先使用第一个主键
        String pkField = primaryKeys.get(0);

        Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
        if (after != null) {
            Object pk = after.get(pkField);
            if (pk != null) {
                return pk;
            }
        }

        Map<String, Object> before = TapEventUtil.getBefore(recordEvent);
        if (before != null) {
            Object pk = before.get(pkField);
            if (pk != null) {
                return pk;
            }
        }

        throw new IllegalStateException("Cannot find primary key value for field: " + pkField +
                                        " in table: " + tableName);
    }

    /**
     * 从 before 数据中提取旧主键，使用 NodeSchemaInfo 获取真实主键信息。
     */
    private static Object extractBeforePrimaryKey(TapRecordEvent recordEvent,
                                                  String tableName,
                                                  NodeSchemaInfo schema) {
        List<String> primaryKeys = schema.getPrimaryKeys();
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalStateException("No primary keys defined for table: " + tableName);
        }

        String pkField = primaryKeys.get(0);
        Map<String, Object> before = TapEventUtil.getBefore(recordEvent);
        if (before != null) {
            Object pk = before.get(pkField);
            if (pk != null) {
                return pk;
            }
        }

        return null;
    }

    /**
     * 从 after 数据中提取新主键，使用 NodeSchemaInfo 获取真实主键信息。
     */
    private static Object extractAfterPrimaryKey(TapRecordEvent recordEvent,
                                                 String tableName,
                                                 NodeSchemaInfo schema) {
        List<String> primaryKeys = schema.getPrimaryKeys();
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalStateException("No primary keys defined for table: " + tableName);
        }

        String pkField = primaryKeys.get(0);
        Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
        if (after != null) {
            Object pk = after.get(pkField);
            if (pk != null) {
                return pk;
            }
        }

        return null;
    }

    /**
     * 简单 last-wins 去重：用于保守场景的兜底处理。
     */
    public static List<Map<String, Object>> mergeLastWins(List<TapdataEvent> events) {
        if (events == null || events.isEmpty()) {
            return Collections.emptyList();
        }

        Map<Object, Map<String, Object>> lastByKey = new LinkedHashMap<>();

        for (TapdataEvent tapEvent : events) {
            TapEvent tapEventInner = tapEvent.getTapEvent();
            if (tapEventInner instanceof TapRecordEvent recordEvent) {
                Object pk = extractPrimaryKey(recordEvent);
                if (pk != null) {
                    Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
                    if (after != null) {
                        lastByKey.put(pk, after);
                    }
                }
            }
        }

        return new ArrayList<>(lastByKey.values());
    }

    /**
     * 简单 last-wins 去重：用于保守场景的兜底处理。
     * 使用 NodeSchemaInfo 获取真实主键信息。
     *
     * @param events TapdataEvent 列表
     * @param tableName 表名
     * @param schema 表的 schema 信息
     * @return 去重后的记录列表
     */
    public static List<Map<String, Object>> mergeLastWins(List<TapdataEvent> events,
                                                           String tableName,
                                                           NodeSchemaInfo schema) {
        if (events == null || events.isEmpty()) {
            return Collections.emptyList();
        }

        Map<Object, Map<String, Object>> lastByKey = new LinkedHashMap<>();

        for (TapdataEvent tapEvent : events) {
            TapEvent tapEventInner = tapEvent.getTapEvent();
            if (tapEventInner instanceof TapRecordEvent recordEvent) {
                Object pk = extractPrimaryKey(recordEvent, tableName, schema);
                if (pk != null) {
                    Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
                    if (after != null) {
                        lastByKey.put(pk, after);
                    }
                }
            }
        }

        return new ArrayList<>(lastByKey.values());
    }


}

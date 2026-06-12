package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;

import java.util.*;

/**
 * Advanced smart merger implementing full merge_events_smart (M3) from ADR D4.
 *
 * <h3>Refactored per design document 2026-06-07</h3>
 * <p>MergedRecord now directly carries beforeRows/afterRows/mainTableBeforePks/mainTableAfterPks,
 * so consumers (AffectedKeyCalculator, HazelcastDuckDbSqlNode) no longer need to re-traverse
 * the original operations.</p>
 *
 * <h3>Merge minimization algorithm:</h3>
 * <ul>
 *   <li>Group events by (tableName, initialPk)</li>
 *   <li>Track first before-state and last after-state</li>
 *   <li>Compress beforeRows to minimal set needed to delete old wide table rows</li>
 *   <li>Independent PK sets for wide table delta calculation</li>
 * </ul>
 *
 * @see <a href="docs/adr/decision-4-smart-merge.md">ADR D4</a>
 */
public class SmartMerger {

    private static final String INSERT_OP = "INSERT";
    private static final String UPDATE_OP = "UPDATE";
    private static final String DELETE_OP = "DELETE";

    /**
     * Represents a single logical afterRecord's full lifecycle through REPLAY phase.
     *
     * <h3>Fields (per 2026-06-07 design):</h3>
     * <ul>
     *   <li>{@code beforeRows} — rows to DELETE from DuckDB staging table (old data)</li>
     *   <li>{@code afterRows} — rows to INSERT into DuckDB staging table (new data)</li>
     *   <li>{@code mainTableBeforePks} — PKs to REMOVE from wide table</li>
     *   <li>{@code mainTableAfterPks} — PKs to ADD to wide table</li>
     *   <li>{@code tableName} — source table name</li>
     *   <li>{@code schema} — NodeSchemaInfo for type normalization</li>
     * </ul>
     */
    public static class MergedRecord {
        // ---- 6-field design (per 2026-06-07 refactor) ----
        private final List<Map<String, Object>> beforeRows = new ArrayList<>();
        private final Map<String, Map<String, Object>> afterRows  = new LinkedHashMap<>();
        private final Set<Object> mainTableBeforePks = new LinkedHashSet<>();
        private final Set<Object> mainTableAfterPks  = new LinkedHashSet<>();
        private String tableName;
        private NodeSchemaInfo schema;

        // ---- Internal tracking fields ----
        private Object originalPk;  // Track original PK for PK migration

        // -- Accessors for new fields --

        public List<Map<String, Object>> getBeforeRows() { return beforeRows; }
        public Collection<Map<String, Object>> getAfterRows()  { return afterRows.values(); }
        public Set<Object> getMainTableBeforePks() { return mainTableBeforePks; }
        public Set<Object> getMainTableAfterPks()  { return mainTableAfterPks; }
        public String getTableName() { return tableName; }
        public void setTableName(String tableName) { this.tableName = tableName; }
        public NodeSchemaInfo getSchema() { return schema; }
        public void setSchema(NodeSchemaInfo schema) { this.schema = schema; }

        // -- afterRows Map accessors --
        public Map<String, Object> getAfterRow(String pk) { return afterRows.get(pk); }
        public void setAfterRow(String pk, Map<String, Object> row) { afterRows.put(pk, row); }
        public boolean hasAfterRow(String pk) { return afterRows.containsKey(pk); }

        // -- Convenience methods --

        /**
         * Get the final operation type based on beforeRows/afterRows state.
         * @return "INSERT", "UPDATE", or "DELETE"
         */
        public String getFinalOp() {
            if (afterRows.isEmpty() && !beforeRows.isEmpty()) {
                return "DELETE";
            } else if (!afterRows.isEmpty() && beforeRows.isEmpty()) {
                return "INSERT";
            } else if (!afterRows.isEmpty() && !beforeRows.isEmpty()) {
                return "UPDATE";
            }
            return "UNKNOWN";
        }

        /**
         * Get the initial PK (original PK before any changes).
         */
        public Object getInitialPk() {
            return originalPk;
        }

        /**
         * Get the current PK (from mainTableAfterPks).
         */
        public Object getCurrentPk() {
            return mainTableAfterPks.isEmpty() ? null : mainTableAfterPks.iterator().next();
        }

        /**
         * Get the final state (last after-state from afterRows).
         * With Map afterRows, returns the single current after-state.
         * @deprecated Use getAfterRows() instead for better performance.
         */
        public Map<String, Object> getFinalState() {
            if (afterRows.isEmpty()) {
                return null;
            }
            return afterRows.values().iterator().next();
        }

        /**
         * Get the original PK (for PK migration tracking).
         */
        public Object getOriginalPk() {
            return originalPk;
        }

        /**
         * Set the original PK (for PK migration tracking).
         */
        public void setOriginalPk(Object originalPk) {
            this.originalPk = originalPk;
        }
    }

    /**
     * MergedRecord 新增字段 + INSERT/UPDATE/DELETE 处理流程
     * INSERT事件分为两个阶段：
     *  - 无阶段 1 — cdc事件的before数据处理
     *  - 阶段 2 — dc事件的after数据处理：
     * 1. 按 PK 查找 `MergedRecord`，不存在则创建并初始化
     * 2. 关键：先从 `afterRows` 中根据 pk 取出旧值（若存在），将该旧值加入 `beforeRows`
     * 3. `afterRows.put(pk, afterState)` — Map 自动去重
     * 4. `mainTableAfterPks.add(pk)` — 与 afterRows 同频
     * 5. `mainTableBeforePks` 与 `beforeRows` 同频
     *
     * UPDATE 事件分为两个阶段：
     *  - 阶段 1 — cdc事件的before数据处理：
     * 1. 按 PK 查找 `MergedRecord`，不存在则创建并初始化
     * 2. 将当前行加入 `beforeRows`
     * 3. `mainTableBeforePks.add(pk)`
     *  - 阶段 2 — dc事件的after数据处理：
     * 1. 按 PK 查找 `MergedRecord`，不存在则创建并初始化
     * 2. 关键：先从 `afterRows` 中根据 pk 取出旧值（若存在），将该旧值加入 `beforeRows`
     * 3. `afterRows.put(pk, afterState)` — Map 自动去重
     * 4. `mainTableAfterPks.add(pk)` — 与 afterRows 同频
     * 5. `mainTableBeforePks` 与 `beforeRows` 同频
     *
     * DELETE 事件：
     * - 阶段 1（before 处理）：同 UPDATE + `afterRows.remove(pk)` + `mainTableAfterPks.remove(pk)`
     * - 无阶段 2（after 处理）
     *
     * @param tapEvents  only TapdataEvent wrappers (no non-afterRecord events)
     * @param tableName  source table name (used when all events belong to same table)
     * @param schema     NodeSchemaInfo for the table
     * @return list of MergedRecord, one per unique logical afterRecord
     */
    public static List<MergedRecord> mergeEventsSmart(List<TapdataEvent> tapEvents,
                                                       String tableName,
                                                       NodeSchemaInfo schema) {
        if (tapEvents == null || tapEvents.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, MergedRecord> mergedRecords = new LinkedHashMap<>();
        // ToDo pkMigration tracks: newPk → initialPk (for PK-change scenarios)
        Map<Object, Object> pkMigration = new HashMap<>();

        for (TapdataEvent tapEvent : tapEvents) {
            TapEvent tapEventInner = tapEvent.getTapEvent();
            if (tapEventInner instanceof TapRecordEvent) {
                processEvent(tapEvent, (TapRecordEvent) tapEventInner,
                            mergedRecords, pkMigration, tableName, schema);
            }
        }

        return new ArrayList<>(mergedRecords.values());
    }

    private static void processEvent(TapdataEvent tapEvent,
                                     TapRecordEvent recordEvent,
                                     Map<String, MergedRecord> mergedRecords,
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
                processAfter(recordEvent, mergedRecords, tableName, schema);
                break;
            case UPDATE_OP:
                processBefore(false, tapEvent, recordEvent, mergedRecords, pkMigration, tableName, schema);
                processAfter(recordEvent, mergedRecords, tableName, schema);
                break;
            case DELETE_OP:
                processBefore(true, tapEvent, recordEvent, mergedRecords, pkMigration, tableName, schema);
                break;
        }
    }

    /**
     * Handle INSERT event.
     *
     * <p>Lifecycle: (none) → INSERTED.
     * Only stage 2 (after data processing):
     * - Get old value from afterRows, add to beforeRows if not already present
     * - afterRows = Map with PK → inserted row (auto-dedup)
     * - mainTableAfterPks in sync with afterRows</p>
     */
    private static void processAfter(TapRecordEvent recordEvent,
                                     Map<String, MergedRecord> mergedRecords,
                                     String tableName,
                                     NodeSchemaInfo schema) {
        Object pk = extractAfterPrimaryKey(recordEvent, tableName, schema);
        String pkKey = pkAsString(pk);

        MergedRecord record = mergedRecords.get(pkKey);
        if (record == null) {
            mergedRecords.put(pkKey, new MergedRecord());
            record = mergedRecords.get(pkKey);
            record.setOriginalPk(pk);
            record.tableName = tableName;
            record.schema = schema;
        }

        // Stage 2: after data processing
        Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
        if (after != null && !after.isEmpty()) {
            // Step 2: Get old value from afterRows, add to beforeRows if not already present
            Map<String, Object> oldAfter = record.getAfterRow(pkKey);
            if (oldAfter != null) {
                record.beforeRows.add(oldAfter);
                record.mainTableBeforePks.add(pkKey);
            }

            // Step 3: Put new after state (Map auto-dedup)
            record.setAfterRow(pkKey, after);

            // Step 4: mainTableAfterPks in sync with afterRows
            record.mainTableAfterPks.add(pk);

        }
    }

    /**
     * Handle UPDATE or DELETE event.
     *
     * <p>For UPDATE: may change PK. BeforeRows = [old state], AfterRows = [new state].
     * For DELETE: BeforeRows = [deleted row], AfterRows = empty.</p>
     */
    private static void processBefore(boolean isDelete,
                                      TapdataEvent tapEvent,
                                      TapRecordEvent recordEvent,
                                      Map<String, MergedRecord> mergedRecords,
                                      Map<Object, Object> pkMigration,
                                      String tableName,
                                      NodeSchemaInfo schema) {
        // Determine which MergedRecord this event belongs to
        Object oldPk = extractBeforePrimaryKey(recordEvent, tableName, schema);
        String oldPkVal = pkAsString(oldPk);

        MergedRecord record = mergedRecords.get(oldPkVal);
        if (record == null) {
            // First time seeing this afterRecord with an UPDATE/DELETE (no prior INSERT in this batch)
            record = new MergedRecord();
            record.setOriginalPk(oldPk);
            record.tableName = tableName;
            record.schema = schema;
            mergedRecords.put(oldPkVal, record);
        }

        Map<String, Object> before = TapEventUtil.getBefore(recordEvent);

        // Stage 1: before data processing
        if (before != null && !before.isEmpty()) {
            record.beforeRows.add(before);
            record.mainTableBeforePks.add(oldPkVal);
        }

        if (isDelete) {
            // No stage 2 for DELETE: clear after state
            record.afterRows.remove(oldPkVal);
            record.mainTableAfterPks.remove(oldPkVal);
        }
    }

    // ==================== Helper methods ====================

    private static String pkAsString(Object pk) {
        if (pk == null) {
            throw new IllegalArgumentException("pk is null");
        }
        return pk.toString();
    }

    private static boolean objectsEqual(Object a, Object b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }

    /**
     * Extract primary key from after data (for INSERT and general cases).
     */
    private static Object extractPrimaryKey(TapRecordEvent recordEvent,
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
            if (pk != null) return pk;
        }

        Map<String, Object> before = TapEventUtil.getBefore(recordEvent);
        if (before != null) {
            Object pk = before.get(pkField);
            if (pk != null) return pk;
        }

        throw new IllegalStateException("Cannot find primary key value for field: " + pkField +
                                        " in table: " + tableName);
    }

    /**
     * Extract old primary key from before data (for UPDATE/DELETE).
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
            if (pk != null) return pk;
        }
        return null;
    }

    /**
     * Extract new primary key from after data (for UPDATE).
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
            if (pk != null) return pk;
        }
        return null;
    }

    /**
     * Simple last-wins dedup (conservative fallback).
     * Returns after-state rows keyed by PK (last event wins).
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

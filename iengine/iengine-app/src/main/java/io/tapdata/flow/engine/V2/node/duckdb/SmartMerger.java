package io.tapdata.flow.engine.V2.node.duckdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.stream.Collectors;

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
        private final List<Map<String, Object>> operations; // All mutations
        private Map<String, Object> finalState;   // Final field values after all updates
        private String finalOp;                   // 'INSERT' | 'UPDATE' | 'DELETE' | 'DELETE_INSERT'

        public MergedRecord(Object initialPk, Object currentPk) {
            this.initialPk = initialPk;
            this.currentPk = currentPk;
            this.operations = new ArrayList<>();
            this.finalState = new HashMap<>();
            this.finalOp = INSERT_OP;
        }

        public Object getInitialPk() { return initialPk; }
        public Object getCurrentPk() { return currentPk; }
        public void setCurrentPk(Object currentPk) { this.currentPk = currentPk; }
        public List<Map<String, Object>> getOperations() { return operations; }
        public Map<String, Object> getFinalState() { return finalState; }
        public void setFinalState(Map<String, Object> finalState) { this.finalState = finalState; }
        public String getFinalOp() { return finalOp; }
        public void setFinalOp(String finalOp) { this.finalOp = finalOp; }
    }

    /**
     * Simple last-wins deduplication (conservative first-step fallback).
     */
    public static List<Map<String, Object>> mergeLastWins(List<Map<String, Object>> events) {
        if (events == null || events.isEmpty()) return Collections.emptyList();

        // preserve insertion order of last seen keys
        Map<String, Map<String, Object>> lastByKey = new LinkedHashMap<>();

        for (Map<String, Object> ev : events) {
            String key = computeKey(ev);
            lastByKey.put(key, ev);
        }

        return new ArrayList<>(lastByKey.values());
    }

    /**
     * Advanced smart merge: merge_events_smart (M3) from ADR D4.
     *
     * @param tapEvents list of TapdataEvent with TapRecordEvent inside
     * @return list of MergedRecord, one per unique initial_pk
     */
    public static List<MergedRecord> mergeEventsSmart(List<Map<String, Object>> tapEvents) {
        if (tapEvents == null || tapEvents.isEmpty()) return Collections.emptyList();

        Map<Object, MergedRecord> mergedRecords = new LinkedHashMap<>(); // initial_pk → MergedRecord
        Map<Object, Object> pkMigration = new HashMap<>();                  // current_pk → initial_pk

        for (Map<String, Object> ev : tapEvents) {
            processEvent(ev, mergedRecords, pkMigration);
        }

        // Post-process: if final_pk == initial_pk, change DELETE_INSERT back to UPDATE
        for (MergedRecord record : mergedRecords.values()) {
            if (DELETE_INSERT_OP.equals(record.getFinalOp()) &&
                Objects.equals(record.getInitialPk(), record.getCurrentPk())) {
                record.setFinalOp(UPDATE_OP);
            }
        }

        return new ArrayList<>(mergedRecords.values());
    }

    /**
     * Process a single event and update merged records state.
     */
    private static void processEvent(Map<String, Object> ev,
                                     Map<Object, MergedRecord> mergedRecords,
                                     Map<Object, Object> pkMigration) {
        String opType = extractOpType(ev);
        if (opType == null) return;

        switch (opType.toUpperCase()) {
            case INSERT_OP:
                processInsert(ev, mergedRecords, pkMigration);
                break;
            case UPDATE_OP:
                processUpdate(ev, mergedRecords, pkMigration);
                break;
            case DELETE_OP:
                processDelete(ev, mergedRecords, pkMigration);
                break;
            default:
                // Unknown op type, treat as INSERT/last-wins
                processInsert(ev, mergedRecords, pkMigration);
        }
    }

    /**
     * Process INSERT event.
     */
    private static void processInsert(Map<String, Object> ev,
                                      Map<Object, MergedRecord> mergedRecords,
                                      Map<Object, Object> pkMigration) {
        Object initialPk = extractPk(ev);
        if (initialPk == null) {
            // No pk, treat as unique record (last-wins)
            String key = computeKey(ev);
            mergedRecords.put(key, new MergedRecord(key, key));
            return;
        }

        // Create or overwrite (last-wins for duplicates)
        MergedRecord record = new MergedRecord(initialPk, initialPk);
        record.getOperations().add(Map.of(
                "op", INSERT_OP,
                "value", ev
        ));
        record.setFinalState(new HashMap<>(ev));
        record.setFinalOp(INSERT_OP);

        mergedRecords.put(initialPk, record);
        pkMigration.put(initialPk, initialPk);
    }

    /**
     * Process UPDATE event.
     */
    private static void processUpdate(Map<String, Object> ev,
                                      Map<Object, MergedRecord> mergedRecords,
                                      Map<Object, Object> pkMigration) {
        Object oldPk = extractOldPk(ev);
        if (oldPk == null) oldPk = extractPk(ev);

        if (oldPk == null || !pkMigration.containsKey(oldPk)) {
            // Orphaned update, skip
            return;
        }

        Object initialPk = pkMigration.get(oldPk);
        MergedRecord record = mergedRecords.get(initialPk);
        if (record == null) return;

        // Extract updated/removed fields
        Map<String, Object> updatedFields = extractUpdatedFields(ev);
        Set<String> removedFields = extractRemovedFields(ev);

        // Check if pk is changing
        Object newPk = extractNewPk(ev, updatedFields, oldPk);

        Map<String, Object> op = new HashMap<>();
        op.put("op", UPDATE_OP);
        if (!Objects.equals(oldPk, newPk)) {
            op.put("old_pk", oldPk);
            op.put("new_pk", newPk);
        }
        op.put("fields", updatedFields);
        record.getOperations().add(op);

        // Update pk migration
        if (!Objects.equals(oldPk, newPk)) {
            pkMigration.remove(oldPk);
            pkMigration.put(newPk, initialPk);
            record.setCurrentPk(newPk);
            record.setFinalOp(DELETE_INSERT_OP);
        } else {
            record.setFinalOp(UPDATE_OP);
        }

        // Apply field updates to final_state
        if (updatedFields != null) {
            record.getFinalState().putAll(updatedFields);
        }
        if (removedFields != null) {
            for (String field : removedFields) {
                record.getFinalState().remove(field);
            }
        }
    }

    /**
     * Process DELETE event.
     */
    private static void processDelete(Map<String, Object> ev,
                                      Map<Object, MergedRecord> mergedRecords,
                                      Map<Object, Object> pkMigration) {
        Object oldPk = extractPk(ev);
        if (oldPk == null) return;

        if (!pkMigration.containsKey(oldPk)) {
            // Orphaned delete, skip
            return;
        }

        Object initialPk = pkMigration.get(oldPk);
        MergedRecord record = mergedRecords.get(initialPk);
        if (record == null) return;

        record.getOperations().add(Map.of("op", DELETE_OP));
        record.setFinalOp(DELETE_OP);

        pkMigration.remove(oldPk);
        mergedRecords.remove(initialPk);
    }

    /**
     * Extract operation type from event.
     */
    private static String extractOpType(Map<String, Object> ev) {
        Object op = ev.get("op");
        if (op instanceof String) {
            return (String) op;
        }
        // Fallback: check if looks like update/delete
        if (ev.containsKey("updatedFields") || ev.containsKey("o2")) {
            return UPDATE_OP;
        }
        return INSERT_OP;
    }

    /**
     * Extract primary key from event data.
     */
    private static Object extractPk(Map<String, Object> ev) {
        for (String pkField : PK_CANDIDATES) {
            if (ev.containsKey(pkField)) {
                return ev.get(pkField);
            }
        }
        // Check o2 (update filter)
        Object o2 = ev.get("o2");
        if (o2 instanceof Map) {
            return extractPk((Map<String, Object>) o2);
        }
        // Check o (delete filter)
        Object o = ev.get("o");
        if (o instanceof Map) {
            return extractPk((Map<String, Object>) o);
        }
        return null;
    }

    /**
     * Extract old pk from update event (from o2).
     */
    private static Object extractOldPk(Map<String, Object> ev) {
        Object o2 = ev.get("o2");
        if (o2 instanceof Map) {
            return extractPk((Map<String, Object>) o2);
        }
        return null;
    }

    /**
     * Extract new pk from update event (from updatedFields).
     */
    private static Object extractNewPk(Map<String, Object> ev,
                                       Map<String, Object> updatedFields,
                                       Object defaultPk) {
        if (updatedFields == null) return defaultPk;
        for (String pkField : PK_CANDIDATES) {
            if (updatedFields.containsKey(pkField)) {
                return updatedFields.get(pkField);
            }
        }
        return defaultPk;
    }

    /**
     * Extract updated fields from event.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractUpdatedFields(Map<String, Object> ev) {
        Object updated = ev.get("updatedFields");
        if (updated instanceof Map) {
            return new HashMap<>((Map<String, Object>) updated);
        }
        // Fallback: treat whole event as updated fields (minus pk)
        Map<String, Object> result = new HashMap<>(ev);
        PK_CANDIDATES.forEach(result::remove);
        return result;
    }

    /**
     * Extract removed fields from event.
     */
    @SuppressWarnings("unchecked")
    private static Set<String> extractRemovedFields(Map<String, Object> ev) {
        Object removed = ev.get("removedFields");
        if (removed instanceof Collection) {
            return new HashSet<>((Collection<String>) removed);
        }
        if (removed instanceof String) {
            return Set.of((String) removed);
        }
        return Collections.emptySet();
    }

    /**
     * Compute fallback key for events without known primary keys.
     */
    private static String computeKey(Map<String, Object> ev) {
        for (String pk : PK_CANDIDATES) {
            if (ev.containsKey(pk)) {
                Object v = ev.get(pk);
                if (v != null) return pk + ":" + v.toString();
            }
        }

        // fallback to full JSON serialization
        try {
            return "_full:" + MAPPER.writeValueAsString(ev);
        } catch (JsonProcessingException e) {
            // last resort: use identity hashcode
            return "_hash:" + System.identityHashCode(ev);
        }
    }

    /**
     * Convert MergedRecord back to flat map for downstream processing.
     */
    public static List<Map<String, Object>> mergedRecordsToMaps(List<MergedRecord> mergedRecords) {
        if (mergedRecords == null || mergedRecords.isEmpty()) return Collections.emptyList();

        return mergedRecords.stream()
                .map(MergedRecord::getFinalState)
                .collect(Collectors.toList());
    }
}

package io.tapdata.flow.engine.V2.node.duckdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Updater for materialized wide tables using incremental recalculation.
 *
 * Features:
 * - Recalculate affected rows from user's SELECT query
 * - Compare old vs new values to generate differential updates
 * - Support optional CDC changelog output
 * - Atomic transaction execution
 *
 * @deprecated Use {@link WideTableIncrementalUpdater} instead.
 *             This class will be removed in a future version.
 */
@Deprecated
public class IncrementalViewUpdater {

    private static final Logger logger = LogManager.getLogger(IncrementalViewUpdater.class);

    private final String wideTableName;
    private final String wideTablePrimaryKey;
    private final String userSql;
    private final boolean outputChangelogEnabled;
    private final DuckDbOperator operator;
    private final List<ChangelogOutputListener> changelogListeners = new ArrayList<>();

    /**
     * Listener interface for receiving generated changelog events.
     */
    @FunctionalInterface
    public interface ChangelogOutputListener {
        void onChangelogEvent(Map<String, Object> event);
    }

    public IncrementalViewUpdater(
            String wideTableName,
            String wideTablePrimaryKey,
            String userSql,
            boolean outputChangelogEnabled,
            DuckDbOperator operator
    ) {
        this.wideTableName = wideTableName;
        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.userSql = userSql;
        this.outputChangelogEnabled = outputChangelogEnabled;
        this.operator = operator;
    }

    /**
     * Add a listener to receive generated changelog events.
     */
    public void addChangelogListener(ChangelogOutputListener listener) {
        if (listener != null) {
            changelogListeners.add(listener);
        }
    }

    /**
     * Update the wide table incrementally for the given affected keys.
     *
     * @param affectedKeys The set of affected wide table primary keys
     * @return The number of rows updated
     */
    public int updateWideTable(Set<Object> affectedKeys) throws SQLException, IOException {
        if (affectedKeys == null || affectedKeys.isEmpty()) {
            logger.debug("No affected keys to update");
            return 0;
        }

        logger.info("Updating {} affected rows in wide table {}", affectedKeys.size(), wideTableName);

        final int[] totalUpdated = {0};

        // Execute all operations in a single transaction
        operator.executeInTransaction(() -> {
            // Step 1: Read old values from wide table
            Map<Object, Map<String, Object>> oldValues = readOldValues(affectedKeys);

            // Step 2: Recalculate new values using user's SQL
            Map<Object, Map<String, Object>> newValues = recalculateNewValues(affectedKeys);

            // Step 3: Compare old and new values to generate differential updates
            List<DiffOperation> diffs = compareValues(oldValues, newValues);

            // Step 4: Apply differential updates to wide table
            totalUpdated[0] = applyDiffs(diffs);

            // Step 5: If enabled, generate and output changelog events
            if (outputChangelogEnabled) {
                generateAndOutputChangelogs(diffs);
            }
        });

        logger.info("Successfully updated {} rows in wide table {}", totalUpdated[0], wideTableName);
        return totalUpdated[0];
    }

    /**
     * Read current values from the wide table for affected keys.
     */
    private Map<Object, Map<String, Object>> readOldValues(Set<Object> affectedKeys) throws SQLException {
        if (affectedKeys.isEmpty()) {
            return Collections.emptyMap();
        }

        String pkCsv = affectedKeys.stream()
                .map(pk -> {
                    if (pk instanceof String) {
                        return "'" + pk.toString().replace("'", "''") + "'";
                    }
                    return pk.toString();
                })
                .collect(Collectors.joining(","));

        String query = String.format(
                "SELECT * FROM %s WHERE %s IN (%s)",
                wideTableName,
                wideTablePrimaryKey,
                pkCsv
        );

        logger.debug("Reading old values: {}", query);
        return operator.queryForMap(query, wideTablePrimaryKey);
    }

    /**
     * Recalculate new values using the user's SQL query.
     */
    private Map<Object, Map<String, Object>> recalculateNewValues(Set<Object> affectedKeys) throws SQLException {
        if (affectedKeys.isEmpty()) {
            return Collections.emptyMap();
        }

        String pkCsv = affectedKeys.stream()
                .map(pk -> {
                    if (pk instanceof String) {
                        return "'" + pk.toString().replace("'", "''") + "'";
                    }
                    return pk.toString();
                })
                .collect(Collectors.joining(","));

        // Wrap user's SQL with a filter for affected keys
        // Assumptions:
        // 1. User's SQL has an alias or CTE we can wrap
        // 2. The wide table primary key is available in the result
        // For simplicity, we'll assume the user's SQL is a SELECT that can be wrapped
        String wrappedSql = String.format(
                "SELECT * FROM (%s) AS _wide_table WHERE %s IN (%s)",
                userSql,
                wideTablePrimaryKey,
                pkCsv
        );

        logger.debug("Recalculating new values with wrapped SQL: {}", wrappedSql);
        return operator.queryForMap(wrappedSql, wideTablePrimaryKey);
    }

    /**
     * Compare old and new values to generate differential operations.
     */
    private List<DiffOperation> compareValues(
            Map<Object, Map<String, Object>> oldValues,
            Map<Object, Map<String, Object>> newValues
    ) {
        List<DiffOperation> diffs = new ArrayList<>();
        Set<Object> allKeys = new LinkedHashSet<>();
        allKeys.addAll(oldValues.keySet());
        allKeys.addAll(newValues.keySet());

        for (Object pk : allKeys) {
            Map<String, Object> oldRow = oldValues.get(pk);
            Map<String, Object> newRow = newValues.get(pk);

            if (oldRow != null && newRow != null) {
                // Row exists in both: check if changed
                if (!areRowsEqual(oldRow, newRow)) {
                    diffs.add(new DiffOperation(DiffOperation.Type.UPDATE, pk, oldRow, newRow));
                }
            } else if (oldRow != null) {
                // Row exists in old but not in new: DELETE
                diffs.add(new DiffOperation(DiffOperation.Type.DELETE, pk, oldRow, null));
            } else if (newRow != null) {
                // Row exists in new but not in old: INSERT
                diffs.add(new DiffOperation(DiffOperation.Type.INSERT, pk, null, newRow));
            }
        }

        logger.debug("Generated {} diff operations for {} keys", diffs.size(), allKeys.size());
        return diffs;
    }

    /**
     * Check if two rows are equal (deep comparison).
     */
    private boolean areRowsEqual(Map<String, Object> row1, Map<String, Object> row2) {
        if (row1 == row2) return true;
        if (row1 == null || row2 == null) return false;
        if (row1.size() != row2.size()) return false;

        for (Map.Entry<String, Object> entry : row1.entrySet()) {
            String key = entry.getKey();
            Object val1 = entry.getValue();
            Object val2 = row2.get(key);

            if (!Objects.equals(val1, val2)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Apply differential operations to the wide table.
     */
    private int applyDiffs(List<DiffOperation> diffs) throws SQLException, IOException {
        int updatedCount = 0;

        for (DiffOperation diff : diffs) {
            switch (diff.type) {
                case INSERT:
                    if (diff.newRow != null) {
                        operator.batchInsert(wideTableName, Collections.singletonList(diff.newRow));
                        updatedCount++;
                    }
                    break;

                case UPDATE:
                    // For UPDATE: DELETE old then INSERT new
                    if (diff.oldRow != null) {
                        deleteRowByPk(diff.pk);
                    }
                    if (diff.newRow != null) {
                        operator.batchInsert(wideTableName, Collections.singletonList(diff.newRow));
                    }
                    updatedCount++;
                    break;

                case DELETE:
                    if (diff.oldRow != null) {
                        deleteRowByPk(diff.pk);
                        updatedCount++;
                    }
                    break;
            }
        }

        return updatedCount;
    }

    /**
     * Delete a row by primary key.
     */
    private void deleteRowByPk(Object pk) throws SQLException {
        String pkValueStr;
        if (pk instanceof String) {
            pkValueStr = "'" + pk.toString().replace("'", "''") + "'";
        } else {
            pkValueStr = pk.toString();
        }

        String deleteSql = String.format(
                "DELETE FROM %s WHERE %s = %s",
                wideTableName,
                wideTablePrimaryKey,
                pkValueStr
        );

        logger.debug("Deleting row: {}", deleteSql);
        operator.executeUpdate(deleteSql);
    }

    /**
     * Generate and output changelog events for the differential operations.
     */
    private void generateAndOutputChangelogs(List<DiffOperation> diffs) {
        for (DiffOperation diff : diffs) {
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("table", wideTableName);
            event.put("timestamp", System.currentTimeMillis());

            switch (diff.type) {
                case INSERT:
                    event.put("op", "INSERT");
                    event.put("after", diff.newRow);
                    break;

                case UPDATE:
                    event.put("op", "UPDATE");
                    event.put("before", diff.oldRow);
                    event.put("after", diff.newRow);
                    break;

                case DELETE:
                    event.put("op", "DELETE");
                    event.put("before", diff.oldRow);
                    break;
            }

            // Notify all listeners
            for (ChangelogOutputListener listener : changelogListeners) {
                try {
                    listener.onChangelogEvent(event);
                } catch (Exception e) {
                    logger.warn("Error notifying changelog listener", e);
                }
            }
        }
    }

    /**
     * Represents a single differential operation.
     */
    public static class DiffOperation {
        public enum Type { INSERT, UPDATE, DELETE }

        public final Type type;
        public final Object pk;
        public final Map<String, Object> oldRow;
        public final Map<String, Object> newRow;

        public DiffOperation(Type type, Object pk, Map<String, Object> oldRow, Map<String, Object> newRow) {
            this.type = type;
            this.pk = pk;
            this.oldRow = oldRow;
            this.newRow = newRow;
        }
    }

    public String getWideTableName() {
        return wideTableName;
    }

    public String getWideTablePrimaryKey() {
        return wideTablePrimaryKey;
    }
}

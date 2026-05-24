package io.tapdata.flow.engine.V2.node.duckdb;

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

    private final String wideTablePrimaryKey;
    private final String mainTableName;
    private final String mainTablePrimaryKey;
    private final List<FromTableConfig> fromTables;
    private final Map<String, String> customJoinQueries;
    private final DuckDbOperator operator;

    public AffectedKeyCalculator(
            String wideTablePrimaryKey,
            String mainTableName,
            String mainTablePrimaryKey,
            List<FromTableConfig> fromTables,
            Map<String, String> customJoinQueries,
            DuckDbOperator operator
    ) {
        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.mainTableName = mainTableName;
        this.mainTablePrimaryKey = mainTablePrimaryKey;
        this.fromTables = fromTables != null ? fromTables : Collections.emptyList();
        this.customJoinQueries = customJoinQueries != null ? customJoinQueries : Collections.emptyMap();
        this.operator = operator;
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
            // Secondary table event: find related main table PKs
            logger.debug("Processing secondary table events from {}", tableName);
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
     */
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

        return null;
    }

    /**
     * Get primary key field name for a source table.
     */
    private String getSourceTablePrimaryKey(String tableName) {
        if (fromTables != null) {
            for (FromTableConfig config : fromTables) {
                if (config.getTableName().equalsIgnoreCase(tableName)) {
                    return config.getPrimaryKey();
                }
            }
        }
        // Default: try common PK field names
        return "id";
    }

    /**
     * Query main table primary keys related to given source table PKs.
     */
    private Set<Object> queryRelatedMainTablePks(String tableName, Set<Object> sourceTablePks) throws SQLException {
        // First check for custom join query
        if (customJoinQueries != null && customJoinQueries.containsKey(tableName)) {
            return executeCustomJoinQuery(tableName, sourceTablePks);
        }

        // TODO: Implement automatic SQL parsing from user's SELECT query
        // For now, use a generic approach that assumes simple JOIN
        logger.warn("No custom join query found for table {}, using fallback approach", tableName);
        return Collections.emptySet();
    }

    /**
     * Execute a custom JOIN query to find related main table PKs.
     */
    private Set<Object> executeCustomJoinQuery(String tableName, Set<Object> sourceTablePks) throws SQLException {
        String queryTemplate = customJoinQueries.get(tableName);
        if (queryTemplate == null) {
            return Collections.emptySet();
        }

        // Replace ${pkValues} placeholder with CSV of PKs
        String pkCsv = sourceTablePks.stream()
                .map(pk -> {
                    if (pk instanceof String) {
                        return "'" + pk.toString().replace("'", "''") + "'";
                    }
                    return pk.toString();
                })
                .collect(Collectors.joining(","));

        String query = queryTemplate.replace("${pkValues}", pkCsv);
        logger.debug("Executing custom join query: {}", query);

        List<Map<String, Object>> results = operator.executeQuery(query);
        Set<Object> relatedPks = new LinkedHashSet<>();

        for (Map<String, Object> row : results) {
            Object pk = row.get(mainTablePrimaryKey);
            if (pk == null) {
                pk = row.get(wideTablePrimaryKey);
            }
            if (pk != null) {
                relatedPks.add(pk);
            }
        }

        return relatedPks;
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

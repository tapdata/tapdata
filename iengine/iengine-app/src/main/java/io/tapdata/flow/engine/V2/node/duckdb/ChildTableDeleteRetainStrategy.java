package io.tapdata.flow.engine.V2.node.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * 子表纯删除时，保留一条宽表记录并将该子表字段置空。
 */
public class ChildTableDeleteRetainStrategy implements WideTableDeleteAdjustmentStrategy {
    private static final Logger logger = LoggerFactory.getLogger(ChildTableDeleteRetainStrategy.class);

    @Override
    public boolean supports(WideTableDeleteAdjustmentContext context) {
        if (context == null || context.getSourceRegistry() == null || context.getSourceRegistry().isEmpty()) {
            return false;
        }
        WideTableSourceDescriptor descriptor = context.getSourceRegistry().getDescriptor(context.getSourceTableName());
        if (descriptor == null || descriptor.isMainTable()) {
            return false;
        }
        if (context.getBeforeKeys() == null || context.getBeforeKeys().isEmpty()) {
            return false;
        }
        if (!context.getAfterResults().isEmpty()) {
            return false;
        }
        return isPureDeleteBatch(context.getMergedRecords());
    }

    @Override
    public List<Map<String, Object>> adjust(WideTableDeleteAdjustmentContext context) throws SQLException {
        List<Map<String, Object>> existingWideRows = queryExistingWideRows(context);
        if (existingWideRows.isEmpty()) {
            return context.getAfterResults();
        }

        List<Map<String, Object>> retainedRows = new ArrayList<>(existingWideRows.size());
        Set<String> fieldsToNull = resolveFieldsToNull(context, existingWideRows.get(0));
        for (Map<String, Object> wideRow : existingWideRows) {
            if (wideRow == null || wideRow.isEmpty()) {
                continue;
            }
            Map<String, Object> retainedRow = new LinkedHashMap<>(wideRow);
            for (String field : fieldsToNull) {
                if (!context.getWideTablePrimaryKey().contains(field) && retainedRow.containsKey(field)) {
                    retainedRow.put(field, null);
                }
            }
            retainedRows.add(retainedRow);
        }

        logger.info("Retained {} wide-table rows for child delete on {}, nullified {} fields",
                retainedRows.size(), context.getSourceTableName(), fieldsToNull.size());
        return retainedRows;
    }

    private boolean isPureDeleteBatch(List<SmartMerger.MergedRecord> mergedRecords) {
        if (mergedRecords == null || mergedRecords.isEmpty()) {
            return false;
        }
        for (SmartMerger.MergedRecord mergedRecord : mergedRecords) {
            if (mergedRecord == null) {
                continue;
            }
            if (mergedRecord.getBeforeRows().isEmpty() || !mergedRecord.getAfterRows().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private List<Map<String, Object>> queryExistingWideRows(WideTableDeleteAdjustmentContext context) throws SQLException {
        String sql = String.format(
                "SELECT * FROM %s WHERE %s",
                WideTableDdlGenerator.quoteIdentifier(context.getWideTableName()),
                subWhere(context)
        );
        return context.getDuckDbOperator().executeQuery(sql);
    }

    String subWhere(WideTableDeleteAdjustmentContext context) {
        StringJoiner or = new StringJoiner(" OR ");
        context.getBeforeKeys()
                .forEach(map -> {
                    StringJoiner and = new StringJoiner(" AND ");
                    map.forEach((k, v) -> {
                        String value = DuckDbSqlValueFormatter.format(v);
                        and.add(k + " = " + value );
                    });
                    or.add(" ( " + and + " ) ");
                });
        return or.toString();
    }

    private Set<String> resolveFieldsToNull(WideTableDeleteAdjustmentContext context, Map<String, Object> retainedRow) {
        Set<String> fieldsToNull = new LinkedHashSet<>(context.getFieldOwnershipResolver().resolveOwnedFields(context.getSourceTableName()));
        if (!fieldsToNull.isEmpty()) {
            return fieldsToNull;
        }

        WideTableSourceDescriptor descriptor = context.getSourceRegistry().getDescriptor(context.getSourceTableName());
        if (descriptor == null || descriptor.getSchemaInfo() == null) {
            logger.warn("No source descriptor/schema found for {}, using full-row retention fallback", context.getSourceTableName());
            return fieldsToNull;
        }

        for (String fieldName : descriptor.getSchemaInfo().getFieldNames()) {
            if (retainedRow.containsKey(fieldName)) {
                fieldsToNull.add(fieldName);
            }
        }
        return fieldsToNull;
    }
}

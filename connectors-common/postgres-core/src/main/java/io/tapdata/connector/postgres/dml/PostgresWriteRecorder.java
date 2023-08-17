package io.tapdata.connector.postgres.dml;

import io.tapdata.common.dml.NormalWriteRecorder;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class PostgresWriteRecorder extends NormalWriteRecorder {

    public PostgresWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    @Override
    protected void insertUpdate(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        boolean containsNull = !hasPk && uniqueCondition.stream().anyMatch(v -> EmptyKit.isNull(before.get(v)));
        Map<String, Object> all = new HashMap<>(before);
        all.putAll(after);
        String preparedStatementKey = "|" + containsNull;
        if (preparedStatementKey.equals(this.preparedStatementKey)) {
            preparedStatement = preparedStatementMap.get(preparedStatementKey);
        } else {
            if (EmptyKit.isNull(this.preparedStatementKey)) {
                preparedStatement = connection.prepareStatement(getInsertUpdateSql(containsNull));
                preparedStatementMap.put(preparedStatementKey, preparedStatement);
            } else {
                executeBatch(listResult);
                preparedStatement = preparedStatementMap.get(preparedStatementKey);
                if (EmptyKit.isNull(preparedStatement)) {
                    preparedStatement = connection.prepareStatement(getInsertUpdateSql(containsNull));
                    preparedStatementMap.put(preparedStatementKey, preparedStatement);
                }
            }
            this.preparedStatementKey = preparedStatementKey;
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : updatedColumn) {
            preparedStatement.setObject(pos++, filterValue(all.get(key), columnTypeMap.get(key)));
        }
        if (!containsNull) {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, filterValue(before.get(key), columnTypeMap.get(key)));
            }
        } else {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, filterValue(before.get(key), columnTypeMap.get(key)));
                preparedStatement.setObject(pos++, filterValue(before.get(key), columnTypeMap.get(key)));
            }
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterValue(all.get(key), columnTypeMap.get(key)));
        }
    }

    protected String getInsertUpdateSql(boolean containsNull) {
        if (!containsNull) {
            return "WITH upsert AS (UPDATE " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " SET " + updatedColumn.stream().map(k -> escapeChar + k + escapeChar + "=?")
                    .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> escapeChar + k + escapeChar + "=?")
                    .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " ("
                    + allColumn.stream().map(k -> escapeChar + k + escapeChar).collect(Collectors.joining(", ")) + ") SELECT "
                    + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
        } else {
            return "WITH upsert AS (UPDATE " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " SET " + updatedColumn.stream().map(k -> escapeChar + k + escapeChar + "=?")
                    .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "(" + escapeChar + k + escapeChar + "=? OR (" + escapeChar + k + escapeChar + " IS NULL AND ?::text IS NULL))")
                    .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " ("
                    + allColumn.stream().map(k -> escapeChar + k + escapeChar).collect(Collectors.joining(", ")) + ") SELECT "
                    + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
        }
    }

    @Override
    protected Object filterValue(Object value, String dataType) {
        if (EmptyKit.isNull(value)) {
            return null;
        }
        if (value instanceof String) {
            return ((String) value).replace("\u0000", "");
        }
        return value;
    }
}

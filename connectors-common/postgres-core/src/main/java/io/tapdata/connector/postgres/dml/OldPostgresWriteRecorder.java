package io.tapdata.connector.postgres.dml;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;

public class OldPostgresWriteRecorder extends PostgresWriteRecorder {

    public OldPostgresWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    @Override
    protected void upsert(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        boolean containsNull = !hasPk && uniqueCondition.stream().anyMatch(v -> EmptyKit.isNull(after.get(v)));
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
            preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
        }
        if (!containsNull) {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
            }
        } else {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
                preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
            }
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
        }
    }

    @Override
    protected void insertIgnore(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        boolean containsNull = !hasPk && uniqueCondition.stream().anyMatch(v -> EmptyKit.isNull(after.get(v)));
        String preparedStatementKey = "|" + containsNull;
        if (preparedStatementKey.equals(this.preparedStatementKey)) {
            preparedStatement = preparedStatementMap.get(preparedStatementKey);
        } else {
            if (EmptyKit.isNull(this.preparedStatementKey)) {
                preparedStatement = connection.prepareStatement(getInsertIgnoreSql(containsNull));
                preparedStatementMap.put(preparedStatementKey, preparedStatement);
            } else {
                executeBatch(listResult);
                preparedStatement = preparedStatementMap.get(preparedStatementKey);
                if (EmptyKit.isNull(preparedStatement)) {
                    preparedStatement = connection.prepareStatement(getInsertIgnoreSql(containsNull));
                    preparedStatementMap.put(preparedStatementKey, preparedStatement);
                }
            }
            this.preparedStatementKey = preparedStatementKey;
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
        }
        if (!containsNull) {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
            }
        } else {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
                preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
            }
        }
    }

    protected String getInsertIgnoreSql(boolean containsNull) {
        if (!containsNull) {
            return "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                    + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT 1 FROM \"" + schema + "\".\"" + tapTable.getId()
                    + "\"  WHERE " + uniqueCondition.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")) + " )";
        } else {
            return "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                    + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT 1 FROM \"" + schema + "\".\"" + tapTable.getId()
                    + "\"  WHERE " + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))").collect(Collectors.joining(" AND ")) + " )";
        }
    }
}

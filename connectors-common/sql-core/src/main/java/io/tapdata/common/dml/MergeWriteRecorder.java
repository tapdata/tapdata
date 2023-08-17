package io.tapdata.common.dml;

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

public class MergeWriteRecorder extends NormalWriteRecorder {

    public MergeWriteRecorder(Connection connection, TapTable tapTable, String schema) {
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
                preparedStatement = connection.prepareStatement(getUpsertSql(containsNull));
                preparedStatementMap.put(preparedStatementKey, preparedStatement);
            } else {
                executeBatch(listResult);
                preparedStatement = preparedStatementMap.get(preparedStatementKey);
                if (EmptyKit.isNull(preparedStatement)) {
                    preparedStatement = connection.prepareStatement(getUpsertSql(containsNull));
                    preparedStatementMap.put(preparedStatementKey, preparedStatement);
                }
            }
            this.preparedStatementKey = preparedStatementKey;
        }
        preparedStatement.clearParameters();
        //make params
        int pos = 1;
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
        for (String key : allColumn.stream().filter(col -> !uniqueCondition.contains(col)).collect(Collectors.toList())) {
            preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
        }
    }

    protected String getUpsertSql(boolean containsNull) {
        String allColumnString = allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(","));
        String insertValue = "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ";
        if (!containsNull) {
            return "MERGE INTO " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " USING " + getSystemVirtualTable() + " ON ("
                    + uniqueCondition.stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(" AND "))
                    + ")" + (allColumn.size() == uniqueCondition.size() ? "" : (" WHEN MATCHED THEN UPDATE SET " + allColumn.stream().filter(col -> !uniqueCondition.contains(col))
                    .map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(", ")))) + " WHEN NOT MATCHED THEN INSERT(" + allColumnString + ") " + insertValue;
        } else {
            return "MERGE INTO " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " USING " + getSystemVirtualTable() + " ON ("
                    + uniqueCondition.stream().map(k -> "(" + escapeChar + k + escapeChar + "=? OR (" + escapeChar + k + escapeChar + " IS NULL AND ? IS NULL))").collect(Collectors.joining(" AND "))
                    + ")" + (allColumn.size() == uniqueCondition.size() ? "" : (" WHEN MATCHED THEN UPDATE SET " + allColumn.stream().filter(col -> !uniqueCondition.contains(col))
                    .map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(", ")))) + " WHEN NOT MATCHED THEN INSERT(" + allColumnString + ") " + insertValue;
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
        //make params
        int pos = 1;
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

    protected String getInsertIgnoreSql(boolean containsNull) {
        String allColumnString = allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(","));
        String insertValue = "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ";
        if (!containsNull) {
            return "MERGE INTO " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " USING " + getSystemVirtualTable() + " ON ("
                    + uniqueCondition.stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(" AND "))
                    + ") WHEN NOT MATCHED THEN INSERT(" + allColumnString + ") " + insertValue;
        } else {
            return "MERGE INTO " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " USING " + getSystemVirtualTable() + " ON ("
                    + uniqueCondition.stream().map(k -> "(" + escapeChar + k + escapeChar + "=? OR (" + escapeChar + k + escapeChar + " IS NULL AND ? IS NULL))").collect(Collectors.joining(" AND "))
                    + ") WHEN NOT MATCHED THEN INSERT(" + allColumnString + ") " + insertValue;
        }
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
                preparedStatement = connection.prepareStatement(getUpsertSql(containsNull));
                preparedStatementMap.put(preparedStatementKey, preparedStatement);
            } else {
                executeBatch(listResult);
                preparedStatement = preparedStatementMap.get(preparedStatementKey);
                if (EmptyKit.isNull(preparedStatement)) {
                    preparedStatement = connection.prepareStatement(getUpsertSql(containsNull));
                    preparedStatementMap.put(preparedStatementKey, preparedStatement);
                }
            }
            this.preparedStatementKey = preparedStatementKey;
        }
        preparedStatement.clearParameters();
        //make params
        int pos = 1;
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
        for (String key : allColumn.stream().filter(col -> !uniqueCondition.contains(col)).collect(Collectors.toList())) {
            preparedStatement.setObject(pos++, filterValue(all.get(key), columnTypeMap.get(key)));
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterValue(all.get(key), columnTypeMap.get(key)));
        }
    }

    protected String getSystemVirtualTable() {
        return "dual";
    }
}

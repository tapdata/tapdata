package io.tapdata.connector.postgres;

import io.tapdata.common.WriteRecorder;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;

public class PostgresWriteRecorder extends WriteRecorder {

    private boolean needNull = false;

    public PostgresWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    public PostgresWriteRecorder(Connection connection, TapTable tapTable, String schema, boolean hasUnique) {
        super(connection, tapTable, schema);
        uniqueConditionIsIndex = uniqueConditionIsIndex && hasUnique;
    }

    @Override
    public void addInsertBatch(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            if (Integer.parseInt(version) > 90500 && uniqueConditionIsIndex) {
                if (insertPolicy.equals("ignore_on_exists")) {
                    conflictIgnoreInsert(after);
                } else {
                    conflictUpdateInsert(after);
                }
            } else {
                if (insertPolicy.equals("ignore_on_exists")) {
                    notExistsInsert(after);
                } else {
                    if (hasPk) {
                        withUpdateInsertWithoutNull(after);
                    } else {
                        if (uniqueCondition.stream().anyMatch(k -> EmptyKit.isNull(after.get(k)))) {
                            if (!needNull) {
                                executeBatch(listResult);
                                preparedStatement = null;
                            }
                            needNull = true;
                            withUpdateInsertWithNull(after);
                        } else {
                            if (needNull) {
                                executeBatch(listResult);
                                preparedStatement = null;
                            }
                            needNull = false;
                            withUpdateInsertWithoutNull(after);
                        }
                    }
                }
            }
        } else {
            justInsert(after);
        }
        preparedStatement.addBatch();
    }

    //on conflict
    private void conflictUpdateInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") " +
                    "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON CONFLICT("
                    + uniqueCondition.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", "))
                    + ") DO UPDATE SET " + allColumn.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", "));
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
    }

    private void conflictIgnoreInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") " +
                    "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON CONFLICT("
                    + uniqueCondition.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", "))
                    + ") DO NOTHING ";
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
    }

    //with update
    private void withUpdateInsertWithoutNull(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + updatedColumn.stream().map(k -> "\"" + k + "\"=?")
                    .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "\"" + k + "\"=?")
                    .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                    + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : updatedColumn) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
        for (String key : uniqueCondition) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
    }

    //with update
    private void withUpdateInsertWithNull(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + updatedColumn.stream().map(k -> "\"" + k + "\"=?")
                    .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                    .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                    + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : updatedColumn) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
        for (String key : uniqueCondition) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
    }

    //insert not exists
    private void notExistsInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql;
            if (hasPk) {
                insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT 1 FROM \"" + schema + "\".\"" + tapTable.getId()
                        + "\"  WHERE " + uniqueCondition.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")) + " )";
            } else {
                insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT 1 FROM \"" + schema + "\".\"" + tapTable.getId()
                        + "\"  WHERE " + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))").collect(Collectors.joining(" AND ")) + " )";
            }
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
        if (hasPk) {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
            }
        } else {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
                preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
            }
        }
    }

    //just insert
    private void justInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") " +
                    "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ";
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
    }

    @Override
    public void addUpdateBatch(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        if (updatePolicy.equals(ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS)) {
            insertUpdate(after, getBeforeForUpdate(after, before, listResult));
        } else {
            justUpdate(after, getBeforeForUpdate(after, before, listResult));
        }
        preparedStatement.addBatch();
    }

    protected void justUpdate(Map<String, Object> after, Map<String, Object> before) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            if (hasPk) {
                preparedStatement = connection.prepareStatement("UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " +
                        after.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " +
                        after.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : after.keySet()) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
        dealNullBefore(before, pos);
    }

    private void insertUpdate(Map<String, Object> after, Map<String, Object> before) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String updateSql;
            if (hasPk) {
                updateSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + updatedColumn.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            } else {
                updateSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + updatedColumn.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                        .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            }
            preparedStatement = connection.prepareStatement(updateSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : updatedColumn) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
        if (hasPk) {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, before.get(key));
            }
        } else {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, before.get(key));
                preparedStatement.setObject(pos++, before.get(key));
            }
        }
        dealNullBefore(before, pos);
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
    }

    @Override
    public void addDeleteBatch(Map<String, Object> before) throws SQLException {
        if (EmptyKit.isEmpty(before)) {
            return;
        }
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            before.keySet().removeIf(k -> !uniqueCondition.contains(k));
        }
        if (EmptyKit.isNull(preparedStatement)) {
            if (hasPk) {
                preparedStatement = connection.prepareStatement("DELETE FROM \"" + schema + "\".\"" + tapTable.getId() + "\" WHERE " +
                        before.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("DELETE FROM \"" + schema + "\".\"" + tapTable.getId() + "\" WHERE " +
                        before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        dealNullBefore(before, 1);
        preparedStatement.addBatch();
    }

    private Object filterInvalid(Object obj) {
        if (EmptyKit.isNull(obj)) {
            return null;
        }
        if (obj instanceof String) {
            return ((String) obj).replace("\u0000", "");
        }
        return obj;
    }
}
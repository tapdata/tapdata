package io.tapdata.connector.yashandb;

import io.tapdata.common.WriteRecorder;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Author:Skeet
 * Date: 2023/5/25
 **/
public class YashandbWriteRecorder extends WriteRecorder {
    public YashandbWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    public static final String DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS = "insert_on_nonexists";

    @Override
    public void addInsertBatch(Map<String, Object> after) throws SQLException {
        //after is empty will be skipped
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        justInsert(after);

        preparedStatement.addBatch();
    }

    private void justInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String allColumnString = allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(","));
            String insertHead = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" (" + allColumnString + ") ";
            String insertValue = "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ";
            String insertSql = insertHead + insertValue;
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }
    @Override
    public void addUpdateBatch(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        if (updatePolicy.equals(DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS)) {
            justInsertUpdate(after,getBeforeForUpdate(after, before, listResult), listResult);
        } else {
            justUpdate(after, getBeforeForUpdate(after, before, listResult));
        }
    }

    public void justInsertUpdate(Map<String, Object> after,Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        //after is empty will be skipped
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        //insert into all columns, make preparedStatement
        if (EmptyKit.isNull(preparedStatement)) {
            String allColumnString = allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(","));
            String insertHead = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" (" + allColumnString + ") ";
            String insertValue = "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ";
            String insertSql = insertHead + insertValue;
            if (EmptyKit.isNotEmpty(uniqueCondition)) {
                if (hasPk) {
                    insertSql = "MERGE INTO \"" + schema + "\".\"" + tapTable.getId() + "\" USING dual ON ("
                            + uniqueCondition.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND "))
                            + ")" + (allColumn.size() == uniqueCondition.size() ? "" : (" WHEN MATCHED THEN UPDATE SET " + allColumn.stream().filter(col -> !uniqueCondition.contains(col))
                            .map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")))) + " WHEN NOT MATCHED THEN INSERT(" + allColumnString + ") " + insertValue;
                } else {
                    insertSql = "MERGE INTO \"" + schema + "\".\"" + tapTable.getId() + "\" USING dual ON ("
                            + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ? IS NULL))").collect(Collectors.joining(" AND "))
                            + ")" + (allColumn.size() == uniqueCondition.size() ? "" : (" WHEN MATCHED THEN UPDATE SET " + allColumn.stream().filter(col -> !uniqueCondition.contains(col))
                            .map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")))) + " WHEN NOT MATCHED THEN INSERT(" + allColumnString + ") " + insertValue;
                }
            }
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        //make params
        int pos = 1;
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            if (hasPk) {
                for (String key : uniqueCondition) {
                    preparedStatement.setObject(pos++, after.get(key));
                }
            } else {
                for (String key : uniqueCondition) {
                    preparedStatement.setObject(pos++, after.get(key));
                    preparedStatement.setObject(pos++, after.get(key));
                }
            }
            for (String key : allColumn.stream().filter(col -> !uniqueCondition.contains(col)).collect(Collectors.toList())) {
                preparedStatement.setObject(pos++, after.get(key));
            }
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        preparedStatement.addBatch();
        executeBatch(listResult);
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
                        before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ? IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : after.keySet()) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        dealNullBefore(before, pos);
        preparedStatement.addBatch();
    }


}

package io.tapdata.connector.dws;

import io.tapdata.common.WriteRecorder;
import io.tapdata.connector.dws.bean.DwsTapTable;
import io.tapdata.connector.dws.config.DwsConfig;
import io.tapdata.connector.postgres.dml.PostgresWriteRecorder;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class DwsWriteRecorder extends PostgresWriteRecorder {
    private DwsTapTable dwsTapTable;

    public DwsWriteRecorder(Connection connection, DwsTapTable dwsTapTable, String schema) {
        super(connection, dwsTapTable.getTapTable(), schema);
        this.dwsTapTable = dwsTapTable;
    }

    public DwsWriteRecorder(Connection connection, DwsTapTable dwsTapTable, String schema, boolean hasUnique) {
        super(connection, dwsTapTable.getTapTable(), schema);
//        uniqueConditionIsIndex = uniqueConditionIsIndex && hasUnique;
        this.dwsTapTable = dwsTapTable;
    }

    @Override
    public void addInsertBatch(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            if (dwsTapTable.isPartition()){
                if (insertPolicy.equals("ignore_on_exists")) {
                    conflictIgnoreInsert(after);
                } else {
                    conflictUpdateInsert(after);
                }
            }else {
                if (insertPolicy.equals("ignore_on_exists")) {
                    notExistsInsert(after);
                } else {
                    withUpdateInsert(after);
                }
            }
        }else {
            justInsert(after);
        }
        preparedStatement.addBatch();
    }


    private Collection<String> buildConflictKeys() {
        Collection<String> conflictKeys = tapTable.primaryKeys(false);
        if (null == conflictKeys || conflictKeys.isEmpty()) {
            if (null!=tapTable.getIndexList()){
                TapIndex firstUniqueIndex = tapTable.getIndexList()
                        .stream()
                        .filter(index -> index.isUnique())
                        .findFirst().orElse(null);
                if (null != firstUniqueIndex) {
                    conflictKeys = firstUniqueIndex.getIndexFields().stream().map(f -> f.getName()).collect(Collectors.toList());
                }
            }
        }
        if (null == conflictKeys || conflictKeys.isEmpty()) {
            // todo 报错, 分区表"%s"没有主键或唯一索引，不支持冲突更新操作，请改用追加模式
            throw new RuntimeException(String.format("The partitioned table \"%s\" lacks a primary key or unique index, ", tapTable.getId()) +
                    "and does not support conflict update operations. Please switch to the append mode.");
        }
        return conflictKeys;
    }

    //on conflict
    private void conflictUpdateInsert(Map<String, Object> after) throws SQLException {
        //INSERT INTO "web_returns_p6" VALUES(20230201,7,7,7) ON CONFLICT("WR_RETURNED_DATE_SK","WR_ITEM_SK") DO UPDATE SET "WR_RETURNED_TIME_SK" = 7, "WR_REFUNDED_CUSTOMER_SK" = 8;
        Collection<String> conflictKeys = buildConflictKeys();
        Set<String> setKeys = buildSetKeys();
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") " +
                    "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON CONFLICT("
                    + conflictKeys.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", "))
                    + ") DO UPDATE SET " + setKeys.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", "));
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
        for (String key : setKeys) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
    }

    private Set<String> buildSetKeys() {
        return allColumn.stream().filter(k -> !dwsTapTable.getDistributedKeys().contains(k)).collect(Collectors.toSet());
    }


    private void conflictIgnoreInsert(Map<String, Object> after) throws SQLException {
        Collection<String> conflictKeys = buildConflictKeys();
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") " +
                    "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON CONFLICT("
                    + conflictKeys.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", "))
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
    private void withUpdateInsert(Map<String, Object> after) throws SQLException {
        Set<String> setKeys = buildSetKeys();
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql;
            if (hasPk) {
                insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + setKeys.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            } else {
                insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + setKeys.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                        .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            }
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        if (hasPk) {
            for (String key : setKeys) {
                preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
            }
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
            }
        } else {
            for (String key : setKeys) {
                preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
            }
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
                preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
            }
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
    protected void justInsert(Map<String, Object> after) throws SQLException {
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
        checkDistributeValue(after, before);
        if (updatePolicy.equals(ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS)) {
            if (dwsTapTable.isPartition()){
                conflictUpdateInsert(after);
            }else {
                insertUpdate(after, getBeforeForUpdate(after, before));
            }
        } else {
            justUpdate(after, getBeforeForUpdate(after, before));
        }
        preparedStatement.addBatch();
    }

    private void checkDistributeValue(Map<String, Object> after, Map<String, Object> before) {
        //判断有没有before，没有抛异常
        if (null == before) {
            throw new RuntimeException("Current Database cannot support update operation");
        }
        List<String> distributedKeys = dwsTapTable.getDistributedKeys();
        for (String distributedKey : distributedKeys) {
            Object afterData = after.get(distributedKey);
            Object beforeData = before.get(distributedKey);
            boolean distributeValueChange = false;
            if (null == afterData && null == beforeData) {
                // equals
            } else if (null != afterData && null == beforeData || null == afterData && null != beforeData) {
                distributeValueChange = true;
            } else if (!afterData.equals(beforeData)) {
                distributeValueChange = true;
            }
            if (distributeValueChange) {
                //Todo 改主键抛异常，优化delete+insert
                throw new RuntimeException("Distributed key column '" + distributedKey + "' can't be updated in table'" + tapTable.getName() + "'.Value: " + beforeData + " => " + afterData);
            }
        }
    }

    protected void justUpdate(Map<String, Object> after, Map<String, Object> before) throws SQLException {
        Set<String> setKeys = buildSetKeys();
        if (EmptyKit.isNull(preparedStatement)) {
            String sql = "UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " +
                    setKeys.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", "));
            if (hasPk) {
                sql += " WHERE " +
                        before.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND "));
            } else {
                sql += " WHERE " +
                        before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                                .collect(Collectors.joining(" AND "));
            }
            preparedStatement = connection.prepareStatement(sql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : setKeys) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }

        dealNullBefore(before, pos);
    }

    private void insertUpdate(Map<String, Object> after, Map<String, Object> before) throws SQLException {
        Set<String> setKeys = buildSetKeys();
        if (EmptyKit.isNull(preparedStatement)) {
            String updateSql;
            if (hasPk) {
                updateSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + updatedColumn.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(", ")) + " WHERE " + before.keySet().stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            } else {
                updateSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + setKeys.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(", ")) + " WHERE " + before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                        .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            }
            preparedStatement = connection.prepareStatement(updateSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        List<String> afterFilter = hasPk ? updatedColumn.stream().collect(Collectors.toList()) : setKeys.stream().collect(Collectors.toList());
        for (String key : afterFilter) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
        pos = dealNullBeforeWithReturn(before, pos);
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterInvalid(after.get(key)));
        }
    }

    @Override
    public void addDeleteBatch(Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
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
    protected void dealNullBefore(Map<String, Object> before, int pos) throws SQLException {
        if (hasPk) {
            for (String key : before.keySet()) {
                preparedStatement.setObject(pos++, before.get(key));
            }
        } else {
            for (String key : before.keySet()) {
                preparedStatement.setObject(pos++, before.get(key));
                preparedStatement.setObject(pos++, before.get(key));
            }
        }
    }

    protected int dealNullBeforeWithReturn(Map<String, Object> before, int pos) throws SQLException {
        if (hasPk) {
            for (String key : before.keySet()) {
                preparedStatement.setObject(pos++, before.get(key));
            }
        } else {
            for (String key : before.keySet()) {
                preparedStatement.setObject(pos++, before.get(key));
                preparedStatement.setObject(pos++, before.get(key));
            }
        }
        return pos;
    }
}

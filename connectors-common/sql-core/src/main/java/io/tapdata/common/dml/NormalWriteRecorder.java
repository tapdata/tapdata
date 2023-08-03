package io.tapdata.common.dml;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class NormalWriteRecorder {

    protected final Connection connection;
    protected final TapTable tapTable;
    protected final String schema;

    protected final List<String> allColumn;
    protected final List<String> updatedColumn;
    protected final List<String> uniqueCondition;
    protected boolean hasPk = false;

    protected String version;
    protected String insertPolicy;
    protected String updatePolicy;
    private char escapeChar = '"';

    protected String preparedStatementKey;
    protected Map<String, PreparedStatement> preparedStatementMap = new HashMap<>();
    protected PreparedStatement preparedStatement = null;

    protected final AtomicLong atomicLong = new AtomicLong(0); //record counter
    protected final List<TapRecordEvent> batchCache = TapSimplify.list(); //event cache

    public NormalWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        this.connection = connection;
        this.tapTable = tapTable;
        this.schema = schema;
        this.allColumn = tapTable.getNameFieldMap().entrySet().stream().sorted(Comparator.comparing(v ->
                EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(Map.Entry::getKey).collect(Collectors.toList());
        if (EmptyKit.isNotEmpty(tapTable.primaryKeys(false))) {
            hasPk = true;
            uniqueCondition = new ArrayList<>(tapTable.primaryKeys(false));
        } else {
            uniqueCondition = new ArrayList<>(tapTable.primaryKeys(true));
        }
        updatedColumn = allColumn.stream().filter(v -> !uniqueCondition.contains(v)).collect(Collectors.toList());
        if (EmptyKit.isEmpty(updatedColumn)) {
            updatedColumn.addAll(allColumn);
        }
    }

    /**
     * batch write events
     *
     * @param listResult results of WriteRecord
     */
    public void executeBatch(WriteListResult<TapRecordEvent> listResult) throws SQLException {
        long succeed = batchCache.size();
        if (succeed <= 0) {
            return;
        }
        try {
            if (preparedStatement != null) {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
                batchCache.clear();
            }
        } catch (SQLException e) {
            Map<TapRecordEvent, Throwable> map = batchCache.stream().collect(Collectors.toMap(Function.identity(), (v) -> e));
            listResult.addErrors(map);
            throw e;
        }
        atomicLong.addAndGet(succeed);
    }

    //commit when cacheSize >= 1000
    public void addAndCheckCommit(TapRecordEvent recordEvent, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        batchCache.add(recordEvent);
        if (batchCache.size() >= 1000) {
            executeBatch(listResult);
        }
    }

    public void releaseResource() {
        preparedStatementMap.forEach((key, value) -> EmptyKit.closeQuietly(value));
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setInsertPolicy(String insertPolicy) {
        this.insertPolicy = insertPolicy;
    }

    public void setUpdatePolicy(String updatePolicy) {
        this.updatePolicy = updatePolicy;
    }

    public void setEscapeChar(char escapeChar) {
        this.escapeChar = escapeChar;
    }

    public AtomicLong getAtomicLong() {
        return atomicLong;
    }

    //many types of insert data
    public void addInsertBatch(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        switch (insertPolicy) {
            case "update_on_exists":
                upsert(after, listResult);
                break;
            case "ignore_on_exists":
                insertIgnore(after, listResult);
                break;
            default:
                justInsert(after);
                break;
        }
        preparedStatement.addBatch();
    }

    protected void upsert(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        throw new UnsupportedOperationException("upsert is not supported");
    }

    protected void insertIgnore(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        throw new UnsupportedOperationException("insertIgnore is not supported");
    }

    protected void justInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "INSERT INTO " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " ("
                    + allColumn.stream().map(k -> escapeChar + k + escapeChar).collect(Collectors.joining(", ")) + ") " +
                    "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ";
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    public void addUpdateBatch(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        Map<String, Object> lastBefore = getBeforeForUpdate(after, before);
        switch (updatePolicy) {
            case "insert_on_nonexists":
                insertUpdate(after, lastBefore, listResult);
                break;
            case "log_on_nonexists":
                updateIgnoreWithLog(after, lastBefore, listResult);
                break;
            default:
                justUpdate(after, lastBefore, listResult);
                break;
        }
        preparedStatement.addBatch();
    }

    protected void insertUpdate(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        throw new UnsupportedOperationException("upsert is not supported");
    }

    protected void updateIgnoreWithLog(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        throw new UnsupportedOperationException("insertIgnore is not supported");
    }

    protected void justUpdate(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        boolean containsNull = !hasPk && before.containsValue(null);
        String preparedStatementKey = String.join(",", after.keySet()) + "|" + containsNull;
        if (preparedStatementKey.equals(this.preparedStatementKey)) {
            preparedStatement = preparedStatementMap.get(preparedStatementKey);
        } else {
            if (EmptyKit.isNull(this.preparedStatementKey)) {
                preparedStatement = connection.prepareStatement(getUpdateSql(before, containsNull));
                preparedStatementMap.put(preparedStatementKey, preparedStatement);
            } else {
                executeBatch(listResult);
                this.preparedStatementKey = preparedStatementKey;
                preparedStatement = preparedStatementMap.get(preparedStatementKey);
                if (EmptyKit.isNull(preparedStatement)) {
                    preparedStatement = connection.prepareStatement(getUpdateSql(before, containsNull));
                    preparedStatementMap.put(preparedStatementKey, preparedStatement);
                }
            }
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : updatedColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        if (!containsNull) {
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

    private Map<String, Object> getBeforeForUpdate(Map<String, Object> after, Map<String, Object> before) {
        //in some datasource, before of events is always empty, so before is unreliable
        Map<String, Object> lastBefore = new HashMap<>();
        if (EmptyKit.isEmpty(uniqueCondition)) {
            allColumn.forEach(v -> lastBefore.put(v, (EmptyKit.isNotEmpty(before) && before.containsKey(v)) ? before.get(v) : after.get(v)));
        } else {
            uniqueCondition.forEach(v -> lastBefore.put(v, (EmptyKit.isNotEmpty(before) && before.containsKey(v)) ? before.get(v) : after.get(v)));
        }
        return lastBefore;
    }

    protected String getUpdateSql(Map<String, Object> before, boolean containsNull) {
        if (!containsNull) {
            return "UPDATE " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " SET " +
                    updatedColumn.stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(", ")) + " WHERE " +
                    before.keySet().stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(" AND "));
        } else {
            return "UPDATE " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " SET " +
                    updatedColumn.stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(", ")) + " WHERE " +
                    before.keySet().stream().map(k -> "(" + escapeChar + k + escapeChar + "=? OR (" + escapeChar + k + escapeChar + " IS NULL AND ? IS NULL))")
                            .collect(Collectors.joining(" AND "));
        }
    }

    //(most often) delete data
    public void addDeleteBatch(Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(before)) {
            return;
        }
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            before.keySet().removeIf(k -> !uniqueCondition.contains(k));
        }
        justDelete(before, listResult);
        preparedStatement.addBatch();
    }

    protected void justDelete(Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        boolean containsNull = !hasPk && before.containsValue(null);
        String preparedStatementKey = "|" + containsNull;
        if (preparedStatementKey.equals(this.preparedStatementKey)) {
            preparedStatement = preparedStatementMap.get(preparedStatementKey);
        } else {
            if (EmptyKit.isNull(this.preparedStatementKey)) {
                preparedStatement = connection.prepareStatement(getDeleteSql(before, containsNull));
                preparedStatementMap.put(preparedStatementKey, preparedStatement);
            } else {
                executeBatch(listResult);
                this.preparedStatementKey = preparedStatementKey;
                preparedStatement = preparedStatementMap.get(preparedStatementKey);
                if (EmptyKit.isNull(preparedStatement)) {
                    preparedStatement = connection.prepareStatement(getDeleteSql(before, containsNull));
                    preparedStatementMap.put(preparedStatementKey, preparedStatement);
                }
            }
        }
        preparedStatement.clearParameters();
        int pos = 1;
        if (!containsNull) {
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

    protected String getDeleteSql(Map<String, Object> before, boolean containsNull) {
        if (!containsNull) {
            return "DELETE FROM " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " WHERE " +
                    before.keySet().stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(" AND "));
        } else {
            return "DELETE FROM " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " WHERE " +
                    before.keySet().stream().map(k -> "(" + escapeChar + k + escapeChar + "=? OR (" + escapeChar + k + escapeChar + " IS NULL AND ? IS NULL))")
                            .collect(Collectors.joining(" AND "));
        }
    }
}

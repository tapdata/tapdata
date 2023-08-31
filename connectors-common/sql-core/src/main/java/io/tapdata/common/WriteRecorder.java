package io.tapdata.common;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class WriteRecorder {

    private static final String TAG = WriteRecorder.class.getSimpleName();

    protected final Connection connection;
    protected final TapTable tapTable;
    protected final List<String> allColumn;
    protected List<String> updatedColumn;
    protected final String schema;
    protected List<String> uniqueCondition;
    protected boolean hasPk = false;
    protected boolean uniqueConditionIsIndex = true; //Target table may not have a unique index, used in Postgres
    protected String version;
    protected String insertPolicy;
    protected String updatePolicy;
    protected List<String> afterKeys;

    protected PreparedStatement preparedStatement = null;
    protected final AtomicLong atomicLong = new AtomicLong(0); //record counter
    protected final List<TapRecordEvent> batchCache = TapSimplify.list(); //event cache

    public WriteRecorder(Connection connection, TapTable tapTable, String schema) {
        this.connection = connection;
        this.tapTable = tapTable;
        this.schema = schema;
        //TM's pos may be null
        this.allColumn = tapTable.getNameFieldMap().entrySet().stream().sorted(Comparator.comparing(v ->
                EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(Map.Entry::getKey).collect(Collectors.toList());
        analyzeTable();
    }

    private void analyzeTable() {
        //1、primaryKeys has first priority
        if (EmptyKit.isNotEmpty(tapTable.primaryKeys(false))) {
            hasPk = true;
            uniqueCondition = new ArrayList<>(tapTable.primaryKeys(false));
        }
        //2、second priority: analyze table with its indexes
        else {
            uniqueCondition = new ArrayList<>(tapTable.primaryKeys(true));
            uniqueConditionIsIndex = EmptyKit.isNotEmpty(tapTable.getIndexList()) && tapTable.getIndexList().stream().filter(TapIndex::isUnique).anyMatch(in ->
                    (in.getIndexFields().size() == uniqueCondition.size()) && new HashSet<>(uniqueCondition)
                            .containsAll(in.getIndexFields().stream().map(TapIndexField::getName).collect(Collectors.toList())));
        }
        updatedColumn = allColumn.stream().filter(v -> !uniqueCondition.contains(v)).collect(Collectors.toList());
        if (EmptyKit.isEmpty(updatedColumn)) {
            updatedColumn = allColumn;
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
        try {
            if (EmptyKit.isNotNull(preparedStatement)) {
                preparedStatement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
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

    public AtomicLong getAtomicLong() {
        return atomicLong;
    }

    //many types of insert data
    public abstract void addInsertBatch(Map<String, Object> after) throws SQLException;

    //(most often) update data
    public void addUpdateBatch(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        justUpdate(after, getBeforeForUpdate(after, before, listResult));
        preparedStatement.addBatch();
    }

    protected Map<String, Object> getBeforeForUpdate(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(afterKeys)) {
            afterKeys = new ArrayList<>(after.keySet());
        }
        if (!afterKeys.equals(new ArrayList<>(after.keySet()))) {
            executeBatch(listResult);
            preparedStatement = null;
            afterKeys = new ArrayList<>(after.keySet());
        }
        //in some datasource, before of events is always empty, so before is unreliable
        Map<String, Object> lastBefore = new HashMap<>();
        if (EmptyKit.isEmpty(uniqueCondition)) {
            allColumn.forEach(v -> lastBefore.put(v, (EmptyKit.isNotEmpty(before) && before.containsKey(v)) ? before.get(v) : after.get(v)));
        } else {
            uniqueCondition.forEach(v -> lastBefore.put(v, (EmptyKit.isNotEmpty(before) && before.containsKey(v)) ? before.get(v) : after.get(v)));
        }
        return lastBefore;
    }

    protected void justUpdate(Map<String, Object> after, Map<String, Object> before) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            if (hasPk) {
                preparedStatement = connection.prepareStatement("UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " +
                        updatedColumn.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " +
                        updatedColumn.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ? IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : updatedColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        dealNullBefore(before, pos);
    }

    //(most often) delete data
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
                        before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ? IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        dealNullBefore(before, 1);
        preparedStatement.addBatch();
    }

    //Once a field can be null, there can be two placeholders in where clause, but the performance will decrease
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

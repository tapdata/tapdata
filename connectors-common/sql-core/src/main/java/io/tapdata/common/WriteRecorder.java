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

    protected final Connection connection;
    protected final TapTable tapTable;
    protected final List<String> allColumn;
    protected final String schema;
    protected List<String> uniqueCondition;
    protected boolean hasPk = false;
    protected boolean uniqueConditionIsIndex = false;
    protected String version;

    protected PreparedStatement preparedStatement = null;
    protected final AtomicLong atomicLong = new AtomicLong(0);
    protected final List<TapRecordEvent> batchCache = TapSimplify.list();

    public WriteRecorder(Connection connection, TapTable tapTable, String schema) {
        this.connection = connection;
        this.tapTable = tapTable;
        this.schema = schema;
        this.allColumn = new ArrayList<>(tapTable.getNameFieldMap().keySet());
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
    }

    public void executeBatch(WriteListResult<TapRecordEvent> listResult) {
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
            succeed = 0;
            e.printStackTrace();
        }
        atomicLong.addAndGet(succeed);
    }

    public void addAndCheckCommit(TapRecordEvent recordEvent, WriteListResult<TapRecordEvent> listResult) {
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

    public AtomicLong getAtomicLong() {
        return atomicLong;
    }

    public abstract void addInsertBatch(Map<String, Object> after) throws SQLException;

    //before is always empty
    public void addUpdateBatch(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isEmpty(after) || EmptyKit.isEmpty(uniqueCondition)) {
            return;
        }
        Map<String, Object> before = new HashMap<>();
        uniqueCondition.forEach(k -> before.put(k, after.get(k)));
        if (EmptyKit.isNull(preparedStatement)) {
            if (hasPk) {
                preparedStatement = connection.prepareStatement("UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " +
                        allColumn.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " +
                        allColumn.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        dealNullBefore(before, pos);
        preparedStatement.addBatch();
    }

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

    private void dealNullBefore(Map<String, Object> before, int pos) throws SQLException {
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
}

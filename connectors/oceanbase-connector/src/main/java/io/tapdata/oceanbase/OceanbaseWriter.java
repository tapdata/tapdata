package io.tapdata.oceanbase;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.oceanbase.connector.OceanbaseJdbcContext;
import io.tapdata.oceanbase.util.JdbcUtil;
import io.tapdata.oceanbase.util.LRUOnRemoveMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author dayun
 * @date 2022/6/23 16:39
 */
public class OceanbaseWriter {
    private static final String TAG = OceanbaseWriter.class.getSimpleName();
    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values(%s)";
    private static final String UPDATE_SQL_TEMPLATE = "UPDATE `%s`.`%s` SET %s WHERE %s";
    private static final String DELETE_SQL_TEMPLATE = "DELETE FROM `%s`.`%s` WHERE %s";
    private static final String CHECK_ROW_EXISTS_TEMPLATE = "SELECT COUNT(1) as count FROM `%s`.`%s` WHERE %s";

    private final Map<String, PreparedStatement> insertMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
    private final Map<String, PreparedStatement> updateMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
    private final Map<String, PreparedStatement> deleteMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
    private final Map<String, PreparedStatement> checkExistsMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));

    private AtomicBoolean running = new AtomicBoolean(true);
    private Connection connection;
    private OceanbaseJdbcContext oceanbaseJdbcContext;
    
    public OceanbaseWriter(final OceanbaseJdbcContext oceanbaseJdbcContext) throws Throwable {
        this.oceanbaseJdbcContext = oceanbaseJdbcContext;
        this.connection = oceanbaseJdbcContext.getConnection();
    }

    public WriteListResult<TapRecordEvent> write(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        TapRecordEvent errorRecord = null;
        try {
            for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
                if (!running.get()) {
                    break;
                }
                try {
                    if (tapRecordEvent instanceof TapInsertRecordEvent) {
                        int insertRow = doInsertOne(tapConnectorContext, tapTable, tapRecordEvent, writeListResult);
                        writeListResult.incrementInserted(insertRow);
                    } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
                        int updateRow = doUpdateOne(tapConnectorContext, tapTable, tapRecordEvent, writeListResult);
                        writeListResult.incrementModified(updateRow);
                    } else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                        int deleteRow = doDeleteOne(tapConnectorContext, tapTable, tapRecordEvent, writeListResult);
                        writeListResult.incrementRemove(deleteRow);
                    } else {
                        writeListResult.addError(tapRecordEvent, new Exception("Event type \"" + tapRecordEvent.getClass().getSimpleName() + "\" not support: " + tapRecordEvent));
                    }
                } catch (Throwable e) {
                    errorRecord = tapRecordEvent;
                    throw e;
                }
            }
            OceanbaseJdbcContext.tryCommit(connection);
        } catch (Throwable e) {
            writeListResult.setInsertedCount(0);
            writeListResult.setModifiedCount(0);
            writeListResult.setRemovedCount(0);
            if (null != errorRecord) writeListResult.addError(errorRecord, e);
            OceanbaseJdbcContext.tryRollBack(connection);
            throw e;
        }
        return writeListResult;
    }

    private int doInsertOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, WriteListResult<TapRecordEvent> writeListResult) throws Throwable {
        PreparedStatement insertPreparedStatement = getInsertPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, insertMap);
        setPreparedStatementValues(tapTable, tapRecordEvent, insertPreparedStatement);
        int row;
        try {
            row = insertPreparedStatement.executeUpdate();
        } catch (Exception e) {
            if (e instanceof SQLIntegrityConstraintViolationException
                    && CollectionUtils.isNotEmpty(tapTable.primaryKeys(true))) {
                TapLogger.warn(TAG, "Execute insert failed, will retry update or insert after check record exists");
                if (rowExists(tapConnectorContext, tapTable, tapRecordEvent)) {
                    row = doUpdateOne(tapConnectorContext, tapTable, tapRecordEvent, writeListResult);
                } else {
                    throw new Exception("Insert data failed, sql: " + insertPreparedStatement + ", message: " + e.getMessage(), e);
                }
            } else {
                throw new Exception("Insert data failed, sql: " + insertPreparedStatement + ", message: " + e.getMessage(), e);
            }
        }
        return row;
    }

    private int doUpdateOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, WriteListResult<TapRecordEvent> writeListResult) throws Throwable {
        PreparedStatement updatePreparedStatement = getUpdatePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, updateMap);
        int parameterIndex = setPreparedStatementValues(tapTable, tapRecordEvent, updatePreparedStatement);
        setPreparedStatementWhere(tapTable, tapRecordEvent, updatePreparedStatement, parameterIndex);
        int row;
        try {
            row = updatePreparedStatement.executeUpdate();
        } catch (Exception e) {
            throw new Exception("Update data failed, sql: " + updatePreparedStatement + ", message: " + e.getMessage(), e);
        }
        return row;
    }

    private int doDeleteOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, WriteListResult<TapRecordEvent> writeListResult) throws Throwable {
        PreparedStatement deletePreparedStatement = getDeletePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, deleteMap);
        setPreparedStatementWhere(tapTable, tapRecordEvent, deletePreparedStatement, 1);
        int row;
        try {
            row = deletePreparedStatement.executeUpdate();
        } catch (Throwable e) {
            throw new Exception("Delete data failed, sql: " + deletePreparedStatement + ", message: " + e.getMessage(), e);
        }
        return row;
    }

    private boolean rowExists(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        PreparedStatement checkRowExistsPreparedStatement = getCheckRowExistsPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, checkExistsMap);
        setPreparedStatementWhere(tapTable, tapRecordEvent, checkRowExistsPreparedStatement, 1);
        AtomicBoolean result = new AtomicBoolean(false);
        try {
            this.oceanbaseJdbcContext.query(checkRowExistsPreparedStatement, rs -> {
                if (rs.next()) {
                    int count = rs.getInt("count");
                    result.set(count > 0);
                }
            });
        } catch (Throwable e) {
            throw new Exception("Check row exists failed, sql: " + checkRowExistsPreparedStatement + ", message: " + e.getMessage(), e);
        }
        return result.get();
    }

    private PreparedStatement getInsertPreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, Map<String, PreparedStatement> insertMap) throws Throwable {
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = insertMap.get(key);
        if (null == preparedStatement) {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            String name = connectionConfig.getString("name");
            String tableId = tapTable.getId();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("Create insert prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
            }
            List<String> fields = new ArrayList<>();
            nameFieldMap.forEach((fieldName, field) -> {
                if (!needAddIntoPreparedStatementValues(field, tapRecordEvent)) {
                    return;
                }
                fields.add("`" + fieldName + "`");
            });
            List<String> questionMarks = fields.stream().map(f -> "?").collect(Collectors.toList());
            String sql = String.format(INSERT_SQL_TEMPLATE, database, tableId, String.join(",", fields), String.join(",", questionMarks));
            try {
                preparedStatement = this.connection.prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create insert prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create insert prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            insertMap.put(key, preparedStatement);
        }
        return preparedStatement;
    }

    private PreparedStatement getUpdatePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, Map<String, PreparedStatement> updateMap) throws Throwable {
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = updateMap.get(key);
        if (null == preparedStatement) {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            String name = connectionConfig.getString("name");
            String tableId = tapTable.getId();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("Create update prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
            }
            List<String> setList = new ArrayList<>();
            nameFieldMap.forEach((fieldName, field) -> {
                if (!needAddIntoPreparedStatementValues(field, tapRecordEvent)) {
                    return;
                }
                setList.add("`" + fieldName + "`=?");
            });
            List<String> whereList = new ArrayList<>();
            Collection<String> uniqueKeys = getUniqueKeys(tapTable);
            for (String uniqueKey : uniqueKeys) {
                whereList.add("`" + uniqueKey + "`<=>?");
            }
            String sql = String.format(UPDATE_SQL_TEMPLATE, database, tableId, String.join(",", setList), String.join(" AND ", whereList));
            try {
                preparedStatement = this.connection.prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create update prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create update prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            updateMap.put(key, preparedStatement);
        }
        return preparedStatement;
    }

    private PreparedStatement getDeletePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, Map<String, PreparedStatement> deleteMap) throws Throwable {
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = deleteMap.get(key);
        if (null == preparedStatement) {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            String name = connectionConfig.getString("name");
            String tableId = tapTable.getId();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("Create delete prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
            }
            List<String> whereList = new ArrayList<>();
            Collection<String> uniqueKeys = getUniqueKeys(tapTable);
            for (String uniqueKey : uniqueKeys) {
                whereList.add("`" + uniqueKey + "`<=>?");
            }
            String sql = String.format(DELETE_SQL_TEMPLATE, database, tableId, String.join(" AND ", whereList));
            try {
                preparedStatement = this.connection.prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create delete prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create delete prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            deleteMap.put(key, preparedStatement);
        }
        return preparedStatement;
    }

    protected PreparedStatement getCheckRowExistsPreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, Map<String, PreparedStatement> checkExistsMap) throws Throwable {
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = checkExistsMap.get(key);
        if (null == preparedStatement) {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            String name = connectionConfig.getString("name");
            String tableId = tapTable.getId();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("Create check row exists prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
            }
            List<String> whereList = new ArrayList<>();
            Collection<String> uniqueKeys = getUniqueKeys(tapTable);
            for (String uniqueKey : uniqueKeys) {
                whereList.add("`" + uniqueKey + "`<=>?");
            }
            String sql = String.format(CHECK_ROW_EXISTS_TEMPLATE, database, tableId, String.join(" AND ", whereList));
            try {
                preparedStatement = this.connection.prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create check row exists prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create check row exists prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            checkExistsMap.put(key, preparedStatement);
        }
        return preparedStatement;
    }

    private int setPreparedStatementValues(TapTable tapTable, TapRecordEvent tapRecordEvent, PreparedStatement preparedStatement) throws Throwable {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        int parameterIndex = 1;
        Map<String, Object> after = getAfter(tapRecordEvent);
        if (MapUtils.isEmpty(after)) {
            throw new Exception("Set prepared statement values failed, after is empty: " + tapRecordEvent);
        }
        List<String> afterKeys = new ArrayList<>(after.keySet());
        for (String fieldName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(fieldName);
            if (!needAddIntoPreparedStatementValues(tapField, tapRecordEvent)) {
                continue;
            }
            Object o = after.get(fieldName);
            preparedStatement.setObject(parameterIndex++, o);
            afterKeys.remove(fieldName);
        }
        if (CollectionUtils.isNotEmpty(afterKeys)) {
            Map<String, Object> missingAfter = new HashMap<>();
            afterKeys.forEach(k -> missingAfter.put(k, after.get(k)));
            TapLogger.warn(TAG, "Found fields in after data not exists in schema fields, will skip it: " + missingAfter);
        }
        return parameterIndex;
    }

    private void setPreparedStatementWhere(TapTable tapTable, TapRecordEvent tapRecordEvent, PreparedStatement preparedStatement, int parameterIndex) throws Throwable {
        if (parameterIndex <= 1) {
            parameterIndex = 1;
        }
        Map<String, Object> before = getBefore(tapRecordEvent);
        Map<String, Object> after = getAfter(tapRecordEvent);
        if (MapUtils.isEmpty(before) && MapUtils.isEmpty(after)) {
            throw new Exception("Set prepared statement where clause failed, before and after both empty: " + tapRecordEvent);
        }
        Map<String, Object> data;
        if (MapUtils.isNotEmpty(before)) {
            data = before;
        } else {
            data = after;
        }
        Collection<String> uniqueKeys = getUniqueKeys(tapTable);
        for (String uniqueKey : uniqueKeys) {
            if (!data.containsKey(uniqueKey)) {
                throw new Exception("Set prepared statement where clause failed, unique key \"" + uniqueKey + "\" not exists in data: " + tapRecordEvent);
            }
            Object value = data.get(uniqueKey);
            preparedStatement.setObject(parameterIndex++, value);
        }
    }

    private boolean needAddIntoPreparedStatementValues(TapField field, TapRecordEvent tapRecordEvent) {
        Map<String, Object> after = getAfter(tapRecordEvent);
        if (null == after) {
            return false;
        }
        if (!after.containsKey(field.getName())) {
            TapLogger.warn(TAG, "Found schema field not exists in after data, will skip it: " + field.getName());
            return false;
        }
        return true;
    }

    protected String getKey(TapTable tapTable, TapRecordEvent tapRecordEvent) {
        Map<String, Object> after = getAfter(tapRecordEvent);
        Map<String, Object> before = getBefore(tapRecordEvent);
        Map<String, Object> data;
        if (MapUtils.isNotEmpty(after)) {
            data = after;
        } else {
            data = before;
        }
        Set<String> keys = data.keySet();
        String keyString = String.join("-", keys);
        return tapTable.getId() + "-" + keyString;
    }

    private Collection<String> getUniqueKeys(TapTable tapTable) {
        return tapTable.primaryKeys(true);
    }

    private Map<String, Object> getBefore(TapRecordEvent tapRecordEvent) {
        Map<String, Object> before = null;
        if (tapRecordEvent instanceof TapUpdateRecordEvent) {
            before = ((TapUpdateRecordEvent) tapRecordEvent).getBefore();
        } else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
            before = ((TapDeleteRecordEvent) tapRecordEvent).getBefore();
        }
        return before;
    }

    private Map<String, Object> getAfter(TapRecordEvent tapRecordEvent) {
        Map<String, Object> after = null;
        if (tapRecordEvent instanceof TapInsertRecordEvent) {
            after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
        } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
            after = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
        }
        return after;
    }

    public void onDestroy() {
        this.running.set(false);
        this.insertMap.clear();
        this.updateMap.clear();
        this.deleteMap.clear();
        this.checkExistsMap.clear();
    }
}

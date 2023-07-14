package io.tapdata.connector.hive1.dml.impl;

import io.tapdata.connector.hive1.Hive1JdbcContext;
import io.tapdata.connector.hive1.config.Hive1Config;
import io.tapdata.connector.hive1.ddl.impl.Hive1JDBCSqlMaker;
import io.tapdata.connector.hive1.dml.Hive1Writer;
import io.tapdata.connector.hive1.util.JdbcUtil;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Hive1WriterJDBC implements Hive1Writer {


    private static final String TAG = Hive1WriterJDBC.class.getSimpleName();
    private final Map<String, JdbcCache> jdbcCacheMap = new ConcurrentHashMap<>();

    private final Map<String, Connection> connectionCacheMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));

    private final Map<String, String> batchInsertColumnSql = new LRUMap<>(10);

    private final Map<String, String> batchInsertValueSql = new LRUMap<>(10);
    private AtomicBoolean running = new AtomicBoolean(true);

    protected Hive1JdbcContext hive1JdbcContext;

    private Hive1Config hive1Config;

    private static ReentrantLock lock = new ReentrantLock();
    protected static final String INSERT_SQL_TEMPLATE = "INSERT INTO `%s`.`%s` values(%s)";
    protected static final String UPDATE_SQL_TEMPLATE = "UPDATE `%s`.`%s` set %s WHERE %s";
    protected static final String DELETE_SQL_TEMPLATE = "DELETE FROM `%s`.`%s` WHERE %s";
    protected static final String CHECK_ROW_EXISTS_TEMPLATE = "SELECT COUNT(1) as count FROM `%s`.`%s` WHERE %s";
    //不支持带字段插入
    protected static final String BATCH_INSERT_SQL = "INSERT INTO `%s`.`%s` VALUES ";


    public Hive1WriterJDBC(Hive1JdbcContext hive1JdbcContext, Hive1Config hive1Config) throws Throwable {
        this.hive1JdbcContext = hive1JdbcContext;
        this.hive1Config = hive1Config;
    }

    public WriteListResult<TapRecordEvent> write(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        TapRecordEvent errorRecord = null;
        List<TapRecordEvent> tapRecordEventList = new ArrayList<>();
        try {
            int msgCnt = 0;
            for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
                if (!running.get()) break;
                try {
                    if (tapRecordEvent instanceof TapInsertRecordEvent) {
                        tapRecordEventList.add(tapRecordEvent);
                        msgCnt++;
                        if (msgCnt >= MAX_BATCH_SAVE_SIZE) {
                            WriteListResult<TapRecordEvent> result = batchInsert(tapConnectorContext, tapTable, tapRecordEventList);
                            tapRecordEventList.clear();
                            msgCnt = 0;
                            sumResult(writeListResult, result);
                        }
                    } else {
                        if (CollectionUtils.isNotEmpty(tapRecordEventList)) {
                            WriteListResult<TapRecordEvent> result = batchInsert(tapConnectorContext, tapTable, tapRecordEventList);
                            tapRecordEventList.clear();
                            msgCnt = 0;
                            sumResult(writeListResult, result);
                        }
                        WriteListResult<TapRecordEvent> result = writeOne(tapConnectorContext, tapTable, Arrays.asList(tapRecordEvent));
                        sumResult(writeListResult, result);
                    }
                } catch (Throwable e) {
                    TapLogger.error("target database process message failed", "table name:{},error msg:{}", tapTable.getId(), e.getMessage(), e);
                    errorRecord = tapRecordEvent;
                    throw e;
                }
            }
            if (CollectionUtils.isNotEmpty(tapRecordEventList)) {
                WriteListResult<TapRecordEvent> result = batchInsert(tapConnectorContext, tapTable, tapRecordEventList);
                tapRecordEventList.clear();
                msgCnt = 0;
                sumResult(writeListResult, result);
            }
        } catch (Throwable e) {
            if (null != errorRecord) writeListResult.addError(errorRecord, e);
            throw e;
        }
        return writeListResult;
    }

//    public class WriteCache {
//        private TapConnectorContext tapConnectorContext;
//
//        private Map<String, TapTable> tapTableMap = new ConcurrentHashMap<>();
//        private Map<String, List<TapRecordEvent>> tapRecordEventMap = new ConcurrentHashMap<>();
//
//        private ReentrantLock lock = new ReentrantLock();
//
//        public WriteCache(TapConnectorContext tapConnectorContext) {
//            this.tapConnectorContext = tapConnectorContext;
//        }
//        public void addEvent(TapTable tapTable,TapRecordEvent tapRecordEvent) {
//            String tabId = tapTable.getId();
//            tapTableMap.putIfAbsent(tabId, tapTable);
//
//            if (tapRecordEventMap.containsKey(tabId)) {
//                tapRecordEventMap.get(tabId).add(tapRecordEvent);
//            } else {
//                try {
//                    lock.lock();
//                    if (tapRecordEventMap.containsKey(tabId)) {
//                        tapRecordEventMap.get(tabId).add(tapRecordEvent);
//                    } else {
//                        List<TapRecordEvent> list = new CopyOnWriteArrayList<>();
//                        list.add(tapRecordEvent);
//                        tapRecordEventMap.put(tabId, list);
//                    }
//                } finally {
//                    lock.unlock();
//                }
//            }
//        }
//    }


    public WriteListResult<TapRecordEvent> batchInsert(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEventList) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        String insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        if (null != tapConnectorContext.getConnectorCapabilities()
                && null != tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY)) {
            insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        }
        if (
                CollectionUtils.isEmpty(tapTable.primaryKeys())
                        && (CollectionUtils.isEmpty(tapTable.getIndexList()) || null == tapTable.getIndexList().stream().filter(TapIndex::isUnique).findFirst().orElse(null))
                        && ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertDmlPolicy)
                        && rowExists(tapConnectorContext, tapTable, tapRecordEventList.get(0))
        ) {
            return writeListResult;
        }
        int row = 0;
        PreparedStatement pstmt = null;
        try {
            String cloneInsertColumnSql = getCloneInsertColumnSql(tapConnectorContext, tapTable);
            if (StringUtils.isBlank(cloneInsertColumnSql)) {
                throw new RuntimeException("Does not found table " + tapTable.getId() + " 's fields ");
            }
            StringBuilder insertColumnSql = new StringBuilder(cloneInsertColumnSql);
            for (TapRecordEvent tapRecordEvent : tapRecordEventList) {
                Map<String, Object> after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
                if (MapUtils.isNotEmpty(after)) {
                    insertColumnSql.append(getCloneInsertValueSql(tapTable)).append(",");
                    row++;
                }
            }
            writeListResult.incrementInserted(row);

            int parameterIndex = 1;
            pstmt = getConnection().prepareStatement(StringUtils.removeEnd(insertColumnSql.toString(), ","));
            for (TapRecordEvent tapRecordEvent : tapRecordEventList) {
                Map<String, Object> after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
                if (MapUtils.isNotEmpty(after)) {
                    for (String fieldName : tapTable.getNameFieldMap().keySet()) {
                        pstmt.setObject(parameterIndex++, after.get(fieldName));
                    }
                }
            }

            pstmt.execute();
//            Hive1JdbcContext.tryCommit(connection);
        } catch (SQLException e) {
            writeListResult = batchErrorHandle(tapConnectorContext, tapTable, tapRecordEventList, pstmt, e);
        }
        return writeListResult;
    }

    protected WriteListResult<TapRecordEvent> batchErrorHandle(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents, PreparedStatement preparedStatement, Exception e) throws Throwable {
        TapLogger.warn("Batch insert data failed,", "fail reason:{},will retry one by one insert,stacks:{}", e.getMessage(), e);
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
            WriteListResult<TapRecordEvent> result = writeOne(tapConnectorContext, tapTable, Arrays.asList(tapRecordEvent));
            sumResult(writeListResult, result);
        }
        return writeListResult;
    }


    private String getCloneInsertColumnSql(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Exception {
        String table = tapTable.getId();
        String sql = batchInsertColumnSql.get(table);
        if (StringUtils.isBlank(sql)) {
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("get insert column error, table \"" + tapTable.getId() + "\"'s fields is empty");
            }
            String database = tapConnectorContext.getConnectionConfig().getString("database");
            sql = String.format(BATCH_INSERT_SQL, database, table.toLowerCase());
            batchInsertColumnSql.put(table, sql);
        }
        return sql;
    }

    private String getCloneInsertValueSql(TapTable tapTable) {
        String table = tapTable.getId();
        String sql = batchInsertValueSql.get(table);
        if (StringUtils.isBlank(sql)) {
            StringBuilder insertValueSB = new StringBuilder("(");
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            nameFieldMap.keySet().forEach(k -> insertValueSB.append("?,"));
            sql = insertValueSB.toString();
            sql = StringUtils.removeEnd(sql, ",") + ")";
            batchInsertValueSql.put(table, sql);
        }
        return sql;
    }

    public WriteListResult<TapRecordEvent> writeOne(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        TapRecordEvent errorRecord = null;
        try {
            for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
                if (!running.get()) break;
                try {
                    if (tapRecordEvent instanceof TapInsertRecordEvent) {
                        int insertRow = doInsertOne(tapConnectorContext, tapTable, tapRecordEvent);
                        writeListResult.incrementInserted(insertRow);
                    } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
                        int updateRow = doUpdateOne(tapConnectorContext, tapTable, tapRecordEvent);
                        writeListResult.incrementModified(updateRow);
                    } else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                        int deleteRow = doDeleteOne(tapConnectorContext, tapTable, tapRecordEvent);
                        writeListResult.incrementRemove(deleteRow);
                    } else {
                        writeListResult.addError(tapRecordEvent, new Exception("Event type \"" + tapRecordEvent.getClass().getSimpleName() + "\" not support: " + tapRecordEvent));
                    }
                } catch (Throwable e) {
                    errorRecord = tapRecordEvent;
                    throw e;
                }
            }
//            Hive1JdbcContext.tryCommit(connection);
        } catch (Throwable e) {
            writeListResult.setInsertedCount(0);
            writeListResult.setModifiedCount(0);
            writeListResult.setRemovedCount(0);
            if (null != errorRecord) writeListResult.addError(errorRecord, e);
//            Hive1JdbcContext.tryRollBack(connection);
            Hive1JdbcContext.tryRollBack(getConnection());
            throw e;
        }
        return writeListResult;
    }

    public void onDestroy() {
        this.running.set(false);
        this.jdbcCacheMap.values().forEach(JdbcCache::clear);
        for (Connection connection : this.connectionCacheMap.values()) {
            try {
                connection.close();
            } catch (SQLException e) {
                TapLogger.error(TAG, "connection:{} close fail:{}", connection, e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    private int doInsertOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        String insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        if (null != tapConnectorContext.getConnectorCapabilities()
                && null != tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY)) {
            insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        }
        if (
                CollectionUtils.isEmpty(tapTable.primaryKeys())
                        && (CollectionUtils.isEmpty(tapTable.getIndexList()) || null == tapTable.getIndexList().stream().filter(TapIndex::isUnique).findFirst().orElse(null))
                        && ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertDmlPolicy)
                        && rowExists(tapConnectorContext, tapTable, tapRecordEvent)
        ) {
            return 0;
        }
        int row;
        try {
            row = doInsert(tapConnectorContext, tapTable, tapRecordEvent);
        } catch (Throwable e) {
            if (null != e.getCause() && e.getCause() instanceof SQLIntegrityConstraintViolationException) {
                if (rowExists(tapConnectorContext, tapTable, tapRecordEvent)) {
                    if (ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertDmlPolicy)) {
                        return 0;
                    } else {
                        TapLogger.warn(TAG, "Execute insert failed, will retry update after check record exists");
                        row = doUpdate(tapConnectorContext, tapTable, tapRecordEvent);
                    }
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        }
        return row;
    }

    private int doInsert(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        final Map<String, PreparedStatement> insertMap = getJdbcCache().getInsertMap();
        PreparedStatement insertPreparedStatement = getInsertPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, insertMap);
//        insertPreparedStatement.setQueryTimeout(60);
        setPreparedStatementValues(tapTable, tapRecordEvent, insertPreparedStatement);
        try {
            return insertPreparedStatement.executeUpdate();
        } catch (Throwable e) {
            throw new RuntimeException("Insert data failed, sql: " + insertPreparedStatement + ", message: " + e.getMessage(), e);
        }
    }

    protected int setPreparedStatementValues(TapTable tapTable, TapRecordEvent tapRecordEvent, PreparedStatement preparedStatement) throws Throwable {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        int parameterIndex = 1;
        Map<String, Object> after = getAfter(tapRecordEvent);
        if (MapUtils.isEmpty(after)) {
            throw new Exception("Set prepared statement values failed, after is empty: " + tapRecordEvent);
        }
        List<String> afterKeys = new ArrayList<>(after.keySet());
        Collection<String> uniqueKeys = getUniqueKeys(tapTable);
        for (String fieldName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(fieldName);
            if (!needAddIntoPreparedStatementValues(tapField, tapRecordEvent)) {
                continue;
            }
            if (!(tapRecordEvent instanceof TapUpdateRecordEvent) || !uniqueKeys.contains(fieldName)) {
                preparedStatement.setObject(parameterIndex++, after.get(fieldName));
            }
            afterKeys.remove(fieldName);
        }
        if (CollectionUtils.isNotEmpty(afterKeys)) {
            Map<String, Object> missingAfter = new HashMap<>();
            afterKeys.forEach(k -> missingAfter.put(k, after.get(k)));
            TapLogger.warn(TAG, "Found fields in after data not exists in schema fields, will skip it: " + missingAfter);
        }
        return parameterIndex;
    }

    private int doUpdateOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        String updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        if (null != tapConnectorContext.getConnectorCapabilities()
                && null != tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY)) {
            updateDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        }
        final Map<String, PreparedStatement> updateMap = getJdbcCache().getUpdateMap();
        PreparedStatement updatePreparedStatement = getUpdatePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, updateMap);
        int parameterIndex = setPreparedStatementValues(tapTable, tapRecordEvent, updatePreparedStatement);
        setPreparedStatementWhere(tapTable, tapRecordEvent, updatePreparedStatement, parameterIndex);
        //不管是否更新成功，api返回的数据条数都是0
        int row = updatePreparedStatement.executeUpdate();
        if (row <= 0 && ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS.equals(updateDmlPolicy)) {
            doInsert(tapConnectorContext, tapTable, tapRecordEvent);
        }
        return row + 1;
    }

    protected PreparedStatement getUpdatePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, Map<String, PreparedStatement> updateMap) throws Throwable {
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = updateMap.get(key);
        if (null == preparedStatement) {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            String tableId = tapTable.getId();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("Create update prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + database + "\"'s database");
            }
            List<String> setList = new ArrayList<>();
            Collection<String> uniqueKeys = getUniqueKeys(tapTable);
            nameFieldMap.forEach((fieldName, field) -> {
                if (!needAddIntoPreparedStatementValues(field, tapRecordEvent)) {
                    return;
                }
                //不更新主键
                if (!uniqueKeys.contains(fieldName))
                    setList.add("`" + fieldName.toLowerCase() + "`=?");
            });
            List<String> whereList = new ArrayList<>();
            for (String uniqueKey : uniqueKeys) {
                whereList.add("`" + uniqueKey + "`=?");
            }
            String sql = String.format(UPDATE_SQL_TEMPLATE, database, tableId, String.join(",", setList), String.join(" AND ", whereList));
            try {
                preparedStatement = getConnection().prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create update prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create update prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            updateMap.put(key, preparedStatement);
        }
        return preparedStatement;
    }

    protected Collection<String> getUniqueKeys(TapTable tapTable) {
        return tapTable.primaryKeys(true);
    }

    private int doUpdate(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        final Map<String, PreparedStatement> updateMap = getJdbcCache().getUpdateMap();
        PreparedStatement updatePreparedStatement = getUpdatePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, updateMap);
        int parameterIndex = setPreparedStatementValues(tapTable, tapRecordEvent, updatePreparedStatement);
        setPreparedStatementWhere(tapTable, tapRecordEvent, updatePreparedStatement, parameterIndex);
        try {
            return updatePreparedStatement.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Update data failed, sql: " + updatePreparedStatement + ", message: " + e.getMessage(), e);
        }
    }

    protected void setPreparedStatementWhere(TapTable tapTable, TapRecordEvent tapRecordEvent, PreparedStatement preparedStatement, int parameterIndex) throws Throwable {
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


    private int doDeleteOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        final Map<String, PreparedStatement> deleteMap = getJdbcCache().getDeleteMap();
        PreparedStatement deletePreparedStatement = getDeletePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, deleteMap);
        setPreparedStatementWhere(tapTable, tapRecordEvent, deletePreparedStatement, 1);
        int row;
        try {
            row = deletePreparedStatement.executeUpdate();
        } catch (Throwable e) {
            throw new Exception("Delete data failed, sql: " + deletePreparedStatement + ", message: " + e.getMessage(), e);
        }
        return row + 1;
    }

    protected PreparedStatement getDeletePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, Map<String, PreparedStatement> deleteMap) throws Throwable {
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = deleteMap.get(key);
        if (null == preparedStatement) {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            String tableId = tapTable.getId();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("Create delete prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + database + "\"'s database");
            }
            List<String> whereList = new ArrayList<>();
            Collection<String> uniqueKeys = getUniqueKeys(tapTable);
            for (String uniqueKey : uniqueKeys) {
                whereList.add("`" + uniqueKey + "`=?");
            }
            String sql = String.format(DELETE_SQL_TEMPLATE, database, tableId, String.join(" AND ", whereList));
            try {
                preparedStatement = getConnection().prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create delete prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create delete prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            deleteMap.put(key, preparedStatement);
        }
        return preparedStatement;
    }


    private boolean rowExists(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        if (CollectionUtils.isEmpty(tapTable.primaryKeys(true))) {
            // not have any logic primary key(s)
            return false;
        }
        final Map<String, PreparedStatement> checkExistsMap = getJdbcCache().getCheckExistsMap();
        PreparedStatement checkRowExistsPreparedStatement = getCheckRowExistsPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, checkExistsMap);
        setPreparedStatementWhere(tapTable, tapRecordEvent, checkRowExistsPreparedStatement, 1);
        AtomicBoolean result = new AtomicBoolean(false);
        try {
            this.hive1JdbcContext.query(checkRowExistsPreparedStatement, rs -> {
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
                preparedStatement = getConnection().prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create check row exists prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create check row exists prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            checkExistsMap.put(key, preparedStatement);
        }
        return preparedStatement;
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

    protected Map<String, Object> getBefore(TapRecordEvent tapRecordEvent) {
        Map<String, Object> before = null;
        if (tapRecordEvent instanceof TapUpdateRecordEvent) {
            before = ((TapUpdateRecordEvent) tapRecordEvent).getBefore();
        } else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
            before = ((TapDeleteRecordEvent) tapRecordEvent).getBefore();
        }
        return before;
    }

    protected Map<String, Object> getAfter(TapRecordEvent tapRecordEvent) {
        Map<String, Object> after = null;
        if (tapRecordEvent instanceof TapInsertRecordEvent) {
            after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
        } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
            after = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
        }
        return after;
    }


    protected PreparedStatement getInsertPreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, Map<String, PreparedStatement> insertMap) throws Throwable {
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
            String sql = String.format(INSERT_SQL_TEMPLATE, database, tableId, String.join(",", questionMarks));

            try {
                preparedStatement = getConnection().prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create insert prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create insert prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            insertMap.put(key, preparedStatement);
        }
        return preparedStatement;
    }


    protected boolean needAddIntoPreparedStatementValues(TapField field, TapRecordEvent tapRecordEvent) {
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

    private JdbcCache getJdbcCache() {
        String name = Thread.currentThread().getName();
        JdbcCache jdbcCache = jdbcCacheMap.get(name);
        if (null == jdbcCache) {
            jdbcCache = new JdbcCache();
            jdbcCacheMap.put(name, jdbcCache);
        }
        return jdbcCache;
    }

    protected static class LRUOnRemoveMap<K, V> extends LRUMap<K, V> {

        private Consumer<Entry<K, V>> onRemove;

        public LRUOnRemoveMap(int maxSize, Consumer<Entry<K, V>> onRemove) {
            super(maxSize);
            this.onRemove = onRemove;
        }

        @Override
        protected boolean removeLRU(LinkEntry<K, V> entry) {
            onRemove.accept(entry);
            return super.removeLRU(entry);
        }

        @Override
        public void clear() {
            Set<Entry<K, V>> entries = this.entrySet();
            for (Entry<K, V> entry : entries) {
                onRemove.accept(entry);
            }
            super.clear();
        }

        @Override
        protected void removeEntry(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
            onRemove.accept(entry);
            super.removeEntry(entry, hashIndex, previous);
        }

        @Override
        protected void removeMapping(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
            onRemove.accept(entry);
            super.removeMapping(entry, hashIndex, previous);
        }
    }

    private static class JdbcCache {
        private final Map<String, PreparedStatement> insertMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
        private final Map<String, PreparedStatement> updateMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
        private final Map<String, PreparedStatement> deleteMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
        private final Map<String, PreparedStatement> checkExistsMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));

        public Map<String, PreparedStatement> getInsertMap() {
            return insertMap;
        }

        public Map<String, PreparedStatement> getUpdateMap() {
            return updateMap;
        }

        public Map<String, PreparedStatement> getDeleteMap() {
            return deleteMap;
        }

        public Map<String, PreparedStatement> getCheckExistsMap() {
            return checkExistsMap;
        }

        public void clear() {
            this.insertMap.clear();
            this.updateMap.clear();
            this.deleteMap.clear();
            this.checkExistsMap.clear();
        }
    }

    public Connection getConnection() {
        String name = Thread.currentThread().getName();
        Connection connection2 = connectionCacheMap.get(name);
        if (connection2 == null) {
            try {
                lock.lock();
                connection2 = connectionCacheMap.get(name);
                if (connection2 == null) {
                    connection2 = this.hive1JdbcContext.getConnection((Hive1Config) hive1JdbcContext.getConfig());
                    connectionCacheMap.put(name, connection2);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
        return connection2;
    }

    public void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        String sql = "CREATE TABLE IF NOT EXISTS " + hive1Config.getDatabase() + "." + tapTable.getId() + "(" + Hive1JDBCSqlMaker.buildColumnDefinition(tapTable, true);
        StringBuilder clusterBySB = new StringBuilder();
        if (EmptyKit.isNotEmpty(primaryKeys)) {
            for (String field : primaryKeys) {
                String escapeFieldStr = "`" + field + "`";
                clusterBySB.append(escapeFieldStr).append(",");
            }
        } else {
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();

            List<Map.Entry<String, TapField>> nameFields = nameFieldMap.entrySet().stream().sorted(Comparator.comparing(v ->
                    EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).collect(Collectors.toList());


            for (int i = 0; i < nameFields.size(); i++) {
                Map.Entry<String, TapField> tapFieldEntry = nameFields.get(i);
                TapField field = tapFieldEntry.getValue();
                String escapeFieldStr = "`" + field.getName() + "`";
                clusterBySB.append(escapeFieldStr);
                if (i < (nameFields.size() - 1)) {
                    clusterBySB.append(",");
                }
            }
            StringUtils.removeEnd(sql, ",");
        }

        String clusterStr = StringUtils.removeEnd(clusterBySB.toString(), ",");
        StringBuilder sb = new StringBuilder();
        sb.append("\n)");
        sb.append("\nCLUSTERED BY (" + clusterStr + ") INTO 2 BUCKETS STORED AS ORC \nTBLPROPERTIES ('transactional'='true')");
        sql = sql + sb.toString();
        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql);
            TapLogger.info("table 为:", "table->{}", tapTable.getId());
            hive1JdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
    }

    @Override
    public void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            hive1JdbcContext.execute("DROP TABLE IF EXISTS " + hive1Config.getDatabase() + "." + tapDropTableEvent.getTableId());
        } catch (SQLException e) {
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }

    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) {
        return hive1JdbcContext.queryAllTables(null).size();
    }

}

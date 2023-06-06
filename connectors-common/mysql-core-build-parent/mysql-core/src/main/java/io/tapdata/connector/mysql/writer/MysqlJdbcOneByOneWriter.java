package io.tapdata.connector.mysql.writer;

import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;

import java.sql.PreparedStatement;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author samuel
 * @Description
 * @create 2022-05-06 10:36
 **/
public class MysqlJdbcOneByOneWriter extends MysqlJdbcWriter {

    private static final String TAG = MysqlJdbcOneByOneWriter.class.getSimpleName();

    /**
     * Use {@link MysqlSqlBatchWriter}
     *
     * @param mysqlJdbcContext
     * @throws Throwable
     */
    @Deprecated
    private MysqlJdbcOneByOneWriter(MysqlJdbcContextV2 mysqlJdbcContext) throws Throwable {
        super(mysqlJdbcContext);
    }

    public MysqlJdbcOneByOneWriter(MysqlJdbcContextV2 mysqlJdbcContext, Map<String, JdbcCache> jdbcCacheMap) throws Throwable {
        super(mysqlJdbcContext, jdbcCacheMap);
    }

    @Override
    public WriteListResult<TapRecordEvent> write(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        TapRecordEvent errorRecord = null;
        try {
            for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
                if (!isAlive()) break;
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
                    if (!isAlive()) {
                        break;
                    }
                    errorRecord = tapRecordEvent;
                    throw e;
                }
            }
//			getJdbcCache().getConnection().commit();
        } catch (Throwable e) {
            writeListResult.setInsertedCount(0);
            writeListResult.setModifiedCount(0);
            writeListResult.setRemovedCount(0);
            if (null != errorRecord) writeListResult.addError(errorRecord, e);
//			getJdbcCache().getConnection().rollback();
            throw e;
        }
        return writeListResult;
    }

    @Override
    public void onDestroy() {
        synchronized (this.jdbcCacheMap) {
            this.jdbcCacheMap.values().forEach(JdbcCache::clear);
            this.jdbcCacheMap.clear();
        }
    }

    private int doInsertOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        String insertDmlPolicy = getDmlInsertPolicy(tapConnectorContext);
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
            if (rowExists(tapConnectorContext, tapTable, tapRecordEvent)) {
                row = doUpdate(tapConnectorContext, tapTable, tapRecordEvent);
            } else {
                row = doInsert(tapConnectorContext, tapTable, tapRecordEvent);
            }
        } catch (Throwable e) {
            if (e.getCause() instanceof SQLIntegrityConstraintViolationException) {
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

    protected int doInsert(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        PreparedStatement insertPreparedStatement = getInsertPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent);
        setPreparedStatementValues(tapConnectorContext, tapTable, tapRecordEvent, insertPreparedStatement);
        try {
            return insertPreparedStatement.executeUpdate();
        } catch (Throwable e) {
            throw exceptionWrapper.wrap(tapConnectorContext, tapTable, tapRecordEvent, e, (ex) -> {
                if (null == ex) {
                    return new RuntimeException(String.format("Insert data failed: %s\n Sql: %s", e.getMessage(), insertPreparedStatement), e);
                }
                return ex;
            });
        }
    }

    protected int doUpdateOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        String updateDmlPolicy = getDmlUpdatePolicy(tapConnectorContext);
        PreparedStatement updatePreparedStatement = getUpdatePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent);
        int parameterIndex = setPreparedStatementValues(tapConnectorContext, tapTable, tapRecordEvent, updatePreparedStatement);
        setPreparedStatementWhere(tapTable, tapRecordEvent, updatePreparedStatement, parameterIndex);
        int row = doUpdate(tapConnectorContext, tapTable, tapRecordEvent);
        if (row <= 0 && ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS.equals(updateDmlPolicy)) {
            doInsert(tapConnectorContext, tapTable, tapRecordEvent);
        }
        return row;
    }

    protected int doUpdate(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        PreparedStatement updatePreparedStatement = getUpdatePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent);
        int parameterIndex = setPreparedStatementValues(tapConnectorContext, tapTable, tapRecordEvent, updatePreparedStatement);
        setPreparedStatementWhere(tapTable, tapRecordEvent, updatePreparedStatement, parameterIndex);
        try {
            return updatePreparedStatement.executeUpdate();
        } catch (Exception e) {
            throw exceptionWrapper.wrap(tapConnectorContext, tapTable, tapRecordEvent, e, (ex) -> {
                if (null == ex) {
                    throw new RuntimeException(String.format("Update data failed: %s\n Sql: %s", e.getMessage(), updatePreparedStatement), e);
                }
                return ex;
            });
        }
    }

    protected int doDeleteOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        PreparedStatement deletePreparedStatement = getDeletePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent);
        setPreparedStatementWhere(tapTable, tapRecordEvent, deletePreparedStatement, 1);
        int row;
        try {
            row = deletePreparedStatement.executeUpdate();
        } catch (Throwable e) {
            throw exceptionWrapper.wrap(tapConnectorContext, tapTable, tapRecordEvent, e, (ex) -> {
                if (null == ex) {
                    throw new RuntimeException(String.format("Delete data failed: %s\n Sql: %s", e.getMessage(), deletePreparedStatement), e);
                }
                return ex;
            });
        }
        return row;
    }

    private boolean rowExists(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        if (CollectionUtils.isEmpty(tapTable.primaryKeys(true))) {
            // not have any logic primary key(s)
            return false;
        }
        PreparedStatement checkRowExistsPreparedStatement = getCheckRowExistsPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent);
        setPreparedStatementWhere(tapTable, tapRecordEvent, checkRowExistsPreparedStatement, 1);
        AtomicBoolean result = new AtomicBoolean(false);
        try {
            this.mysqlJdbcContext.query(checkRowExistsPreparedStatement, rs -> {
                if (rs.next()) {
                    int count = rs.getInt("count");
                    result.set(count > 0);
                }
            });
        } catch (Throwable e) {
            throw new Exception(String.format("Check row exists failed: %s\n Sql: %s", e.getMessage(), checkRowExistsPreparedStatement), e);
        }
        return result.get();
    }
}

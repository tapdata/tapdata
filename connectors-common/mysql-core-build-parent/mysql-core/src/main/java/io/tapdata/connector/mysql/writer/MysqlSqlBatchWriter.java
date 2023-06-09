package io.tapdata.connector.mysql.writer;

import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.util.ExceptionWrapper;
import io.tapdata.connector.mysql.util.MysqlUtil;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

/**
 * @author samuel
 * @Description
 * @create 2022-10-25 16:13
 **/
public class MysqlSqlBatchWriter extends MysqlJdbcWriter {
    private final static String TAG = MysqlSqlBatchWriter.class.getSimpleName();
    private static final String DELETE_FROM_SQL_TEMPLATE = "DELETE FROM `%s`.`%s` WHERE %s";
    protected MysqlJdbcOneByOneWriter mysqlJdbcOneByOneWriter;

    public MysqlSqlBatchWriter(MysqlJdbcContextV2 mysqlJdbcContext) throws Throwable {
        super(mysqlJdbcContext);
        this.mysqlJdbcOneByOneWriter = new MysqlJdbcOneByOneWriter(mysqlJdbcContext, jdbcCacheMap);
    }

    @Override
    public void setExceptionWrapper(ExceptionWrapper exceptionWrapper) {
        super.setExceptionWrapper(exceptionWrapper);
        this.mysqlJdbcOneByOneWriter.setExceptionWrapper(exceptionWrapper);
    }

    public void onDestroy() {
        mysqlJdbcOneByOneWriter.onDestroy();
        super.onDestroy();
    }

    @Override
    public WriteListResult<TapRecordEvent> write(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        AtomicReference<WriteListResult<TapRecordEvent>> writeListResult = new AtomicReference<>(new WriteListResult<>(0L, 0L, 0L, new HashMap<>()));
        try {
            dispatch(tapRecordEvents, consumeEvents -> {
                if (!isAlive()) return;
                boolean batch = false;
                try {
                    if (consumeEvents.get(0) instanceof TapInsertRecordEvent) {
                        if (canLargeInsertSql(tapTable)) {
                            batch = true;
                            writeListResult.get().incrementInserted(doInsert(tapConnectorContext, tapTable, consumeEvents));
                        } else {
                            doOneByOne(tapConnectorContext, tapTable, writeListResult, consumeEvents);
                        }
                    } else if (consumeEvents.get(0) instanceof TapUpdateRecordEvent) {
                        String dmlUpdatePolicy = getDmlUpdatePolicy(tapConnectorContext);
                        if (canReplaceInto(tapTable, dmlUpdatePolicy)) {
                            batch = true;
                            writeListResult.get().incrementModified(doUpdate(tapConnectorContext, tapTable, consumeEvents));
                        } else {
                            doOneByOne(tapConnectorContext, tapTable, writeListResult, consumeEvents);
                        }
                    } else if (consumeEvents.get(0) instanceof TapDeleteRecordEvent) {
                        batch = true;
                        writeListResult.get().incrementRemove(doDelete(tapConnectorContext, tapTable, consumeEvents));
                    }
                } catch (Throwable e) {
                    if (batch) {
                        getJdbcCache().getConnection().rollback();
                        if (isAlive()) {
                            TapLogger.warn(TAG, "Do batch operation failed: " + e.getMessage() + "\n Will try one by one mode");
                            doOneByOne(tapConnectorContext, tapTable, writeListResult, consumeEvents);
                        }
                    } else {
                        throw e;
                    }
                }
            });
            getJdbcCache().getConnection().commit();
        } catch (Throwable e) {
            if (isAlive()) {
                exceptionCollector.collectTerminateByServer(e);
                exceptionCollector.collectViolateNull(null, e);
                TapRecordEvent errorEvent = writeListResult.get().getErrorMap().keySet().stream().findFirst().orElse(null);
                exceptionCollector.collectViolateUnique(toJson(tapTable.primaryKeys(true)), errorEvent, null, e);
                exceptionCollector.collectWritePrivileges("writeRecord", Collections.emptyList(), e);
                exceptionCollector.collectWriteType(null, null, errorEvent, e);
                exceptionCollector.collectWriteLength(null, null, errorEvent, e);
                throw e;
            }
        }
        return writeListResult.get();
    }

    protected void doOneByOne(TapConnectorContext tapConnectorContext, TapTable tapTable, AtomicReference<WriteListResult<TapRecordEvent>> writeListResult, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        WriteListResult<TapRecordEvent> oneByOneInsertResult = mysqlJdbcOneByOneWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
        writeListResult.get().incrementInserted(oneByOneInsertResult.getInsertedCount());
        writeListResult.get().incrementModified(oneByOneInsertResult.getModifiedCount());
        writeListResult.get().incrementRemove(oneByOneInsertResult.getRemovedCount());
        writeListResult.get().addErrors(oneByOneInsertResult.getErrorMap());
    }

    protected boolean canLargeInsertSql(TapTable tapTable) {
        Collection<String> pkOrUniqueIndex = tapTable.primaryKeys();
        return !CollectionUtils.isEmpty(pkOrUniqueIndex);
    }

    private boolean canReplaceInto(TapTable tapTable, String updatePolicy) {
        Collection<String> pkOrUniqueIndex = tapTable.primaryKeys();
        if (CollectionUtils.isEmpty(pkOrUniqueIndex)) {
            return false;
        }
        return !ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS.equals(updatePolicy);
    }

    protected int doInsert(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        if (CollectionUtils.isEmpty(tapRecordEvents)) {
            return 0;
        }
        int result = 0;
        String sql;
        String dmlInsertPolicy = getDmlInsertPolicy(tapConnectorContext);
        if (EmptyKit.isEmpty(tapTable.primaryKeys(true))) {
            sql = appendLargeInsertSql(tapConnectorContext, tapTable, tapRecordEvents);
        } else if (ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(dmlInsertPolicy)) {
            sql = appendLargeInsertIgnoreSql(tapConnectorContext, tapTable, tapRecordEvents);
        } else {
            sql = appendLargeInsertOnDuplicateUpdateSql(tapConnectorContext, tapTable, tapRecordEvents);
        }
        TapLogger.debug(TAG, "Execute insert sql: " + sql);
        JdbcCache jdbcCache = getJdbcCache();
        try (Statement statement = jdbcCache.getStatement()) {
            while (isAlive()) {
                try {
                    result = statement.executeUpdate(sql);
                    break;
                } catch (SQLException e) {
                    if (e.getMessage().contains("Deadlock found when trying to get lock")) {
                        continue;
                    }
                    throw new RuntimeException("Execute insert sql failed: " + e.getMessage() + "\nSql: " + sql, e);
                }
            }
        }
//		jdbcCache.getConnection().commit();
        return result;
    }

    private int doUpdate(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        if (CollectionUtils.isEmpty(tapRecordEvents)) {
            return 0;
        }
        int result = 0;
//		String sql = appendReplaceIntoSql(tapConnectorContext, tapTable, tapRecordEvents);
        String sql = appendLargeInsertOnDuplicateUpdateSql(tapConnectorContext, tapTable, tapRecordEvents);
        TapLogger.debug(TAG, "Execute update sql: " + sql);
        JdbcCache jdbcCache = getJdbcCache();
        try (Statement statement = jdbcCache.getStatement()) {
            while (isAlive()) {
                try {
                    statement.execute(sql);
                    result = tapRecordEvents.size();
                    break;
                } catch (SQLException e) {
                    if (e.getMessage().contains("Deadlock found when trying to get lock")) {
                        continue;
                    }
                    throw new RuntimeException("Execute update sql failed: " + e.getMessage() + "\nSql: " + sql, e);
                }
            }
        }
//		getJdbcCache().getConnection().commit();
        return result;
    }

    protected int doDelete(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        if (CollectionUtils.isEmpty(tapRecordEvents)) {
            return 0;
        }
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        if (EmptyKit.isEmpty(primaryKeys)) {
            primaryKeys = tapTable.getNameFieldMap().keySet();
        }
        List<String> whereList = new ArrayList<>();
        for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
            Map<String, Object> before = ((TapDeleteRecordEvent) tapRecordEvent).getBefore();
            List<String> subWhereList = new ArrayList<>();
            for (String primaryKey : primaryKeys) {
                if (!before.containsKey(primaryKey)) {
                    throw new RuntimeException(String.format("Append delete sql failed, before data not contains key '%s', cannot append where clause in delete sql\nBefore data: %s", primaryKey, before));
                }
                subWhereList.add("`" + primaryKey + "`<=>" + MysqlUtil.object2String(before.get(primaryKey)));
            }
            whereList.add("(" + String.join(" AND ", subWhereList) + ")");
        }
        String whereClause = String.join(" OR ", whereList);
        String database = tapConnectorContext.getConnectionConfig().getString("database");
        String sql = String.format(DELETE_FROM_SQL_TEMPLATE, database, tapTable.getId(), whereClause);
        TapLogger.debug(TAG, "Execute delete sql: " + sql);
        JdbcCache jdbcCache = getJdbcCache();
        int deleted;
        try (Statement statement = jdbcCache.getStatement()) {
            try {
                deleted = statement.executeUpdate(sql);
            } catch (SQLException e) {
                throw new RuntimeException("Execute delete sql failed: " + e.getMessage() + "\nSql: " + sql, e);
            }
        }
//		getJdbcCache().getConnection().commit();
        return deleted;
    }
}

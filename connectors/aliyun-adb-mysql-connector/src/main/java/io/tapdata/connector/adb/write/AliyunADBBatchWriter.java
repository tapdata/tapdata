package io.tapdata.connector.adb.write;

import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.writer.MysqlSqlBatchWriter;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author GavinXiao
 * @description AliyunADBWriter create by Gavin
 * @create 2023/6/5 14:09
 **/
public class AliyunADBBatchWriter extends MysqlSqlBatchWriter implements WriteStage {
    private final static String TAG = AliyunADBBatchWriter.class.getSimpleName();
    private static final String DELETE_FROM_SQL_TEMPLATE = "DELETE FROM `%s`.`%s` WHERE %s";

    public AliyunADBBatchWriter(MysqlJdbcContextV2 mysqlJdbcContext) throws Throwable {
        super(mysqlJdbcContext);
        mysqlJdbcOneByOneWriter = new AliyunOneByOneWriter(mysqlJdbcContext, jdbcCacheMap);
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
//                        String dmlUpdatePolicy = getDmlUpdatePolicy(tapConnectorContext);
//                        if (canReplaceInto(tapTable, dmlUpdatePolicy)) {
//                            batch = true;
//                            writeListResult.get().incrementModified(doUpdate(tapConnectorContext, tapTable, consumeEvents));
//                        } else {
                            doOneByOne(tapConnectorContext, tapTable, writeListResult, consumeEvents);
//                        }
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
                throw e;
            }
        }
        return writeListResult.get();
    }

    protected int doUpdate(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
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

    @Override
    public int splitToInsertAndDeleteFromUpdate(TapConnectorContext context, TapUpdateRecordEvent event, TapTable table) throws Throwable {
//        if (null == event){
//            return;
//        }
//        Map<String, Object> before = event.getBefore();
//        Map<String, Object> after = event.getAfter();
//        if (null == before || before.isEmpty() || null == after || after.isEmpty()){
//            int updateRow = super.doUpdateOne(context, table, event);
//
//
//            result.incrementModified(updateRow);
//            return;
//        }
//        if (Objects.isNull(table)){
//            throw new CoreException("TapTable can not be empty, update event will be cancel");
//        }
//        String tableId = table.getId();
//        Long referenceTime = event.getReferenceTime();
//
//        if (canLargeInsertSql(tapTable)) {
//            batch = true;
//            writeListResult.get().incrementInserted(doInsert(tapConnectorContext, tapTable, consumeEvents));
//        } else {
//            doOneByOne(tapConnectorContext, tapTable, writeListResult, consumeEvents);
//        }
//        result.incrementRemove(doDelete(context, table, deleteDMLEvent(before, tableId).referenceTime(referenceTime)));
//
//        if (canLargeInsertSql(table)) {
//            doInsert(context, table, consumeEvents)
//        } else {
//            doOneByOne(tapConnectorContext, tapTable, writeListResult, consumeEvents);
//        }
//        writeListResult.get().incrementRemove(doDelete(tapConnectorContext, tapTable, consumeEvents));
//        int addCount = doInsertOne(context, table, insertRecordEvent(after, tableId).referenceTime(referenceTime));
//        result.incrementInserted(addCount);
        return 1;
    }
}

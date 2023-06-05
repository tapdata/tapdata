package io.tapdata.connector.adb.write;

import io.tapdata.connector.mysql.writer.MysqlJdbcOneByOneWriter;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.tapdata.base.ConnectorBase.deleteDMLEvent;
import static io.tapdata.base.ConnectorBase.insertRecordEvent;
import static io.tapdata.base.ConnectorBase.toJson;

/**
 * @author GavinXiao
 * @description AliyunOneByOneWriter create by Gavin
 * @create 2023/6/5 14:11
 **/
public class AliyunOneByOneWriter extends MysqlJdbcOneByOneWriter implements WriteStage{

    private static final String TAG = AliyunOneByOneWriter.class.getSimpleName();

    public AliyunOneByOneWriter(MysqlJdbcContext mysqlJdbcContext, Map<String, JdbcCache> jdbcCacheMap) throws Throwable {
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
                        if (hasEqualsValueOfPrimaryKey(tapConnectorContext, (TapUpdateRecordEvent) tapRecordEvent, tapTable)) {
                            try {
                                int updateRow = doUpdateOne(tapConnectorContext, tapTable, tapRecordEvent);
                                writeListResult.incrementModified(updateRow);
                            } catch (Exception e) {
                                tapConnectorContext.getLog().error("A record fail to update event, message: %s, %s", e.getMessage(), toJson(tapRecordEvent));
                            }
                        } else {
                                splitToInsertAndDeleteFromUpdate(tapConnectorContext, (TapUpdateRecordEvent) tapRecordEvent, tapTable, writeListResult);
                        }
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
        } catch (Throwable e) {
            writeListResult.setInsertedCount(0);
            writeListResult.setModifiedCount(0);
            writeListResult.setRemovedCount(0);
            if (null != errorRecord) writeListResult.addError(errorRecord, e);
            throw e;
        }
        return writeListResult;
    }

    @Override
    protected int doUpdateOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        removePrimaryKeys(tapTable, (TapUpdateRecordEvent) tapRecordEvent);
        return super.doUpdateOne(tapConnectorContext, tapTable, tapRecordEvent);
    }

    @Override
    public void splitToInsertAndDeleteFromUpdate(TapConnectorContext context, TapUpdateRecordEvent event, TapTable table, WriteListResult<TapRecordEvent> result) throws Throwable {
        if (null == event){
            return;
        }
        Map<String, Object> before = event.getBefore();
        Map<String, Object> after = event.getAfter();
        if (null == before || before.isEmpty() || null == after || after.isEmpty()){
            int updateRow = super.doUpdateOne(context, table, event);
            result.incrementModified(updateRow);
            return;
        }
        if (Objects.isNull(table)){
            throw new CoreException("TapTable can not be empty, update event will be cancel");
        }
        String tableId = table.getId();
        Long referenceTime = event.getReferenceTime();

        int deleteCount = doDeleteOne(context, table, deleteDMLEvent(before, tableId).referenceTime(referenceTime));
        result.incrementRemove(deleteCount);

        int addCount = doInsertOne(context, table, insertRecordEvent(after, tableId).referenceTime(referenceTime));
        result.incrementInserted(addCount);
    }
}

package io.tapdata.oceanbase;

import com.google.common.collect.Maps;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.JdbcContext;
import io.tapdata.common.RecordWriter;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.oceanbase.connector.OceanbaseJdbcContext;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @Author dayun
 * @Date 8/24/22
 */
public class OceanbaseRecordWriter extends RecordWriter implements AutoCloseable {

    private final OceanbaseWriteRecorder singleInsertRecorder;
    private final OceanbaseWriteRecorder singleUpdateRecorder;
    private final OceanbaseWriteRecorder singleDeleteRecorder;

    public OceanbaseRecordWriter(OceanbaseJdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        singleInsertRecorder = new OceanbaseWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        singleUpdateRecorder = new OceanbaseWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        singleDeleteRecorder = new OceanbaseWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());

//        writer = new OceanbaseWriter(jdbcContext);
        // TODO: 2022/6/29 加insert、update策略
//        insertRecorder.setInsertPolicy("");
//        updateRecorder.setUpdatePolicy("");
    }

    public void writeRecord(final List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        singleInsertRecorder.setVersion(version);
        singleInsertRecorder.setInsertPolicy(insertPolicy);
        singleUpdateRecorder.setVersion(version);
        singleUpdateRecorder.setUpdatePolicy(updatePolicy);
        singleDeleteRecorder.setVersion(version);
        //result of these events
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();

        for (TapRecordEvent recordEvent : tapRecordEvents) {
            if (recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                singleInsertRecorder.addInsertBatch(insertRecordEvent.getAfter());
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                int cachedInsertSucceed = singleInsertRecorder.executeBatchInsert();
                singleInsertRecorder.addAndCheckCommit(cachedInsertSucceed);

                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                int updateSucceed = singleUpdateRecorder.executeUpdate(updateRecordEvent.getAfter());
                singleUpdateRecorder.addAndCheckCommit(updateSucceed);
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                int cachedInsertSucceed = singleInsertRecorder.executeBatchInsert();
                singleInsertRecorder.addAndCheckCommit(cachedInsertSucceed);

                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                int deleteSucceed = singleDeleteRecorder.executeDelete(deleteRecordEvent.getBefore());
                singleDeleteRecorder.addAndCheckCommit(deleteSucceed);
            }
        }
        int cachedInsertSucceed = singleInsertRecorder.executeBatchInsert();
        singleInsertRecorder.addAndCheckCommit(cachedInsertSucceed);

        if (!connection.getAutoCommit()) {
            connection.commit();
        }

        writeListResultConsumer.accept(listResult
                .insertedCount(singleInsertRecorder.getAtomicLong().get())
                .modifiedCount(singleUpdateRecorder.getAtomicLong().get())
                .removedCount(singleDeleteRecorder.getAtomicLong().get()));
        singleInsertRecorder.getAtomicLong().set(0);
        singleUpdateRecorder.getAtomicLong().set(0);
        singleDeleteRecorder.getAtomicLong().set(0);
    }

    @Override
    public void close() throws Exception {
        singleInsertRecorder.releaseResource();
        singleUpdateRecorder.releaseResource();
        singleDeleteRecorder.releaseResource();
        connection.close();
    }

    // todo by dayun
//    private boolean makeSureHasUnique(OceanbaseJdbcContext jdbcContext, TapTable tapTable) {
//        return jdbcContext.queryAllIndexes(Collections.singletonList(tapTable.getId())).stream().anyMatch(v -> (boolean) v.get("is_unique"));
//    }

}

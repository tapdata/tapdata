package io.tapdata.common;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

public class RecordWriter {

    protected WriteRecorder insertRecorder;
    protected WriteRecorder updateRecorder;
    protected WriteRecorder deleteRecorder;
    protected String version;
    protected Connection connection;
    protected final TapTable tapTable;

    public RecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        this.connection = jdbcContext.getConnection();
        this.tapTable = tapTable;
    }

    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        insertRecorder.setVersion(version);
        updateRecorder.setVersion(version);
        deleteRecorder.setVersion(version);
        //result of these events
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            if (recordEvent instanceof TapInsertRecordEvent) {
                updateRecorder.executeBatch(listResult);
                deleteRecorder.executeBatch(listResult);
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                insertRecorder.addInsertBatch(insertRecordEvent.getAfter());
                insertRecorder.addAndCheckCommit(recordEvent, listResult);
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                insertRecorder.executeBatch(listResult);
                deleteRecorder.executeBatch(listResult);
                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                updateRecorder.addUpdateBatch(updateRecordEvent.getAfter());
                updateRecorder.addAndCheckCommit(recordEvent, listResult);
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                insertRecorder.executeBatch(listResult);
                updateRecorder.executeBatch(listResult);
                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                deleteRecorder.addDeleteBatch(deleteRecordEvent.getBefore());
                deleteRecorder.addAndCheckCommit(recordEvent, listResult);
            }
        }
        insertRecorder.executeBatch(listResult);
        updateRecorder.executeBatch(listResult);
        deleteRecorder.executeBatch(listResult);
        connection.commit();
        insertRecorder.releaseResource();
        updateRecorder.releaseResource();
        deleteRecorder.releaseResource();
        connection.close();
        writeListResultConsumer.accept(listResult
                .insertedCount(insertRecorder.getAtomicLong().get())
                .modifiedCount(updateRecorder.getAtomicLong().get())
                .removedCount(deleteRecorder.getAtomicLong().get()));
    }

    public RecordWriter setVersion(String version) {
        this.version = version;
        return this;
    }
}

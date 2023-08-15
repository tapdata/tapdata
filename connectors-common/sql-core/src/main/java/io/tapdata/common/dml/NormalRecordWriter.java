package io.tapdata.common.dml;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class NormalRecordWriter {

    protected NormalWriteRecorder insertRecorder;
    protected String insertPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
    protected NormalWriteRecorder updateRecorder;
    protected String updatePolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
    protected NormalWriteRecorder deleteRecorder;
    protected String version;
    protected Connection connection;
    protected final TapTable tapTable;
    protected ExceptionCollector exceptionCollector = new AbstractExceptionCollector() {
    };
    protected boolean isTransaction = false;
    protected Log tapLogger;

    public NormalRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        this.connection = jdbcContext.getConnection();
        this.tapTable = tapTable;
    }

    public NormalRecordWriter(Connection connection, TapTable tapTable) {
        this.connection = connection;
        this.tapTable = tapTable;
        isTransaction = true;
    }

    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) throws SQLException {
        //result of these events
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        try {
            insertRecorder.setVersion(version);
            insertRecorder.setInsertPolicy(insertPolicy);
            insertRecorder.setTapLogger(tapLogger);
            updateRecorder.setVersion(version);
            updateRecorder.setUpdatePolicy(updatePolicy);
            updateRecorder.setTapLogger(tapLogger);
            deleteRecorder.setVersion(version);
            deleteRecorder.setTapLogger(tapLogger);
            //insert,update,delete events must consecutive, so execute the other two first
            for (TapRecordEvent recordEvent : tapRecordEvents) {
                if (null != isAlive && !isAlive.get()) {
                    break;
                }
                if (recordEvent instanceof TapInsertRecordEvent) {
                    updateRecorder.executeBatch(listResult);
                    deleteRecorder.executeBatch(listResult);
                    TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                    insertRecorder.addInsertBatch(insertRecordEvent.getAfter(), listResult);
                    insertRecorder.addAndCheckCommit(recordEvent, listResult);
                } else if (recordEvent instanceof TapUpdateRecordEvent) {
                    insertRecorder.executeBatch(listResult);
                    deleteRecorder.executeBatch(listResult);
                    TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                    updateRecorder.addUpdateBatch(updateRecordEvent.getAfter(), updateRecordEvent.getBefore(), listResult);
                    updateRecorder.addAndCheckCommit(recordEvent, listResult);
                } else if (recordEvent instanceof TapDeleteRecordEvent) {
                    insertRecorder.executeBatch(listResult);
                    updateRecorder.executeBatch(listResult);
                    TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                    deleteRecorder.addDeleteBatch(deleteRecordEvent.getBefore(), listResult);
                    deleteRecorder.addAndCheckCommit(recordEvent, listResult);
                }
            }
            insertRecorder.executeBatch(listResult);
            updateRecorder.executeBatch(listResult);
            deleteRecorder.executeBatch(listResult);
            //some datasource must be auto commit, error will occur when commit
            if (!connection.getAutoCommit() && !isTransaction) {
                connection.commit();
            }
            //release resource

        } catch (SQLException e) {
            exceptionCollector.collectTerminateByServer(e);
            exceptionCollector.collectViolateNull(null, e);
            TapRecordEvent errorEvent = listResult.getErrorMap().keySet().stream().findFirst().orElse(null);
            exceptionCollector.collectViolateUnique(toJson(tapTable.primaryKeys(true)), errorEvent, null, e);
            exceptionCollector.collectWritePrivileges("writeRecord", Collections.emptyList(), e);
            exceptionCollector.collectWriteType(null, null, errorEvent, e);
            exceptionCollector.collectWriteLength(null, null, errorEvent, e);
            exceptionCollector.revealException(e);
            throw e;
        } finally {
            insertRecorder.releaseResource();
            updateRecorder.releaseResource();
            deleteRecorder.releaseResource();
            if (!isTransaction) {
                connection.close();
            }
            writeListResultConsumer.accept(listResult
                    .insertedCount(insertRecorder.getAtomicLong().get())
                    .modifiedCount(updateRecorder.getAtomicLong().get())
                    .removedCount(deleteRecorder.getAtomicLong().get()));
        }
    }

    public NormalRecordWriter setVersion(String version) {
        this.version = version;
        return this;
    }

    public NormalRecordWriter setInsertPolicy(String insertPolicy) {
        this.insertPolicy = insertPolicy;
        return this;
    }

    public NormalRecordWriter setUpdatePolicy(String updatePolicy) {
        this.updatePolicy = updatePolicy;
        return this;
    }

    public NormalRecordWriter setTapLogger(Log tapLogger) {
        this.tapLogger = tapLogger;
        return this;
    }
}

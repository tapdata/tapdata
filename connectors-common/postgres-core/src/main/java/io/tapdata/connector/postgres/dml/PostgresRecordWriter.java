package io.tapdata.connector.postgres.dml;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.schema.TapTable;

import java.sql.Connection;
import java.sql.SQLException;

public class PostgresRecordWriter extends NormalRecordWriter {

    public PostgresRecordWriter(JdbcContext jdbcContext, TapTable tapTable, String version) throws SQLException {
        super(jdbcContext, tapTable);
        exceptionCollector = new PostgresExceptionCollector();
        if (Integer.parseInt(version) > 90500) {
            insertRecorder = new ConflictWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            updateRecorder = new ConflictWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            deleteRecorder = new ConflictWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        } else {
            insertRecorder = new OldPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            updateRecorder = new OldPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            deleteRecorder = new OldPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        }
    }

    public PostgresRecordWriter(JdbcContext jdbcContext, Connection connection, TapTable tapTable, String version) throws SQLException {
        super(connection, tapTable);
        exceptionCollector = new PostgresExceptionCollector();
        if (Integer.parseInt(version) > 90500) {
            insertRecorder = new ConflictWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            updateRecorder = new ConflictWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            deleteRecorder = new ConflictWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        } else {
            insertRecorder = new OldPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            updateRecorder = new OldPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            deleteRecorder = new OldPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        }
    }

}

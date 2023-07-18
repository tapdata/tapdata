package io.tapdata.connector.greenplum;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.RecordWriter;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;

public class GreenplumRecordWriter extends RecordWriter {

    public GreenplumRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        exceptionCollector = new PostgresExceptionCollector();
        insertRecorder = new GreenplumWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new GreenplumWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new GreenplumWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

}

package io.tapdata.connector.clickhouse.dml;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.connector.clickhouse.ClickhouseExceptionCollector;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;

public class ClickhouseRecordWriter extends NormalRecordWriter {

    public ClickhouseRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        exceptionCollector = new ClickhouseExceptionCollector();
        insertRecorder = new ClickhouseWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new ClickhouseWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new ClickhouseWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

}

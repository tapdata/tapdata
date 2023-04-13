package io.tapdata.connector.adb;


import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.PostgresRecordWriter;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;


public class AliyunADBPostgresRecordWriter extends PostgresRecordWriter {
    public AliyunADBPostgresRecordWriter(PostgresJdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        insertRecorder = new AliyunADBPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new AliyunADBPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

}
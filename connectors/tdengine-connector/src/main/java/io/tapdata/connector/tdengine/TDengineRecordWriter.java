package io.tapdata.connector.tdengine;

import io.tapdata.common.RecordWriter;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;

public class TDengineRecordWriter extends RecordWriter {

    public TDengineRecordWriter(TDengineJdbcContext jdbcContext, TapTable tapTable, String timestampField) throws SQLException {
        super(jdbcContext, tapTable);
        insertRecorder = new TDengineWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase(), timestampField);
        updateRecorder = new TDengineWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase(), timestampField);
        deleteRecorder = new TDengineWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase(), timestampField);
    }

}

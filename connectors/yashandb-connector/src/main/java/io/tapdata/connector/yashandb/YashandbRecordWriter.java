package io.tapdata.connector.yashandb;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.RecordWriter;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;

/**
 * Author:Skeet
 * Date: 2023/5/25
 **/
public class YashandbRecordWriter extends RecordWriter {

    public YashandbRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        insertRecorder = new YashandbWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new YashandbWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new YashandbWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }
}

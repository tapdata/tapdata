package io.tapdata.connector.dameng.dml;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.RecordWriter;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;

public class DamengRecordWriter extends RecordWriter {

    public DamengRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        insertRecorder = new DamengWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new DamengWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new DamengWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

}

package io.tapdata.connector.gbase8s;

import io.tapdata.common.RecordWriter;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;

public class Gbase8sRecordWriter extends RecordWriter {

    public Gbase8sRecordWriter(Gbase8sJdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        insertRecorder = new Gbase8sWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new Gbase8sWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new Gbase8sWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

}

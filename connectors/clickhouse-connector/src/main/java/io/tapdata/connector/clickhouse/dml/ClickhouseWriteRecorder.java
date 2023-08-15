package io.tapdata.connector.clickhouse.dml;

import io.tapdata.common.dml.NormalWriteRecorder;
import io.tapdata.entity.schema.TapTable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class ClickhouseWriteRecorder extends NormalWriteRecorder {

    public ClickhouseWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
        setEscapeChar('`');
    }


}

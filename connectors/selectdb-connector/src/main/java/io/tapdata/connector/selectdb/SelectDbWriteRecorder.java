package io.tapdata.connector.selectdb;

import io.tapdata.common.WriteRecorder;
import io.tapdata.entity.schema.TapTable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Author:Skeet
 * Date: 2022/12/8 16:25
 **/
public class SelectDbWriteRecorder extends WriteRecorder {
    public SelectDbWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    @Override
    public void addInsertBatch(Map<String, Object> after) throws SQLException {

    }
}

package io.tapdata.connector.selectdb;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.RecordWriter;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;

/**
 * Author:Skeet
 * Date: 2022/12/8
 **/
public class SelectDbRecordWriter extends RecordWriter {
    public SelectDbRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
    }
}

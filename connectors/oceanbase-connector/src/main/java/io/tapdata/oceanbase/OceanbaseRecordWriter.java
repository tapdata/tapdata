package io.tapdata.oceanbase;

import io.tapdata.common.RecordWriter;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.oceanbase.connector.OceanbaseJdbcContext;

import java.sql.SQLException;

/**
 * @Author dayun
 * @Date 8/24/22
 */
public class OceanbaseRecordWriter extends RecordWriter {

    public OceanbaseRecordWriter(OceanbaseJdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        insertRecorder = new OceanbaseWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        updateRecorder = new OceanbaseWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        deleteRecorder = new OceanbaseWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        // TODO: 2022/6/29 加insert、update策略
//        insertRecorder.setInsertPolicy("");
//        updateRecorder.setUpdatePolicy("");
    }

//    private boolean makeSureHasUnique(OceanbaseJdbcContext jdbcContext, TapTable tapTable) {
//        return jdbcContext.queryAllIndexes(Collections.singletonList(tapTable.getId())).stream().anyMatch(v -> (boolean) v.get("is_unique"));
//    }
}

package io.tapdata.connector.tidb.dml;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.RecordWriter;
import io.tapdata.connector.tidb.TidbJdbcRunner;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;
import java.util.Collections;

/**
 * @author lemon
 */
public class TidbRecordWrite extends RecordWriter {

    public TidbRecordWrite(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        insertRecorder = new TidbWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        updateRecorder = new TidbWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        deleteRecorder = new TidbWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
    }
    private boolean makeSureHasUnique(TidbJdbcRunner jdbcContext, TapTable tapTable) {
        return jdbcContext.queryAllIndexes(Collections.singletonList(tapTable.getId())).stream().anyMatch(v -> (boolean) v.get("is_unique"));
    }
}

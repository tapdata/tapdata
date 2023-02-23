package io.tapdata.connector.gauss;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.RecordWriter;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;

import java.sql.SQLException;
import java.util.Collections;

public class GaussRecordWriter extends RecordWriter {

    public GaussRecordWriter(GaussJdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        openIdentity(jdbcContext);
        insertRecorder = new GaussWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema(), makeSureHasUnique(jdbcContext, tapTable));
        updateRecorder = new GaussWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new GaussWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

    private void openIdentity(JdbcContext jdbcContext) throws SQLException {
        if (EmptyKit.isEmpty(tapTable.primaryKeys())
                && (EmptyKit.isEmpty(tapTable.getIndexList()) || tapTable.getIndexList().stream().noneMatch(TapIndex::isUnique))) {
            jdbcContext.execute("ALTER TABLE \"" + jdbcContext.getConfig().getSchema() + "\".\"" + tapTable.getId() + "\" REPLICA IDENTITY FULL");
        }
    }

    protected boolean makeSureHasUnique(GaussJdbcContext jdbcContext, TapTable tapTable) {
        return jdbcContext.queryAllIndexes(Collections.singletonList(tapTable.getId())).stream().anyMatch(v -> (boolean) v.get("is_unique"));
    }
}

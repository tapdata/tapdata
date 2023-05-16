package io.tapdata.connector.postgres;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.RecordWriter;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;

public class PostgresRecordWriter extends RecordWriter {

    public PostgresRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        openIdentity(jdbcContext);
        exceptionCollector = new PostgresExceptionCollector();
        insertRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema(), makeSureHasUnique(jdbcContext, tapTable));
        updateRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

    public PostgresRecordWriter(JdbcContext jdbcContext, Connection connection, TapTable tapTable) throws SQLException {
        super(connection, tapTable);
        openIdentity(jdbcContext);
        exceptionCollector = new PostgresExceptionCollector();
        insertRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema(), makeSureHasUnique(jdbcContext, tapTable));
        updateRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

    private void openIdentity(JdbcContext jdbcContext) throws SQLException {
        if (EmptyKit.isEmpty(tapTable.primaryKeys())
                && (EmptyKit.isEmpty(tapTable.getIndexList()) || tapTable.getIndexList().stream().noneMatch(TapIndex::isUnique))) {
            jdbcContext.execute("ALTER TABLE \"" + jdbcContext.getConfig().getSchema() + "\".\"" + tapTable.getId() + "\" REPLICA IDENTITY FULL");
        }
    }

    protected boolean makeSureHasUnique(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        return jdbcContext.queryAllIndexes(Collections.singletonList(tapTable.getId())).stream().anyMatch(v -> "1".equals(v.getString("isUnique")));
    }

}

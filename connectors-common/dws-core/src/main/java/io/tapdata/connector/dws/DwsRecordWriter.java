package io.tapdata.connector.dws;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.RecordWriter;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.connector.dws.bean.DwsTapTable;
import io.tapdata.connector.dws.config.DwsConfig;
import io.tapdata.connector.dws.exception.DwsExceptionCollector;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public class DwsRecordWriter extends NormalRecordWriter {
    public DwsRecordWriter(JdbcContext jdbcContext, DwsTapTable dwsTapTable) throws SQLException {
        super(jdbcContext, dwsTapTable.getTapTable());
        exceptionCollector = new DwsExceptionCollector();
        if (!dwsTapTable.isPartition()){
            openIdentity(jdbcContext);
        }
        exceptionCollector = new DwsExceptionCollector();
        insertRecorder = new DwsWriteRecorder(connection, dwsTapTable, jdbcContext.getConfig().getSchema(), makeSureHasUnique(jdbcContext, dwsTapTable.getTapTable()));
        updateRecorder = new DwsWriteRecorder(connection, dwsTapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new DwsWriteRecorder(connection, dwsTapTable, jdbcContext.getConfig().getSchema());
    }

    public DwsRecordWriter(JdbcContext jdbcContext, Connection connection, DwsTapTable dwsTapTable) throws SQLException {
        super(connection, dwsTapTable.getTapTable());
        if (!dwsTapTable.isPartition()){
            openIdentity(jdbcContext);
        }
        exceptionCollector = new DwsExceptionCollector();
        insertRecorder = new DwsWriteRecorder(connection, dwsTapTable, jdbcContext.getConfig().getSchema(), makeSureHasUnique(jdbcContext, dwsTapTable.getTapTable()));
        updateRecorder = new DwsWriteRecorder(connection, dwsTapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new DwsWriteRecorder(connection, dwsTapTable, jdbcContext.getConfig().getSchema());
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

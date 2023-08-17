package io.tapdata.connector.postgres.dml;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;

public class ConflictWriteRecorder extends PostgresWriteRecorder {

    public ConflictWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    @Override
    protected void upsert(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            preparedStatement = connection.prepareStatement(getUpsertSql());
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
        }
    }

    protected String getUpsertSql() {
        return "INSERT INTO " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " ("
                + allColumn.stream().map(k -> escapeChar + k + escapeChar).collect(Collectors.joining(", ")) + ") " +
                "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON CONFLICT("
                + uniqueCondition.stream().map(k -> escapeChar + k + escapeChar).collect(Collectors.joining(", "))
                + ") DO UPDATE SET " + allColumn.stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(", "));
    }

    @Override
    protected void insertIgnore(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            preparedStatement = connection.prepareStatement(getInsertIgnoreSql());
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
        }
    }

    protected String getInsertIgnoreSql() {
        return "INSERT INTO " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " ("
                + allColumn.stream().map(k -> escapeChar + k + escapeChar).collect(Collectors.joining(", ")) + ") " +
                "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON CONFLICT("
                + uniqueCondition.stream().map(k -> escapeChar + k + escapeChar).collect(Collectors.joining(", "))
                + ") DO NOTHING ";
    }

}

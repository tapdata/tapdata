package io.tapdata.connector.postgres;

import io.tapdata.common.WriteRecorder;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;

public class PostgresWriteRecorder extends WriteRecorder {

    public PostgresWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    public PostgresWriteRecorder(Connection connection, TapTable tapTable, String schema, boolean hasUnique) {
        super(connection, tapTable, schema);
        uniqueConditionIsIndex = uniqueConditionIsIndex && hasUnique;
    }

    @Override
    public void addInsertBatch(Map<String, Object> after) throws SQLException {
        //after is empty will be skipped
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        //insert into all columns, make preparedStatement
        if (EmptyKit.isNull(preparedStatement)) {
            String insertHead = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") ";
            String insertValue = "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ";
            String insertSql = insertHead + insertValue;
            if (EmptyKit.isNotEmpty(uniqueCondition)) {
                if (Integer.parseInt(version) > 90500 && uniqueConditionIsIndex) {
                    insertSql += "ON CONFLICT("
                            + uniqueCondition.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", "))
                            + ") DO UPDATE SET " + allColumn.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", "));
                } else {
                    if (hasPk) {
                        insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + allColumn.stream().map(k -> "\"" + k + "\"=?")
                                .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "\"" + k + "\"=?")
                                .collect(Collectors.joining(" AND ")) + " RETURNING *) " + insertHead + " SELECT "
                                + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
                    } else {
                        insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + allColumn.stream().map(k -> "\"" + k + "\"=?")
                                .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                                .collect(Collectors.joining(" AND ")) + " RETURNING *) " + insertHead + " SELECT "
                                + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
                    }
                }
            }
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        //make params
        int pos = 1;
        if ((Integer.parseInt(version) <= 90500 || !uniqueConditionIsIndex) && EmptyKit.isNotEmpty(uniqueCondition)) {
            for (String key : allColumn) {
                preparedStatement.setObject(pos++, after.get(key));
            }
            if (hasPk) {
                for (String key : uniqueCondition) {
                    preparedStatement.setObject(pos++, after.get(key));
                }
            } else {
                for (String key : uniqueCondition) {
                    preparedStatement.setObject(pos++, after.get(key));
                    preparedStatement.setObject(pos++, after.get(key));
                }
            }
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        if (EmptyKit.isNotEmpty(uniqueCondition) && Integer.parseInt(version) > 90500 && uniqueConditionIsIndex) {
            for (String key : allColumn) {
                preparedStatement.setObject(pos++, after.get(key));
            }
        }
        preparedStatement.addBatch();
    }
}

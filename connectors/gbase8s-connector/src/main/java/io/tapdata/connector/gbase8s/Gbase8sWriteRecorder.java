package io.tapdata.connector.gbase8s;

import io.tapdata.common.WriteRecorder;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;

public class Gbase8sWriteRecorder extends WriteRecorder {

    public Gbase8sWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    @Override
    public void addInsertBatch(Map<String, Object> after) throws SQLException {
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
                if (hasPk) {
                    insertSql = "MERGE INTO \"" + schema + "\".\"" + tapTable.getId() + "\" USING dual ON ("
                            + uniqueCondition.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND "))
                            + ")" + (allColumn.size() == uniqueCondition.size() ? "" : (" WHEN MATCHED THEN UPDATE SET " + allColumn.stream().filter(col -> !uniqueCondition.contains(col))
                            .map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")))) + " WHEN NOT MATCHED THEN INSERT " + insertValue;
                } else {
                    insertSql = "MERGE INTO \"" + schema + "\".\"" + tapTable.getId() + "\" USING dual ON ("
                            + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ? IS NULL))").collect(Collectors.joining(" AND "))
                            + ")" + (allColumn.size() == uniqueCondition.size() ? "" : (" WHEN MATCHED THEN UPDATE SET " + allColumn.stream().filter(col -> !uniqueCondition.contains(col))
                            .map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")))) + " WHEN NOT MATCHED THEN INSERT " + insertValue;
                }
            }
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        //make params
        int pos = 1;
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
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
            for (String key : allColumn.stream().filter(col -> !uniqueCondition.contains(col)).collect(Collectors.toList())) {
                preparedStatement.setObject(pos++, after.get(key));
            }
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        preparedStatement.addBatch();
    }

}

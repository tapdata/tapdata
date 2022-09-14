package io.tapdata.oceanbase;

import io.tapdata.common.WriteRecorder;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import org.apache.commons.collections4.MapUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @Author dayun
 * @Date 8/24/22
 */
public class OceanbaseWriteRecorder extends WriteRecorder {
    private static final String TAG = OceanbaseWriteRecorder.class.getSimpleName();
    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values(%s)";
    private static final String INSERT_IGNORE_SQL_TEMPLATE = "INSERT IGNORE INTO `%s`.`%s`(%s) values(%s)";
    private static final String INSERT_UPDATE_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values(%s) ON DUPLICATE KEY UPDATE %s";
    private static final String UPDATE_SQL_TEMPLATE = "UPDATE `%s`.`%s` SET %s WHERE %s";
    private static final String DELETE_SQL_TEMPLATE = "DELETE FROM `%s`.`%s` WHERE %s";
    private static final String CHECK_ROW_EXISTS_TEMPLATE = "SELECT COUNT(1) as count FROM `%s`.`%s` WHERE %s";
    public OceanbaseWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    @Override
    public void addInsertBatch(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            if (insertPolicy.equals(ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS)) {
                conflictIgnoreInsert(after);
            } else {
                conflictUpdateInsert(after);
            }
        } else {
            justInsert(after);
        }
        try {
            preparedStatement.addBatch();
        } catch (SQLException e) {
            TapLogger.error(TAG, "add insert batch error, sql:{}", preparedStatement.toString());
        }
    }

    //on conflict
    private void conflictUpdateInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            final String allColumnNames = allColumn.stream().map(k -> "`" + k + "`").collect(Collectors.joining(", "));
            final String placeHolder = StringKit.copyString("?", allColumn.size(), ",");
            final String allColumnNamesAndValues = allColumn.stream().map(k -> "`" + k + "`=?").collect(Collectors.joining(", "));
            final String insertSql = String.format(INSERT_UPDATE_SQL_TEMPLATE, schema, tapTable.getId(), allColumnNames, placeHolder, allColumnNamesAndValues);

            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        // fill placeHolders
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    private void conflictIgnoreInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            final String allColumnNames = allColumn.stream().map(k -> "`" + k + "`").collect(Collectors.joining(", "));
            final String placeHolder = StringKit.copyString("?", allColumn.size(), ",");
            final String insertSql = String.format(INSERT_IGNORE_SQL_TEMPLATE, schema, tapTable.getId(), allColumnNames, placeHolder);

            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    //with update
    private void withUpdateInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql;
            if (hasPk) {
                insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + allColumn.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            } else {
                insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + allColumn.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                        .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            }
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
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
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    //insert not exists
    private void notExistsInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql;
            if (hasPk) {
                insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT 1 FROM \"" + schema + "\".\"" + tapTable.getId()
                        + "\"  WHERE " + uniqueCondition.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")) + " )";
            } else {
                insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT 1 FROM \"" + schema + "\".\"" + tapTable.getId()
                        + "\"  WHERE " + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))").collect(Collectors.joining(" AND ")) + " )";
            }
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
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

    //just insert
    private void justInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {

            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new SQLException("Create insert prepared statement error, table \"" + tapTable.getId() + "\"'s fields is empty");
            }
            List<String> fields = new ArrayList<>();
            nameFieldMap.forEach((fieldName, field) -> {
                if (!needAddIntoPreparedStatementValues(field, after)) {
                    return;
                }
                fields.add("`" + fieldName + "`");
            });
            List<String> questionMarks = fields.stream().map(f -> "?").collect(Collectors.toList());
            String insertSql = String.format(INSERT_SQL_TEMPLATE, schema, tapTable.getId(), String.join(",", fields), String.join(",", questionMarks));
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    @Override
    public void addUpdateBatch(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isEmpty(after) || EmptyKit.isEmpty(uniqueCondition)) {
            return;
        }
        Map<String, Object> before = new HashMap<>();
        uniqueCondition.forEach(k -> before.put(k, after.get(k)));
        if (updatePolicy.equals(ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS)) {
            conflictUpdateInsert(after);
        } else {
            justUpdate(after, before);
        }
        preparedStatement.addBatch();
    }

    protected void justUpdate(Map<String, Object> after, Map<String, Object> before) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            final String placeHolders = after.keySet().stream().map(k -> "`" + k + "`=?").collect(Collectors.joining(", "));
            final String conditionPlaceHolders;
            final String updateSql;
            if (hasPk) {
                conditionPlaceHolders = before.keySet().stream().map(k -> "`" + k + "`=?").collect(Collectors.joining(" AND "));
            } else {
                conditionPlaceHolders = before.keySet().stream().map(k -> "(`" + k + "`=? OR (`" + k + "` IS NULL AND ? IS NULL))")
                        .collect(Collectors.joining(" AND "));
            }
            updateSql = String.format(UPDATE_SQL_TEMPLATE, schema, tapTable.getId(), placeHolders, conditionPlaceHolders);
            preparedStatement = connection.prepareStatement(updateSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : after.keySet()) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        dealNullBefore(before, pos);
    }

    private boolean needAddIntoPreparedStatementValues(TapField field, Map<String, Object> after) {
        if (null == after) {
            return false;
        }
        if (!after.containsKey(field.getName())) {
            TapLogger.warn(TAG, "Found schema field not exists in after data, will skip it: " + field.getName());
            return false;
        }
        return true;
    }

    private Collection<String> getUniqueKeys(TapTable tapTable) {
        return tapTable.primaryKeys(true);
    }

    private boolean rowExists(TapTable tapTable, Map<String, Object> before, Map<String, Object> after) throws SQLException {
        PreparedStatement checkRowExistsPreparedStatement = getCheckRowExistsPreparedStatement(tapTable);
        setPreparedStatementWhere(tapTable, before, after, checkRowExistsPreparedStatement, 1);
        AtomicBoolean result = new AtomicBoolean(false);
        try {
            ResultSet rs = checkRowExistsPreparedStatement.executeQuery();
            if (rs.next()) {
                int count = rs.getInt("count");
                result.set(count > 0);
            }
        } catch (Throwable e) {
            throw new SQLException("Check row exists failed, sql: " + checkRowExistsPreparedStatement + ", message: " + e.getMessage(), e);
        }
        return result.get();
    }

    private void setPreparedStatementWhere(TapTable tapTable, Map<String, Object> before, Map<String, Object> after, PreparedStatement preparedStatement, int parameterIndex) throws SQLException {
        if (parameterIndex <= 1) {
            parameterIndex = 1;
        }
        if (MapUtils.isEmpty(before) && MapUtils.isEmpty(after)) {
            throw new SQLException("Set prepared statement where clause failed, before and after both empty");
        }
        Map<String, Object> data;
        if (MapUtils.isNotEmpty(before)) {
            data = before;
        } else {
            data = after;
        }
        Collection<String> uniqueKeys = getUniqueKeys(tapTable);
        for (String uniqueKey : uniqueKeys) {
            if (!data.containsKey(uniqueKey)) {
                throw new SQLException("Set prepared statement where clause failed, unique key \"" + uniqueKey + "\" not exists in data: " + after);
            }
            Object value = data.get(uniqueKey);
            preparedStatement.setObject(parameterIndex++, value);
        }
    }

    private PreparedStatement getCheckRowExistsPreparedStatement(TapTable tapTable) throws SQLException {
        String tableId = tapTable.getId();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (MapUtils.isEmpty(nameFieldMap)) {
            throw new SQLException("Create check row exists prepared statement error, table \"" + tableId + "\"'s fields is empty");
        }
        List<String> whereList = new ArrayList<>();
        Collection<String> uniqueKeys = getUniqueKeys(tapTable);
        for (String uniqueKey : uniqueKeys) {
            whereList.add("`" + uniqueKey + "`<=>?");
        }
        String sql = String.format(CHECK_ROW_EXISTS_TEMPLATE, schema, tableId, String.join(" AND ", whereList));
        try {
            return connection.prepareStatement(sql);
        } catch (SQLException e) {
            throw new SQLException("Create check row exists prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
        } catch (Exception e) {
            throw new SQLException("Create check row exists prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
        }
    }

    @Override
    public void addDeleteBatch(Map<String, Object> before) throws SQLException {
        if (EmptyKit.isEmpty(before)) {
            return;
        }
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            before.keySet().removeIf(k -> !uniqueCondition.contains(k));
        }
        if (EmptyKit.isNull(preparedStatement)) {
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new SQLException("Create delete prepared statement error, table \"" + tapTable.getId() + "\"'s fields is empty");
            }
            List<String> whereList = new ArrayList<>();
            Collection<String> uniqueKeys = getUniqueKeys(tapTable);
            for (String uniqueKey : uniqueKeys) {
                whereList.add("`" + uniqueKey + "`<=>?");
            }
            String deleteSql = String.format(DELETE_SQL_TEMPLATE, schema, tapTable.getId(), String.join(" AND ", whereList));
            preparedStatement = connection.prepareStatement(deleteSql);
        }
        preparedStatement.clearParameters();
        dealNullBefore(before, 1);
        preparedStatement.addBatch();
    }
}

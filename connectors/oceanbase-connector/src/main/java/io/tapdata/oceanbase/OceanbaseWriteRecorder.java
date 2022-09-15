package io.tapdata.oceanbase;

import com.google.common.collect.Lists;
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
    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values %s";
    private static final String INSERT_IGNORE_SQL_TEMPLATE = "INSERT IGNORE INTO `%s`.`%s`(%s) values(%s)";
    private static final String INSERT_UPDATE_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values %s ON DUPLICATE KEY UPDATE %s";
    private static final String UPDATE_SQL_TEMPLATE = "UPDATE `%s`.`%s` SET %s WHERE %s";
    private static final String DELETE_SQL_TEMPLATE = "DELETE FROM `%s`.`%s` WHERE %s";
    private static final String CHECK_ROW_EXISTS_TEMPLATE = "SELECT COUNT(1) as count FROM `%s`.`%s` WHERE %s";

    private List<Map<String, Object>> insertParamMaps = Lists.newArrayList();
    public OceanbaseWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    @Override
    public void addInsertBatch(Map<String, Object> after) {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        insertParamMaps.add(after);
    }

    public int executeBatchInsert() throws SQLException {
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            if (insertPolicy.equals(ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS)) {
                conflictIgnoreInsert();
            } else {
                conflictUpdateInsert();
            }
        } else {
            justInsert();
        }
        int succeed;
        try {
            preparedStatement.execute();
            succeed = insertParamMaps.size();
            insertParamMaps.clear();
        } catch (SQLException sqle) {
            TapLogger.error("batch insert failed, sql:{}, msg:{}", preparedStatement.toString(), sqle.getMessage());
            insertParamMaps.clear();
            succeed = 0;
        }

        return succeed;
    }

    private void conflictIgnoreInsert() throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            final String allColumnNames = allColumn.stream().map(k -> "`" + k + "`").collect(Collectors.joining(", "));
            final String placeHolder = buildValuesPlaceHolderStr();
            final String insertSql = String.format(INSERT_IGNORE_SQL_TEMPLATE, schema, tapTable.getId(), allColumnNames, placeHolder);

            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        // fill placeHolders
        int pos = 1;
        for (final Map<String, Object> after : insertParamMaps) {
            for (String key : allColumn) {
                preparedStatement.setObject(pos++, after.get(key));
            }
        }
    }

    private String buildValuesPlaceHolderStr() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < insertParamMaps.size(); i++) {
            sb.append("(");
            sb.append(StringKit.copyString("?", allColumn.size(), ","));
            sb.append(")");
            if (i < insertParamMaps.size() - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }



    public void addAndCheckCommit(int succeed) {
        atomicLong.addAndGet(succeed);
    }

    //on conflict
    private void conflictUpdateInsert() throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            final String allColumnNames = allColumn.stream().map(k -> "`" + k + "`").collect(Collectors.joining(", "));
            final String placeHolder = buildValuesPlaceHolderStr();
            final String allColumnNamesAndValues = allColumn.stream().filter(k -> !uniqueCondition.contains(k)).
                    map(k -> "`" + k + "`=values(" + k + ")").collect(Collectors.joining(", "));
            final String insertSql = String.format(INSERT_UPDATE_SQL_TEMPLATE, schema, tapTable.getId(), allColumnNames, placeHolder, allColumnNamesAndValues);

            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        // fill placeHolders
        int pos = 1;
        for (Map<String, Object> after : insertParamMaps) {
            for (String key : allColumn) {
                preparedStatement.setObject(pos++, after.get(key));
            }
        }
    }

    private void justInsert() throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            final String allColumnNames = allColumn.stream().map(k -> "`" + k + "`").collect(Collectors.joining(", "));
            String questionMarks = buildValuesPlaceHolderStr();
            String insertSql = String.format(INSERT_SQL_TEMPLATE, schema, tapTable.getId(), allColumnNames, questionMarks);
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (Map<String, Object> after : insertParamMaps) {
            for (String key : allColumn) {
                preparedStatement.setObject(pos++, after.get(key));
            }
        }
    }

    public int executeUpdate(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isEmpty(after) || EmptyKit.isEmpty(uniqueCondition)) {
            return 0;
        }
        Map<String, Object> before = new HashMap<>();
        uniqueCondition.forEach(k -> before.put(k, after.get(k)));
        if (updatePolicy.equals(ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS)) {
            addInsertBatch(after);
            conflictUpdateInsert();
        } else {
            justUpdate(after, before);
        }
        int succeed = 0;
        try {
            boolean result = preparedStatement.execute();
            if (result) {
                succeed = 1;
            }
        } catch (SQLException sqle) {
            TapLogger.error(TAG, "update failed, sql:{}", preparedStatement.toString(), sqle.getMessage());
            return succeed;
        } catch (Exception e) {
            TapLogger.error(TAG, "update error, sql:{}", preparedStatement.toString(), e.getMessage());
            return succeed;
        }
        return succeed;
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

    public int executeDelete(Map<String, Object> before) throws SQLException {
        if (EmptyKit.isEmpty(before)) {
            return 0;
        }
        justDelete(before);
        int succeed = 0;
        try {
            boolean result = preparedStatement.execute();
            if (result) {
                succeed = 1;
            }
        } catch (SQLException sqle) {
            TapLogger.error(TAG, "delete failed, sql:{}", preparedStatement.toString(), sqle.getMessage());
            return succeed;
        } catch (Exception e) {
            TapLogger.error(TAG, "delete error, sql:{}", preparedStatement.toString(), e.getMessage());
            return succeed;
        }
        return succeed;
    }

    private void justDelete(Map<String, Object> before) throws SQLException {
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
    }
}

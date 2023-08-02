package io.tapdata.connector.tidb.dml;

import io.tapdata.common.WriteRecorder;
import io.tapdata.connector.tidb.ddl.TidbSqlMaker;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lemon
 */
public class TidbWriteRecorder extends WriteRecorder {

    private static final String INSERT_UPDATE_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values %s ON DUPLICATE KEY UPDATE %s";
    private static final String INSERT_IGNORE_SQL_TEMPLATE = "INSERT IGNORE INTO `%s`.`%s`(%s) values(%s)";
    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values %s";
    private final static String UPDATE_SQL = "UPDATE %s SET %s WHERE %s";

    private final Map<String, String> fieldsDataType = new HashMap<>();
    private String setClause;
    private String whereClause;

    public TidbWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
        init();
    }

    private void init() {
        if (null == tapTable) {
            return;
        }
        for (Map.Entry<String, TapField> entry : tapTable.getNameFieldMap().entrySet()) {
            String name = entry.getKey();
            TapField field = entry.getValue();
            fieldsDataType.put(name, field.getDataType());
        }
    }

    @Override
    public void addInsertBatch(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            if (insertPolicy.equals("ignore_on_exists")) {
                insertIfNotExist(after);
            } else {
                upsert(after);
            }
        } else {
            justInsert(after);
        }
        preparedStatement.addBatch();
    }


    private void insertIfNotExist(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            final String allColumnNames = allColumn.stream().map(k -> "`" + k + "`").collect(Collectors.joining(", "));
            final String placeHolder = buildValuesPlaceHolderStr();
            final String insertSql = String.format(INSERT_IGNORE_SQL_TEMPLATE, schema, tapTable.getId(), allColumnNames, placeHolder);
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        // fill placeHolders
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    private String buildValuesPlaceHolderStr() {
        return "(" + StringKit.copyString("?", allColumn.size(), ",") + ")";
    }

    private void upsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            final String allColumnNames = allColumn.stream().map(k -> "`" + k + "`").collect(Collectors.joining(", "));
            final String placeHolder = buildValuesPlaceHolderStr();
            final String allColumnNamesAndValues = allColumn.stream().
                    map(k -> "`" + k + "`=values(`" + k + "`)").collect(Collectors.joining(", "));
            final String insertSql = String.format(INSERT_UPDATE_SQL_TEMPLATE, schema, tapTable.getId(), allColumnNames, placeHolder, allColumnNamesAndValues);
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        // fill placeHolders
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    private void justInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            final String allColumnNames = allColumn.stream().map(k -> "`" + k + "`").collect(Collectors.joining(", "));
            String questionMarks = buildValuesPlaceHolderStr();
            String insertSql = String.format(INSERT_SQL_TEMPLATE, schema, tapTable.getId(), allColumnNames, questionMarks);
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    @Override
    protected void justUpdate(Map<String, Object> after, Map<String, Object> before) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            if (hasPk) {
                preparedStatement = connection.prepareStatement("UPDATE " + "`" + schema + "`" + "." + "`" + tapTable.getId() + "`" + " SET " +
                        after.keySet().stream().map(k -> "`" + k + "`=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "`" + k + "`=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("UPDATE " + "`" + schema + "`" + "." + "`" + tapTable.getId() + "`" + " SET " +
                        after.keySet().stream().map(k -> "`" + k + "`=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "(`" + k + "`=? OR (`" + k + "` IS NULL AND ? IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : after.keySet()) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        dealNullBefore(before, pos);
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
            if (hasPk) {
                preparedStatement = connection.prepareStatement("DELETE FROM" + "`" + schema + "`" + "." + "`" + tapTable.getId() + "`" + " WHERE " +
                        before.keySet().stream().map(k -> "`" + k + "`=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("DELETE FROM " + "`" + schema + "`" + "." + "`" + tapTable.getId() + "`" + "WHERE " +
                        before.keySet().stream().map(k -> "(`" + k + "`=? OR (`" + k + "` IS NULL AND ? IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        dealNullBefore(before, 1);
        preparedStatement.addBatch();
    }

//    public void addUpdateBatch(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
//        if (EmptyKit.isEmpty(after)) {
//            return;
//        }
//        if (EmptyKit.isNull(preparedStatement)) {
//            String sql = String.format(UPDATE_SQL, formatTableName(), appendSetClause(), appendWhereClause());
//            preparedStatement = connection.prepareStatement(sql);
//        }
//        preparedStatement.clearParameters();
//        int pos = 1;
//        pos = setParametersForSetClause(preparedStatement, after, pos);
//        setParametersForWhereClause(preparedStatement, getBeforeForUpdate(after, before, listResult), pos);
//        preparedStatement.addBatch();
//    }

    private int setParametersForSetClause(PreparedStatement pstmt, Map<String, Object> data, int pos) throws SQLException {
        pos = setAllObject(pstmt, data, pos);
        return pos;
    }

    private int setAllObject(PreparedStatement pstmt, Map<String, Object> data, int pos) throws SQLException {
        for (String column : allColumn) {
            Object value = data.get(column);
            try {
                pos = setObject(pstmt, pos, fieldsDataType.get(column), value);
            } catch (SQLException e) {
                throw new SQLException(String.format("Set object failed: %s | Column: %s | Value: %s", e.getMessage(), column, value), e.getSQLState(), e);
            }
        }
        return pos;
    }

    private int setObject(PreparedStatement pstmt, int pos, String fieldDataType, Object value) throws SQLException {
        if (null != fieldDataType && fieldDataType.toLowerCase().contains("binary")) {
            pstmt.setBytes(pos, (byte[]) value);
        } else {
            pstmt.setObject(pos, value);
        }
        return ++pos;
    }

    private void setParametersForWhereClause(PreparedStatement pstmt, Map<String, Object> data, int pos) throws SQLException {
        // using pk(logic pk) in where clause
        if (hasPk || !EmptyKit.isEmpty(uniqueCondition)) {
            for (String column : uniqueCondition) {
                pos = setObject(pstmt, pos, fieldsDataType.get(column), data.get(column));
            }
        } else {
            fieldsDataType.keySet().removeIf(this::shouldSkipField);
            for (String column : fieldsDataType.keySet()) {
                pos = setObject(pstmt, pos, fieldsDataType.get(column), data.get(column));
                pos = setObject(pstmt, pos, fieldsDataType.get(column), data.get(column));
            }
        }

    }

    private boolean shouldSkipField(String field) {
        // skip the "ntext", "text" and "image" in where clause since cdc table column values of these data types
        // are always be null in delete event and update before event, more detail at:
        // https://docs.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-2017
        return fieldsDataType.get(field) != null &&
                (
                        fieldsDataType.get(field).startsWith("text") ||
                                fieldsDataType.get(field).startsWith("ntext") ||
                                fieldsDataType.get(field).startsWith("image")
                );

    }

    private String appendSetClause() {
        if (setClause == null) {
            setClause = allColumn.stream().map(k -> TidbSqlMaker.formatFieldName(k) + " = ?").collect(Collectors.joining(", "));
        }
        return setClause;
    }

    private String appendWhereClause() {
        if (whereClause == null) {
            Stream<String> streaming;
            // using pk(logic pk) in where clause
            if (hasPk || !EmptyKit.isEmpty(uniqueCondition)) {
                streaming = uniqueCondition.stream().map(k -> TidbSqlMaker.formatFieldName(k) + " = ? ");
            } else {
                fieldsDataType.keySet().removeIf(this::shouldSkipField);
                streaming = fieldsDataType.keySet().stream().map(k ->
                        "(" + TidbSqlMaker.formatFieldName(k) + " = ? OR (" + TidbSqlMaker.formatFieldName(k) + " IS NULL AND ? IS NULL))"
                );
            }
            whereClause = streaming.collect(Collectors.joining(" AND "));
        }
        return whereClause;
    }

    private String formatTableName() {
        return TidbSqlMaker.formatTableName(schema, tapTable.getId());
    }
}

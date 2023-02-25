package io.tapdata.connector.tidb.dml;

import io.tapdata.common.WriteRecorder;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author lemon
 */
public class TidbWriteRecorder extends WriteRecorder {

    public TidbWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    private static final String INSERT_UPDATE_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values %s ON DUPLICATE KEY UPDATE %s";

    private static final String INSERT_IGNORE_SQL_TEMPLATE = "INSERT IGNORE INTO `%s`.`%s`(%s) values(%s)";

    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values %s";

    private final static String UPDATE_SQL = "UPDATE %s SET %s WHERE %s";
    private final static String DELETE_SQL = "DELETE FROM %s WHERE %s";

    @Override
    public void addInsertBatch(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }

        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            if (insertPolicy.equals("ignore-on-exists")) {
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
        StringBuilder sb = new StringBuilder();
            sb.append("(");
            sb.append(StringKit.copyString("?", allColumn.size(), ","));
            sb.append(")");
        return sb.toString();
    }

    private void upsert(Map<String, Object> after) throws SQLException {
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
                preparedStatement = connection.prepareStatement("UPDATE " +"`"+  schema + "`" +"." + "`"+tapTable.getId()+"`" + " SET " +
                        after.keySet().stream().map(k ->k + "=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k ->k + "=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("UPDATE " +"`"+  schema + "`" +"." + "`"+tapTable.getId()+"`" +" SET " +
                        after.keySet().stream().map(k -> k + "\"=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "("+ k + "=? OR (" + k + " IS NULL AND ? IS NULL))")
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
                preparedStatement = connection.prepareStatement("DELETE FROM" +"`"+  schema + "`" +"." + "`"+tapTable.getId()+"`" + " WHERE " +
                        before.keySet().stream().map(k -> k + "=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("DELETE FROM "+"`"+ schema + "`" +"." + "`"+tapTable.getId()+"`" + "WHERE " +
                        before.keySet().stream().map(k -> "(" + k + "=? OR (" + k + " IS NULL AND ? IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        dealNullBefore(before, 1);
        preparedStatement.addBatch();
    }

}

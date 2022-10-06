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

}

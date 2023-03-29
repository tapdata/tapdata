package io.tapdata.databend;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.databend.config.DatabendConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class DatabendJdbcContext extends JdbcContext {

    private final static String TAG = DatabendJdbcContext.class.getSimpleName();

    public static final String DATABASE_TIMEZON_SQL = "SELECT TIMEZONE();";
    private final static String Databend_ALL_TABLE = "select database,name from system.tables where database !='system' and database='%s' ";
    private final static String Databend_ALL_COLUMN = "select * from system.columns where database='%s'";
//    private final static String CK_ALL_INDEX = "";

    public DatabendJdbcContext(DatabendConfig config) {
        super(config);
    }


    @Override
    public String queryVersion() {
        AtomicReference<String> version = new AtomicReference<>("");
        try {
            queryWithNext("select VERSION();", resulSet -> version.set(resulSet.getString(1)));
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return version.get();
    }

    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) {
        TapLogger.debug(TAG, "Databend Query some tables,schema:" + getConfig().getSchema());
        List<DataMap> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(Databend_ALL_TABLE, getConfig().getDatabase()) + tableSql, resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Databend Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        return tableList;
    }

    //    @Override
    public void queryAllTables(List<String> tableNames, int batchSize, Consumer<List<String>> consumer) {

    }

    @Override
    public List<DataMap> queryAllColumns(List<String> tableNames) {

        TapLogger.debug(TAG, "Databend Query columns of some tables, schema: " + getConfig().getSchema());
        List<DataMap> columnList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(Databend_ALL_COLUMN, getConfig().getDatabase()), resultSet -> columnList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Databend Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList;
    }

    @Override
    public List<DataMap> queryAllIndexes(List<String> tableNames) {
        TapLogger.debug(TAG, "Query indexes of some tables, schema: " + getConfig().getSchema());
        List<DataMap> indexList = TapSimplify.list();
        return indexList;
    }


    public void query(String sql, ResultSetConsumer resultSetConsumer) throws SQLException {
        TapLogger.debug(TAG, "Execute query, sql: " + sql);
        try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
            statement.setFetchSize(1000); //protected from OM
            ResultSet resultSet = statement.executeQuery(sql);
            resultSetConsumer.accept(resultSet);
            resultSet.close();
        } catch (SQLException e) {
            throw new SQLException("Execute query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }

    public String timezone() throws SQLException {

        String timeZone = null;
        TapLogger.debug(TAG, "Get timezone sql: " + DATABASE_TIMEZON_SQL);
        try (Connection connection = getConnection(); Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(DATABASE_TIMEZON_SQL)) {
            while (resultSet.next()) {
                timeZone = resultSet.getString(1);
                return timeZone;
            }
        }
        return timeZone;
    }

    public static void tryCommit(Connection connection) {
        try {
            if (connection != null && connection.isValid(5) && !connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void tryRollBack(Connection connection) {
        try {
            if (connection != null && connection.isValid(5) && !connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (Throwable ignored) {
        }
    }
}


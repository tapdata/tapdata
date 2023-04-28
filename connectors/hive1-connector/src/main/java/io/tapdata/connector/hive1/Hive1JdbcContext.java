package io.tapdata.connector.hive1;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.hive1.config.Hive1Config;
import io.tapdata.connector.hive1.util.JdbcUtil;
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
import java.util.stream.Collectors;

public class Hive1JdbcContext extends JdbcContext {

    private final static String TAG = Hive1JdbcContext.class.getSimpleName();

    //todo  query time zone method not found yet
    public static final String DATABASE_TIMEZON_SQL = "SELECT timeZone()";
    private final static String HIVE_ALL_TABLE = "show tables";
    private final static String HIVE1_ALL_COLUMN = "desc `%s` ";

    private final static String CK_ALL_INDEX = "";//从异构数据源 同步到clickhouse 索引对应不上

    public Hive1JdbcContext(Hive1Config config) {
        super(config);
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection connection = getConnection();
        connection.setAutoCommit(true);
        return connection;
    }

    public static void tryRollBack(Connection connection) {
        try {
            if (connection != null && !connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (Throwable ignored) {
        }
    }

    @Override
    public String queryVersion() {
        AtomicReference<String> version = new AtomicReference<>("");
        try {
            queryWithNext("show server_version_num", resulSet -> version.set(resulSet.getString(1)));
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return version.get();
    }

    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) {
        TapLogger.debug(TAG, "Hive1 Query some tables,schema:" + getConfig().getSchema());
        List<DataMap> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(HIVE_ALL_TABLE,
                    resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Hive1 Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        if (EmptyKit.isNotEmpty(tableNames)) {
            return tableList.stream().filter(t -> tableNames.contains(t.getString("tab_name"))).collect(Collectors.toList());
        }
        return tableList;
    }

    //    @Override
    public void queryAllTables(List<String> tableNames, int batchSize, Consumer<List<String>> consumer) {

    }

    @Override
    public List<DataMap> queryAllColumns(List<String> tableNames) {

        TapLogger.debug(TAG, "CK Query columns of some tables, schema: " + getConfig().getSchema());
        List<DataMap> columnList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            //只能查询单表的表结构
            query(String.format(HIVE1_ALL_COLUMN, tableNames.get(0)),
                    resultSet -> columnList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "CK Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList;
    }


    public List<DataMap> queryColumnsOfTable(String database, String tableName) {

        TapLogger.debug(TAG, "HIVE1 Query columns of some tables, schema: " + getConfig().getSchema());
        List<DataMap> columnList = TapSimplify.list();
        try {
            //查询单表的表结构
            query(String.format(HIVE1_ALL_COLUMN, tableName),
                    resultSet -> columnList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "CK Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList;
    }

    @Override
    public List<DataMap> queryAllIndexes(List<String> tableNames) {
        TapLogger.debug(TAG, "Query indexes of some tables, schema: " + getConfig().getSchema());
        List<DataMap> indexList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(CK_ALL_INDEX, getConfig().getDatabase(), getConfig().getSchema(), tableSql),
                    resultSet -> indexList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return indexList;
    }


    public void query(String sql, ResultSetConsumer resultSetConsumer) throws SQLException {
        TapLogger.debug(TAG, "Execute query, sql: " + sql);
        try (
                Connection connection = getConnection((Hive1Config) getConfig());
                Statement statement = connection.createStatement()
        ) {
            statement.setFetchSize(1000); //protected from OM
            ResultSet resultSet = statement.executeQuery(sql);
            resultSetConsumer.accept(resultSet);
            resultSet.close();
        } catch (SQLException e) {
            TapLogger.error("query error", "error is:{}", e.getMessage(), e);
            throw new SQLException("Execute query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }


    }

    public String timezone() throws SQLException {

        String timeZone = null;
        TapLogger.debug(TAG, "Get timezone sql: " + DATABASE_TIMEZON_SQL);
        try (
                Connection connection = getConnection((Hive1Config) getConfig());
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(DATABASE_TIMEZON_SQL)
        ) {
            while (resultSet.next()) {
                timeZone = resultSet.getString(1);
                return timeZone;
            }
        }
        return timeZone;
    }

    public Connection getConnection(Hive1Config hive1Config) throws SQLException {
        return JdbcUtil.createConnection(hive1Config);
    }

    @Override
    public void execute(String sql) throws SQLException {
        TapLogger.debug(TAG, "Execute sql: " + sql);
        try (
                Connection connection = getConnection((Hive1Config) getConfig());
                Statement statement = connection.createStatement()
        ) {
            statement.execute(sql);
//            connection.commit();
        } catch (SQLException e) {
            throw new SQLException("Execute sql failed, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
        }
    }


    @Override
    public void batchExecute(List<String> sqls) throws SQLException {
        TapLogger.debug(TAG, "batchExecute sqls: " + sqls);
        try (

                Connection connection = getConnection((Hive1Config) getConfig());
                Statement statement = connection.createStatement()
        ) {
            for (String sql : sqls) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new SQLException("batchExecute sql failed, sqls: " + sqls + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
        }
    }

}

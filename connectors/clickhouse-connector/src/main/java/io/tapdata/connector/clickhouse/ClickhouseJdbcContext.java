package io.tapdata.connector.clickhouse;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.common.JdbcContext;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.clickhouse.config.ClickhouseConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import ru.yandex.clickhouse.except.ClickHouseUnknownException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class ClickhouseJdbcContext extends JdbcContext {

    private final static String TAG = ClickhouseJdbcContext.class.getSimpleName();

    public static final String DATABASE_TIMEZON_SQL = "SELECT timeZone()";
    private final static String CK_ALL_TABLE = "select database,name,comment from system.tables where database !='system' and database='%s' ";
    private final static String CK_ALL_COLUMN = "select * from system.columns where database='%s'";
    private final static String CK_ALL_INDEX = "";//从异构数据源 同步到clickhouse 索引对应不上

    private final static String CK_TABLE_INFO= "select * from system.tables where name ='%s' and database='%s' ";


    public ClickhouseJdbcContext(ClickhouseConfig config, HikariDataSource hikariDataSource) {
        super(config, hikariDataSource);
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

    @Override
    public String queryVersion() {
        AtomicReference<String> version = new AtomicReference<>("");
        try {
            queryWithNext("show server_version_num",resulSet->version.set(resulSet.getString(1)));
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return version.get();
    }

    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) {
        TapLogger.debug(TAG,"CK Query some tables,schema:"+getConfig().getSchema());
        List<DataMap> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(CK_ALL_TABLE, getConfig().getDatabase())+ tableSql,
                    resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "CK Execute queryAllTables failed, error: " + e.getMessage(), e);
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
            query(String.format(CK_ALL_COLUMN, getConfig().getDatabase(), tableSql),
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


    public void query(String sql, ResultSetConsumer resultSetConsumer) throws Throwable {
        TapLogger.debug(TAG, "Execute query, sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            statement.setFetchSize(1000); //protected from OM
            ResultSet resultSet = statement.executeQuery(sql);
            resultSetConsumer.accept(resultSet);
            resultSet.close();
        } catch (SQLException e) {
            if (e instanceof ClickHouseUnknownException && e.getMessage().contains("UNKNOWN_TABLE")) {
                resultSetConsumer.accept(null);
            } else
                throw new SQLException("Execute query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }

    public DataMap getTableInfo(String tableName) throws Throwable {
        DataMap  dataMap = DataMap.create();
        List  list  = new ArrayList();
        list.add("NUM_ROWS");
        list.add("AVG_ROW_LEN");
        try {
            query(String.format(CK_TABLE_INFO, tableName,getConfig().getDatabase()),resultSet -> {
                while (resultSet.next()) {
                    dataMap.putAll(DbKit.getRowFromResultSet(resultSet, list));
                }
            });

        }catch (Throwable e) {
            TapLogger.error(TAG, "Execute getTableInfo failed, error: " + e.getMessage(), e);
        }
        return dataMap;
    }

    public String timezone() throws SQLException {

        String timeZone = null;
        TapLogger.debug(TAG, "Get timezone sql: " + DATABASE_TIMEZON_SQL);
        try (
                Connection connection = getConnection();
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
}

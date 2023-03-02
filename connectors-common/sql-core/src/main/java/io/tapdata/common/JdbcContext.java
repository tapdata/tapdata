package io.tapdata.common;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;

import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

/**
 * abstract jdbc context
 *
 * @author Jarad
 * @date 2022/5/30
 */
public abstract class JdbcContext {

    private final static String TAG = JdbcContext.class.getSimpleName();
    private final HikariDataSource hikariDataSource;
    private boolean isFinish = false;
    private final CommonDbConfig config;
    private final List<String> connectorIds = new ArrayList<>(); //number of initialization

    public JdbcContext incrementConnector(String connectorId) {
        connectorIds.add(connectorId);
        return this;
    }

    public JdbcContext(CommonDbConfig config, HikariDataSource hikariDataSource) {
        this.config = config;
        this.hikariDataSource = hikariDataSource;
    }

    public CommonDbConfig getConfig() {
        return config;
    }

    /**
     * get sql connection
     *
     * @return Connection
     * @throws SQLException SQLException
     */
    public Connection getConnection() throws SQLException {
        final Connection connectionProxy = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class[]{Connection.class},
                new JdbcConnectionProxy(hikariDataSource.getConnection()));

        return connectionProxy;
    }

    public boolean testValid() {
        try {
            getConnection().close();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * query version of database
     *
     * @return version description
     */
    public String queryVersion() {
        AtomicReference<String> version = new AtomicReference<>("");
        try {
            queryWithNext("SELECT VERSION()", resultSet -> version.set(resultSet.getString(1)));
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return version.get();
    }

    public void queryWithNext(String sql, ResultSetConsumer resultSetConsumer) throws Throwable {
        TapLogger.debug(TAG, "Execute query, sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            statement.setFetchSize(1000); //protected from OM
            try (
                    ResultSet resultSet = statement.executeQuery(sql)
            ) {
                if (EmptyKit.isNotNull(resultSet)) {
                    resultSet.next(); //move to first row
                    resultSetConsumer.accept(resultSet);
                }
            }
        } catch (SQLException e) {
            throw new SQLException("Execute query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }

    public void query(String sql, ResultSetConsumer resultSetConsumer) throws Throwable {
        TapLogger.debug(TAG, "Execute query, sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        ) {
            statement.setFetchSize(5000); //protected from OM
            ResultSet resultSet = statement.executeQuery(sql);
            resultSetConsumer.accept(resultSet);
            resultSet.close();
        } catch (SQLException e) {
            throw new SQLException("Execute query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }

    public void query(PreparedStatement preparedStatement, ResultSetConsumer resultSetConsumer) throws Throwable {
        TapLogger.debug(TAG, "Execute query, sql: " + preparedStatement);
        try (
                ResultSet resultSet = preparedStatement.executeQuery()
        ) {
            if (EmptyKit.isNotNull(resultSet)) {
                resultSet.next();
                resultSetConsumer.accept(resultSet);
            }
        } catch (SQLException e) {
            throw new SQLException("Execute query failed, sql: " + preparedStatement + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }

    public void prepareQuery(String prepareSql, List<Object> params, ResultSetConsumer resultSetConsumer) throws Throwable {
        System.out.println("[SQL]" + prepareSql + ":[params]" + toJson(params));
        try (
                Connection connection = getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(prepareSql)
        ) {
            int pos = 1;
            for (Object obj : params) {
                preparedStatement.setObject(pos++, obj);
            }
            try (
                    ResultSet resultSet = preparedStatement.executeQuery()
            ) {
                if (EmptyKit.isNotNull(resultSet)) {
                    resultSetConsumer.accept(resultSet);
                }
            }
        } catch (SQLException e) {
            throw new SQLException("Execute query failed, sql: " + prepareSql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }

    public void execute(String sql) throws SQLException {
        TapLogger.debug(TAG, "Execute sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            statement.execute(sql);
            connection.commit();
        } catch (SQLException e) {
            throw new SQLException("Execute sql failed, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
        }
    }

    public void batchExecute(List<String> sqls) throws SQLException {
        TapLogger.debug(TAG, "batchExecute sqls: " + sqls);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            for (String sql : sqls) {
                statement.execute(sql);
            }
            connection.commit();
        } catch (SQLException e) {
            throw new SQLException("batchExecute sql failed, sqls: " + sqls + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
        }
    }

    public boolean isFinish() {
        return isFinish;
    }

    public void setFinish(boolean finish) {
        isFinish = finish;
    }

    public void finish(String connectorId) {
        connectorIds.remove(connectorId);
        if (EmptyKit.isEmpty(connectorIds)) {
            this.hikariDataSource.close();
            setFinish(true);
            DataSourcePool.removeJdbcContext(config);
        }
    }

    /**
     * query tableNames and Comments from one database and one schema
     *
     * @param tableNames some tables(all tables if tableName is empty or null)
     * @return List<TableName and Comments>
     */
    public abstract List<DataMap> queryAllTables(List<String> tableNames);

    public void queryAllTables(List<String> tableNames, int batchSize, Consumer<List<String>> consumer) {
        throw new UnsupportedOperationException();
    }

    /**
     * query all column info from some tables
     *
     * @param tableNames some tables(all tables if tableName is empty or null)
     * @return List<column info>
     */
    public abstract List<DataMap> queryAllColumns(List<String> tableNames);

    /**
     * query all index info from some tables
     *
     * @param tableNames some tables(all tables if tableName is empty or null)
     * @return List<index info>
     */
    public abstract List<DataMap> queryAllIndexes(List<String> tableNames);

    public HikariDataSource getHikariDataSource() {
        return hikariDataSource;
    }
}

package io.tapdata.connector.doris;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import lombok.Getter;
import lombok.Setter;

import java.sql.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @Author dayun
 * @Date 7/14/22
 */
@Setter
@Getter
public class DorisContext implements AutoCloseable {
    private static final String TAG = DorisContext.class.getSimpleName();
    private TapConnectionContext tapConnectionContext;
    private DorisConfig dorisConfig;
    public  Connection connection;
    public Statement statement;
    private String jdbcUrl;
    private HikariDataSource hikariDataSource;
    private static final String SELECT_COUNT = "SELECT count(*) FROM `%s`.`%s` t";
    private static final String SELECT_DORIS_VERSION = "select version() as version";

    public DorisContext(final TapConnectionContext tapConnectionContext) {
        this.tapConnectionContext = tapConnectionContext;
        DataMap config = tapConnectionContext.getConnectionConfig();
        try {
            if (connection == null) {
                if (dorisConfig == null) {
                    dorisConfig = new DorisConfig().load(config);
                }
                jdbcUrl= dorisConfig.getDatabaseUrl();
                Class.forName(dorisConfig.getJdbcDriver());
                connection =getConnection();
            }
            if (statement == null) {
                statement = connection.createStatement();
            }
        } catch (Exception e) {
            throw new RuntimeException("Create Connection Failed!");
        }
    }


    public Connection getConnection() throws SQLException, IllegalArgumentException {
        try {
            Connection connection = DriverManager.getConnection(jdbcUrl, dorisConfig.getUser(), dorisConfig.getPassword());
            this.connection=connection;
        } catch (Throwable ignored) {
        }
        return connection;
    }

    public void query(String sql, ResultSetConsumer resultSetConsumer) throws Throwable {
        TapLogger.debug(TAG, "Execute query, sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            statement.setFetchSize(1000);
            if (null != resultSet) {
                resultSetConsumer.accept(resultSet);
            }
        } catch (SQLException e) {
            throw new Exception("Execute query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }
    public ResultSet executeQuery(final Statement statement, final String sql) throws Exception {
        TapLogger.debug(TAG, "Execute sql: " + sql);
        try {
            return statement.executeQuery(sql);
        } catch (SQLException e) {
            throw new SQLException("Execute sql failed, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
        }
    }

    public void execute(String sql) throws SQLException {
        TapLogger.debug(TAG, "Execute sql: " + sql);
        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new SQLException("Execute sql failed, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
        }
    }

    public int count(String tableName) throws Throwable {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        try (
                ResultSet rs = statement.executeQuery(String.format(SELECT_COUNT, database, tableName))
        ) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }

    public TapConnectionContext getTapConnectionContext() {
        return tapConnectionContext;
    }

    public WriteFormat getWriteFormat() {
        if (null == tapConnectionContext) return WriteFormat.json;
        DataMap nodeConfig = tapConnectionContext.getNodeConfig();
        if (null == nodeConfig) return WriteFormat.json;
        String writeFormat = nodeConfig.getString("writeFormat");
        if (null == writeFormat || "".equals(writeFormat.trim())) return WriteFormat.json;
        try {
            return WriteFormat.valueOf(writeFormat);
        } catch (IllegalArgumentException e) {
            return WriteFormat.json;
        }
    }

    @Override
    public void close() throws Exception {
        if (!connection.isClosed()) {
            if (!statement.isClosed()) {
                statement.close();
            }
            connection.close();
        }
    }

    public enum WriteFormat {
        json,
        csv,
    }
}

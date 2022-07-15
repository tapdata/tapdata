package io.tapdata.connector.doris;

import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
    private Connection connection;
    private Statement statement;

    public DorisContext(final TapConnectionContext tapConnectionContext) {
        this.tapConnectionContext = tapConnectionContext;
        DataMap config = tapConnectionContext.getConnectionConfig();
        try {
            if (connection == null) {
                if (dorisConfig == null) {
                    dorisConfig = new DorisConfig().load(config);
                }
                String dbUrl = dorisConfig.getDatabaseUrl();
                Class.forName(dorisConfig.getJdbcDriver());
                connection = DriverManager.getConnection(dbUrl, dorisConfig.getUser(), dorisConfig.getPassword());
            }
            if (statement == null) {
                statement = connection.createStatement();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Create Connection Failed!");
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

    @Override
    public void close() throws Exception {
        if (!connection.isClosed()) {
            if (!statement.isClosed()) {
                statement.close();
            }
            connection.close();
        }
    }
}

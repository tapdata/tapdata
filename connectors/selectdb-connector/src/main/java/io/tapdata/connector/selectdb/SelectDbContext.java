package io.tapdata.connector.selectdb;

import io.tapdata.connector.selectdb.config.SelectDbConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import lombok.Getter;
import lombok.Setter;

import java.sql.*;

/**
 * Author:Skeet
 * Date: 2022/12/15
 **/

@Setter
@Getter
public class SelectDbContext implements AutoCloseable {
    private static final String TAG = SelectDbContext.class.getSimpleName();
    private TapConnectionContext tapConnectionContext;
    private SelectDbConfig selectDbConfig;
    private Connection connection;
    private Statement statement;
    public TapConnectionContext tapConnectionContext(){
        return tapConnectionContext;
    }

    public SelectDbContext(final TapConnectionContext tapConnectionContext) {
        this.tapConnectionContext = tapConnectionContext;
        DataMap config = tapConnectionContext.getConnectionConfig();
        try {
            if (connection == null) {
                if (selectDbConfig == null) {
                    selectDbConfig = new SelectDbConfig().load(config);
                }
                String dbUrl = selectDbConfig.getDatabaseUrl();
                Class.forName(selectDbConfig.getJdbcDriver());
                connection = DriverManager.getConnection(dbUrl, selectDbConfig.getUser(), selectDbConfig.getPassword());
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
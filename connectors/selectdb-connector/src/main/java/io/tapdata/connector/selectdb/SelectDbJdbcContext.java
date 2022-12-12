package io.tapdata.connector.selectdb;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.connector.selectdb.util.JdbcUtil;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Author:Skeet
 * Date: 2022/12/8 16:24
 **/
public class SelectDbJdbcContext implements AutoCloseable {
    private static final String TAG = SelectDbJdbcContext.class.getSimpleName();

    private static final String SELECT_SQL_MODE = "select @@sql_mode";
    private static final String SET_CLIENT_SQL_MODE = "set sql_mode = ?";
    public static final String DATABASE_TIMEZON_SQL = "SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP()) as timezone";
    private TapConnectionContext tapConnectionContext;
    private String jdbcUrl;
    private HikariDataSource hikariDataSource;

    private static final List<String> ignoreSqlModes = new ArrayList<String>() {{
        add("NO_ZERO_DATE");
    }};

    public SelectDbJdbcContext(TapConnectionContext tapConnectionContext) {
        this.tapConnectionContext = tapConnectionContext;
        this.jdbcUrl = jdbcUrl();
        this.hikariDataSource = HikariConnection.getHikariDataSource(tapConnectionContext, jdbcUrl);
    }

    public Connection getConnection() throws SQLException, IllegalArgumentException {
        Connection connection = this.hikariDataSource.getConnection();
        try {
            setIgnoreSqlMode(connection);
        } catch (Throwable ignored) {
        }
        return connection;
    }

    private String jdbcUrl() {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String type = tapConnectionContext.getSpecification().getId();
        String host = String.valueOf(connectionConfig.get("host"));
        int port = ((Number) connectionConfig.get("port")).intValue();//todu 不为null
        String databaseName = String.valueOf(connectionConfig.get("database"));

        String userName = String.valueOf(connectionConfig.get("user"));
        String password = String.valueOf(connectionConfig.get("password"));


        StringBuilder sbURL = new StringBuilder("jdbc:").append("mysql").append("://").append(host).append(":").append(port).append("/")
                .append(databaseName).append("?user=").append(userName).append("&password=").append(password);

        return sbURL.toString();
    }

    private void setIgnoreSqlMode(Connection connection) throws Throwable {
        if (connection == null) {
            return;
        }
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(SELECT_SQL_MODE)) {
            if (resultSet.next()) {
                String sqlMode = resultSet.getString(1);
                if (StringUtils.isBlank(sqlMode)) {
                    return;
                }
                for (String ignoreSqlMode : ignoreSqlModes) {
                    sqlMode = sqlMode.replace("," + ignoreSqlMode, "");
                    sqlMode = sqlMode.replace(ignoreSqlMode + ",", "");
                }

                try (PreparedStatement preparedStatement = connection.prepareStatement(SET_CLIENT_SQL_MODE)) {
                    preparedStatement.setString(1, sqlMode);
                    preparedStatement.execute();
                }
            }
        }
    }
            //TUDO Hikarpool  **********************
    static class HikariConnection {
        public static HikariDataSource getHikariDataSource(TapConnectionContext tapConnectionContext, String jdbcUrl) throws IllegalArgumentException {
            HikariDataSource hikariDataSource;
            if (null == tapConnectionContext) {
                throw new IllegalArgumentException("TapConnectionContext cannot be null");
            }
            hikariDataSource = new HikariDataSource();
            DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
            hikariDataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
            String username = connectionConfig.getString("username");
            String password = connectionConfig.getString("password");
            hikariDataSource.setJdbcUrl(jdbcUrl);
            hikariDataSource.setUsername(username);
            hikariDataSource.setPassword(password);
            hikariDataSource.setMinimumIdle(1);
            hikariDataSource.setMaximumPoolSize(100);
            hikariDataSource.setAutoCommit(false);
            hikariDataSource.setIdleTimeout(60 * 1000L);
            return hikariDataSource;
        }
    }

    @Override
    public void close() throws Exception {
        JdbcUtil.closeQuietly(hikariDataSource);
    }

    public TapConnectionContext getTapConnectionContext() {
        return tapConnectionContext;
    }
}
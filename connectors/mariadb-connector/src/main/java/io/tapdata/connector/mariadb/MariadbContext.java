package io.tapdata.connector.mariadb;

import com.mysql.cj.jdbc.StatementImpl;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariProxyStatement;
import io.tapdata.connector.mariadb.entity.MariadbBinlogPosition;
import io.tapdata.connector.mariadb.util.JdbcUtil;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MariadbContext implements AutoCloseable {

    private static final String TAG = MariadbContext.class.getSimpleName();
    public static final String DATABASE_TIMEZON_SQL = "SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP()) as timezone";
    private TapConnectionContext tapConnectionContext;
    private String jdbcUrl;
    private HikariDataSource hikariDataSource;
    private static final String SELECT_SQL_MODE = "select @@sql_mode";
    private static final String SET_CLIENT_SQL_MODE = "set sql_mode = ?";
    private static final String SELECT_MYSQL_VERSION = "select version() as version";
    public static final String SELECT_TABLE = "SELECT t.* FROM `%s`.`%s` t";
    private static final String SELECT_COUNT = "SELECT count(*) FROM `%s`.`%s` t";
    private static final String CHECK_TABLE_EXISTS_SQL = "SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'";
    private static final String DROP_TABLE_IF_EXISTS_SQL = "DROP TABLE IF EXISTS `%s`.`%s`";
    private static final String TRUNCATE_TABLE_SQL = "TRUNCATE TABLE `%s`.`%s`";

    private static final Map<String, String> DEFAULT_PROPERTIES = new HashMap<String, String>() {{
        put("rewriteBatchedStatements", "true");
        put("useCursorFetch", "true");
        put("useSSL", "false");
        put("zeroDateTimeBehavior", "convertToNull");
        put("allowPublicKeyRetrieval", "true");
        put("useTimezone", "false");
        // mysql的布尔类型，实际存储是tinyint(1)，该参数控制mysql客户端接收tinyint(1)的数据类型，默认true，接收为布尔类型，false则为数字:1,0
        put("tinyInt1isBit", "false");
    }};

    private static final List<String> ignoreSqlModes = new ArrayList<String>() {{
        add("NO_ZERO_DATE");
    }};

    public MariadbContext(TapConnectionContext tapConnectionContext) {
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

    public static void tryCommit(Connection connection) throws SQLException {
        try {
            if (connection != null && !connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (Throwable ignored) {
            String err = "CommitSql error: " + ignored.getMessage();
            TapLogger.warn(TAG, err);
            throw new SQLException(err, ignored);
        }
    }

    public static void tryRollBack(Connection connection) throws SQLException {
        try {
            if (connection != null && !connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (Throwable ignored) {
            String err = "RollBackSql error: " + ignored.getMessage();
            TapLogger.warn(TAG, err);
            throw new SQLException(err, ignored);
        }
    }

    /**
     * 组装连接JDBC连接串
     * @return
     */
    private String jdbcUrl() {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        // mariadb 按照mysql的驱动以及连接串连接
        String type ="mysql";
        String host = String.valueOf(connectionConfig.get("host"));
        int port = ((Number) connectionConfig.get("port")).intValue();
        String databaseName = String.valueOf(connectionConfig.get("database"));

        String additionalString = String.valueOf(connectionConfig.get("addtionalString"));
        additionalString = null == additionalString ? "" : additionalString.trim();
        if (additionalString.startsWith("?")) {
            additionalString = additionalString.substring(1);
        }

        Map<String, String> properties = new HashMap<>();
        StringBuilder sbURL = new StringBuilder("jdbc:").append(type).append("://").append(host).append(":").append(port).append("/").append(databaseName);

        if (StringUtils.isNotBlank(additionalString)) {
            String[] additionalStringSplit = additionalString.split("&");
            for (String s : additionalStringSplit) {
                String[] split = s.split("=");
                if (split.length == 2) {
                    properties.put(split[0], split[1]);
                }
            }
        }
        for (String defaultKey : DEFAULT_PROPERTIES.keySet()) {
            if (properties.containsKey(defaultKey)) {
                continue;
            }
            properties.put(defaultKey, DEFAULT_PROPERTIES.get(defaultKey));
        }
        String timezone = connectionConfig.getString("timezone");
        if (StringUtils.isNotBlank(timezone)) {
            try {
                ZoneId.of(timezone);
                timezone = "GMT" + timezone;
                String serverTimezone = timezone.replace("+", "%2B").replace(":00", "");
                properties.put("serverTimezone", serverTimezone);
            } catch (Exception ignored) {
            }
        }
        StringBuilder propertiesString = new StringBuilder();
        properties.forEach((k, v) -> propertiesString.append("&").append(k).append("=").append(v));

        if (propertiesString.length() > 0) {
            additionalString = StringUtils.removeStart(propertiesString.toString(), "&");
            sbURL.append("?").append(additionalString);
        }

        return sbURL.toString();
    }

    /**
     * 设置mariadb忽略语法校验规则
     * @param connection
     * @throws Throwable
     */
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
                // 允许0值的日期插入不报错
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

    /**
     * 获取mariadb版本号
     * @throws Throwable
     */
    public String getMariadbVersion() throws Throwable {
        AtomicReference<String> version = new AtomicReference<>();
        query(SELECT_MYSQL_VERSION, resultSet -> {
            if (resultSet.next()) {
                version.set(resultSet.getString("version"));
            }
        });
        return version.get();
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

    public void query(PreparedStatement preparedStatement, ResultSetConsumer resultSetConsumer) throws Throwable {
        TapLogger.debug(TAG, "Execute query, sql: " + preparedStatement);
        preparedStatement.setFetchSize(1000);
        try (
                ResultSet resultSet = preparedStatement.executeQuery()
        ) {
            if (null != resultSet) {
                resultSetConsumer.accept(resultSet);
            }
        } catch (SQLException e) {
            throw new Exception("Execute query failed, sql: " + preparedStatement + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }

    public void queryWithStream(String sql, ResultSetConsumer resultSetConsumer) throws Throwable {
        TapLogger.debug(TAG, "Execute query with stream, sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            if (statement instanceof HikariProxyStatement) {
                StatementImpl statementImpl = statement.unwrap(StatementImpl.class);
                if (null != statementImpl) {
                    statementImpl.enableStreamingResults();
                }
            }
            statement.setFetchSize(1000);
            try (
                    ResultSet resultSet = statement.executeQuery(sql)
            ) {
                if (null != resultSet) {
                    resultSetConsumer.accept(resultSet);
                }
            }
        } catch (SQLException e) {
            throw new Exception("Execute steaming query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }

    public void execute(String sql) throws Throwable {
        TapLogger.debug(TAG, "Execute sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new Exception("Execute sql failed, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
        }
    }

    /**
     * 获取表中数据总数
     * @throws Throwable
     */
    public int count(String tableName) throws Throwable {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        AtomicInteger count = new AtomicInteger(0);
        query(String.format(SELECT_COUNT, database, tableName), rs -> {
            if (rs.next()) {
                count.set(rs.getInt(1));
            }
        });
        return count.get();
    }

    /**
     * 查询表是否存在
     * @throws Throwable
     */
    public boolean tableExists(String tableName) throws Throwable {
        AtomicBoolean exists = new AtomicBoolean();
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        query(String.format(CHECK_TABLE_EXISTS_SQL, database, tableName), rs -> {
            exists.set(rs.next());
        });
        return exists.get();
    }

    /**
     * 删除表
     * @throws Throwable
     */
    public void dropTable(String tableName) throws Throwable {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        String sql = String.format(DROP_TABLE_IF_EXISTS_SQL, database, tableName);
        execute(sql);
    }

    /**
     * 清空表数据
     * @throws Throwable
     */
    public void clearTable(String tableName) throws Throwable {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        String sql = String.format(TRUNCATE_TABLE_SQL, database, tableName);
        execute(sql);
    }

    /**
     * 获取数据库当前binlog位置
     * @return
     * @throws Throwable
     */
    public MariadbBinlogPosition readBinlogPosition() throws Throwable {
        AtomicReference<MariadbBinlogPosition> mysqlBinlogPositionAtomicReference = new AtomicReference<>();
        query("SHOW MASTER STATUS", rs -> {
            if (rs.next()) {
                String binlogFilename = rs.getString(1);
                long binlogPosition = rs.getLong(2);
                mysqlBinlogPositionAtomicReference.set(new MariadbBinlogPosition(binlogFilename, binlogPosition));
                if (rs.getMetaData().getColumnCount() > 4) {
                    // This column exists only in MySQL 5.6.5 or later ...
                    String gtidSet = rs.getString(5); // GTID set, may be null, blank, or contain a GTID set
                    mysqlBinlogPositionAtomicReference.get().setGtidSet(gtidSet);
                }
            }
        });
        return mysqlBinlogPositionAtomicReference.get();
    }

    /**
     * 获取mariadb serverID值
     * @return
     * @throws Throwable
     */
    public String getServerId() throws Throwable {
        AtomicReference<String> serverId = new AtomicReference<>();
        query("SHOW VARIABLES LIKE 'SERVER_ID'", rs -> {
            if (rs.next()) {
                serverId.set(rs.getString("Value"));
            }
        });
        return serverId.get();
    }

    /**
     * 获取数据库时区
     * @throws Exception
     */
    public String timezone() throws Exception {

        String formatTimezone = null;
        TapLogger.debug(TAG, "Get timezone sql: " + DATABASE_TIMEZON_SQL);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(DATABASE_TIMEZON_SQL)
        ) {
            while (resultSet.next()) {
                String timezone = resultSet.getString(1);
                formatTimezone = formatTimezone(timezone);
            }
        }
        return formatTimezone;
    }

    /**
     * 格式化时区
     * @return
     * e.g. 08:00:00 返回GMT+08
     */
    private static String formatTimezone(String timezone) {
        StringBuilder sb = new StringBuilder("GMT");
        String[] split = timezone.split(":");
        String str = split[0];
        if (str.contains("-")) {
            if (str.length() == 3) {
                sb.append(str);
            } else {
                sb.append("-0").append(StringUtils.right(str, 1));
            }
        } else if (str.contains("+")) {
            if (str.length() == 3) {
                sb.append(str);
            } else {
                sb.append("+0").append(StringUtils.right(str, 1));
            }
        } else {
            sb.append("+");
            if (str.length() == 2) {
                sb.append(str);
            } else {
                sb.append("0").append(StringUtils.right(str, 1));
            }
        }
        return sb.toString();
    }

    @Override
    public void close() throws Exception {
        JdbcUtil.closeQuietly(hikariDataSource);
    }

    public TapConnectionContext getTapConnectionContext() {
        return tapConnectionContext;
    }

    /**
     * 创建Hikari连接池
     */
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
            hikariDataSource.setMaximumPoolSize(20);
            hikariDataSource.setAutoCommit(false);
            hikariDataSource.setIdleTimeout(60 * 1000L);
            hikariDataSource.setKeepaliveTime(60 * 1000L);
            return hikariDataSource;
        }
    }

}



package io.tapdata.connector.mysql;

import com.mysql.cj.jdbc.StatementImpl;
import com.zaxxer.hikari.pool.HikariProxyStatement;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.JdbcContext;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.text.DecimalFormat;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;

public class MysqlJdbcContextV2 extends JdbcContext {

    private static final String TAG = MysqlJdbcContextV2.class.getSimpleName();

    private static final String SELECT_SQL_MODE = "select @@sql_mode";
    private static final String SET_CLIENT_SQL_MODE = "set sql_mode = ?";
    private static final List<String> ignoreSqlModes = new ArrayList<String>() {{
        add("NO_ZERO_DATE");
    }};

    public MysqlJdbcContextV2(CommonDbConfig config) {
        super(config);
        exceptionCollector = new MysqlExceptionCollector();
    }

    @Override
    public String queryVersion() throws SQLException {
        AtomicReference<String> version = new AtomicReference<>();
        queryWithNext(MYSQL_VERSION, resultSet -> version.set(resultSet.getString(1)));
        return version.get();
    }

    public TimeZone queryTimeZone() throws SQLException {
        AtomicReference<Long> timeOffset = new AtomicReference<>();
        queryWithNext(MYSQL_TIMEZONE, resultSet -> timeOffset.set(resultSet.getLong(1)));
        DecimalFormat decimalFormat = new DecimalFormat("00");
        if (timeOffset.get() >= 0) {
            return TimeZone.getTimeZone(ZoneId.of("+" + decimalFormat.format(timeOffset.get()) + ":00"));
        } else {
            return TimeZone.getTimeZone(ZoneId.of(decimalFormat.format(timeOffset.get()) + ":00"));
        }
    }

    @Override
    protected String queryAllTablesSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND TABLE_NAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        return String.format(MYSQL_ALL_TABLE, schema, tableSql);
    }

    @Override
    protected String queryAllColumnsSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND TABLE_NAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        return String.format(MYSQL_ALL_COLUMN, schema, tableSql);
    }

    @Override
    protected String queryAllIndexesSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND TABLE_NAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        return String.format(MYSQL_ALL_INDEX, schema, tableSql);
    }

    public DataMap getTableInfo(String tableName) {
        DataMap dataMap = DataMap.create();
        List<String> list = new ArrayList<>();
        list.add("TABLE_ROWS");
        list.add("DATA_LENGTH");
        try {
            query(String.format(GET_TABLE_INFO_SQL, getConfig().getDatabase(), tableName), resultSet -> {
                while (resultSet.next()) {
                    dataMap.putAll(DbKit.getRowFromResultSet(resultSet, list));
                }
            });

        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute getTableInfo failed, error: " + e.getMessage(), e);
        }
        return dataMap;
    }

    public Connection getConnection() throws SQLException, IllegalArgumentException {
        Connection connection = super.getConnection();
        try {
            setIgnoreSqlMode(connection);
        } catch (Throwable ignored) {
        }
        return connection;
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

    public MysqlBinlogPosition readBinlogPosition() throws Throwable {
        AtomicReference<MysqlBinlogPosition> mysqlBinlogPositionAtomicReference = new AtomicReference<>();
        normalQuery("SHOW MASTER STATUS", rs -> {
            if (rs.next()) {
                String binlogFilename = rs.getString(1);
                long binlogPosition = rs.getLong(2);
                mysqlBinlogPositionAtomicReference.set(new MysqlBinlogPosition(binlogFilename, binlogPosition));
                if (rs.getMetaData().getColumnCount() > 4) {
                    // This column exists only in MySQL 5.6.5 or later ...
                    String gtidSet = rs.getString(5); // GTID set, may be null, blank, or contain a GTID set
                    mysqlBinlogPositionAtomicReference.get().setGtidSet(gtidSet);
                }
            }
        });
        return mysqlBinlogPositionAtomicReference.get();
    }

    public String getServerId() throws Throwable {
        AtomicReference<String> serverId = new AtomicReference<>();
        normalQuery("SHOW VARIABLES LIKE 'SERVER_ID'", rs -> {
            if (rs.next()) {
                serverId.set(rs.getString("Value"));
            }
        });
        return serverId.get();
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

    private static final String MYSQL_ALL_TABLE =
            "SELECT\n" +
                    "\tTABLE_NAME `tableName`,\n" +
                    "\tTABLE_COMMENT `tableComment`\n" +
                    "FROM\n" +
                    "\tINFORMATION_SCHEMA.TABLES\n" +
                    "WHERE\n" +
                    "\tTABLE_SCHEMA = '%s' %s\n" +
                    "\tAND TABLE_TYPE = 'BASE TABLE'";

    private static final String MYSQL_ALL_COLUMN =
            "SELECT TABLE_NAME `tableName`,\n" +
                    "       COLUMN_NAME `columnName`,\n" +
                    "       COLUMN_TYPE `dataType`,\n" +
                    "       IS_NULLABLE `nullable`,\n" +
                    "       COLUMN_COMMENT `columnComment`\n" +
                    "FROM INFORMATION_SCHEMA.COLUMNS\n" +
                    "WHERE TABLE_SCHEMA = '%s' %s\n" +
                    "ORDER BY ORDINAL_POSITION";

    private final static String MYSQL_ALL_INDEX =
            "SELECT\n" +
                    "\tTABLE_NAME `tableName`,\n" +
                    "\tINDEX_NAME `indexName`,\n" +
                    "\t(CASE\n" +
                    "\t\tWHEN COLLATION = 'A' THEN 1\n" +
                    "\t\tELSE 0\n" +
                    "\tEND) `isAsc`,\n" +
                    "\t(CASE\n" +
                    "\t\tWHEN NON_UNIQUE = 0 THEN 1\n" +
                    "\t\tELSE 0\n" +
                    "\tEND) `isUnique`,\n" +
                    "\t(CASE\n" +
                    "\t\tWHEN INDEX_NAME = 'PRIMARY' THEN 1\n" +
                    "\t\tELSE 0\n" +
                    "\tEND) `isPk`,\n" +
                    "\tCOLUMN_NAME `columnName`\n" +
                    "FROM\n" +
                    "\tINFORMATION_SCHEMA.STATISTICS\n" +
                    "WHERE\n" +
                    "\tTABLE_SCHEMA = '%s' %s\n" +
                    "ORDER BY\n" +
                    "\tINDEX_NAME,\n" +
                    "\tSEQ_IN_INDEX";

    private final static String MYSQL_VERSION = "SELECT VERSION()";

    private final static String MYSQL_TIMEZONE = "SELECT TIMESTAMPDIFF(HOUR, UTC_TIMESTAMP(), NOW()) as timeoffset";

    private static final String GET_TABLE_INFO_SQL = "SELECT TABLE_ROWS,DATA_LENGTH  FROM information_schema.tables WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'";

}

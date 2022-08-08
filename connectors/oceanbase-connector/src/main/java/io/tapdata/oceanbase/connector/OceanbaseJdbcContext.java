package io.tapdata.oceanbase.connector;

import com.google.common.collect.Lists;
import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.oceanbase.OceanbaseMaker;
import io.tapdata.oceanbase.util.ConnectionUtil;
import io.tapdata.oceanbase.util.JdbcUtil;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapFilter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * @author dayun
 * @date 2022/6/23 16:26
 */
public class OceanbaseJdbcContext implements AutoCloseable {
    private static final String TAG = OceanbaseJdbcContext.class.getSimpleName();
    public static final String DATABASE_TIMEZON_SQL = "SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP()) as timezone";
    private TapConnectionContext tapConnectionContext;
    private String jdbcUrl;
    private HikariDataSource hikariDataSource;
    private static final String SELECT_SQL_MODE = "select @@sql_mode";
    private static final String SET_CLIENT_SQL_MODE = "set sql_mode = ?";
    private static final String SELECT_TABLE = "SELECT t.* FROM `%s`.`%s` t";
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

    public OceanbaseJdbcContext(TapConnectionContext tapConnectionContext) {
        this.tapConnectionContext = tapConnectionContext;
        this.jdbcUrl = jdbcUrl();
        this.hikariDataSource = ConnectionUtil.getHikariDataSource(tapConnectionContext, jdbcUrl);
    }

    public Connection getConnection() throws SQLException, IllegalArgumentException {
        Connection connection = this.hikariDataSource.getConnection();
        try {
            setIgnoreSqlMode(connection);
        } catch (Throwable ignored) {
        }
        return connection;
    }

    public static void tryCommit(Connection connection) {
        try {
            if (connection != null && connection.isValid(5) && !connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (Throwable ignored) {
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

    private String jdbcUrl() {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String host = String.valueOf(connectionConfig.get("host"));
        int port = ((Number) connectionConfig.get("port")).intValue();
        String databaseName = String.valueOf(connectionConfig.get("database"));

        String additionalString = String.valueOf(connectionConfig.get("addtionalString"));
        additionalString = null == additionalString ? "" : additionalString.trim();
        if (additionalString.startsWith("?")) {
            additionalString = additionalString.substring(1);
        }

        Map<String, String> properties = new HashMap<>();
        StringBuilder sbURL = new StringBuilder("jdbc:").append("mysql").append("://").append(host).append(":").append(port).append("/").append(databaseName);

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

    public List<DataMap> query(String sql, final Set<String> fieldNames) throws Throwable {
        TapLogger.debug(TAG, "Execute query, sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            statement.setFetchSize(1000);
            final List<DataMap> dataMapList = Lists.newLinkedList();
            if (null != resultSet) {
                while (resultSet.next()) {
                    DataMap rowData = DataMap.create();
                    for (String columnName : fieldNames) {
                        rowData.put(columnName, resultSet.getObject(columnName));
                    }
                    dataMapList.add(rowData);
                }
                return dataMapList;
            }
        } catch (SQLException e) {
            throw new Exception("Execute query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
        return null;
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

    public boolean tableExists(String tableName) throws Throwable {
        AtomicBoolean exists = new AtomicBoolean();
        String database = getDatabase();
        query(String.format(CHECK_TABLE_EXISTS_SQL, database, tableName), rs -> {
            exists.set(rs.next());
        });
        return exists.get();
    }

    public void dropTable(String tableName) throws Throwable {
        String database = getDatabase();
        String sql = String.format(DROP_TABLE_IF_EXISTS_SQL, database, tableName);
        execute(sql);
    }

    public void clearTable(String tableName) throws Throwable {
        String database = getDatabase();
        String sql = String.format(TRUNCATE_TABLE_SQL, database, tableName);
        execute(sql);
    }

    public void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer) {
        Set<String> columnNames = tapTable.getNameFieldMap().keySet();
        List<FilterResult> filterResults = new LinkedList<>();
        TapAdvanceFilter tapAdvanceFilter = convert2TapAdvanceFilter(filters);

        final String sql = OceanbaseMaker.selectSql(connectorContext, tapTable, tapAdvanceFilter);

        try {
            final List<DataMap> dataMapList = query(sql, columnNames);
            if (CollectionUtils.isEmpty(dataMapList)) {
                final FilterResult emptyResult = new FilterResult();
                filterResults.add(emptyResult);
            } else {
                for (DataMap dataMap : dataMapList) {
                    final FilterResult filterResult = new FilterResult();
                    filterResult.setResult(dataMap);
                    filterResults.add(filterResult);
                }
            }
        } catch (Throwable e) {
            final FilterResult errorResult = new FilterResult();
            errorResult.setError(e);
            filterResults.add(errorResult);
        }
        listConsumer.accept(filterResults);
    }

    private TapAdvanceFilter convert2TapAdvanceFilter(final List<TapFilter> filters) {
        TapAdvanceFilter advanceFilter = new TapAdvanceFilter();
        DataMap dataMap = new DataMap();
        for (final TapFilter filter : filters) {
            dataMap.putAll(filter.getMatch());
        }
        advanceFilter.setMatch(dataMap);
        return advanceFilter;
    }

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

    public String getDatabase() {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        return connectionConfig.getString("database");
    }

    @Override
    public void close() {
        JdbcUtil.closeQuietly(hikariDataSource);
    }

    public TapConnectionContext getTapConnectionContext() {
        return tapConnectionContext;
    }
}

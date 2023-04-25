package io.tapdata.connector.tencent.db.mysql;

import com.mysql.cj.jdbc.StatementImpl;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariProxyStatement;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.connector.mysql.util.JdbcUtil;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author samuel
 * @Description
 * @create 2022-04-28 16:24
 **/
public class MysqlJdbcContext implements AutoCloseable {

	private static final String TAG = MysqlJdbcContext.class.getSimpleName();
	public static final String DATABASE_TIMEZON_SQL = "SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP()) as timezone";
	private TapConnectionContext tapConnectionContext;
	private String jdbcUrl;
	private HikariDataSource hikariDataSource;
	private static final String SELECT_SQL_MODE = "select @@sql_mode";
	private static final String SET_CLIENT_SQL_MODE = "set sql_mode = ?";
	private static final String SELECT_MYSQL_VERSION = "select version() as version";
	public static final String SELECT_TABLE = "SELECT t.* FROM `%s`.`%s` t";
	public static final String SELECT_SOME_FROM_TABLE = "SELECT `%s` FROM `%s`.`%s` ";
	private static final String SELECT_COUNT = "SELECT count(*) FROM `%s`.`%s` t";
	private static final String CHECK_TABLE_EXISTS_SQL = "SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'";
	private static final String DROP_TABLE_IF_EXISTS_SQL = "DROP TABLE IF EXISTS `%s`.`%s`";
	private static final String TRUNCATE_TABLE_SQL = "TRUNCATE TABLE `%s`.`%s`";

	public static final String FIELD_TEMPLATE = "`%s`";

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

	public MysqlJdbcContext(TapConnectionContext tapConnectionContext) {
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
		String type = tapConnectionContext.getSpecification().getId();
		String host = String.valueOf(connectionConfig.get("host"));
		int port = ((Number) connectionConfig.get("port")).intValue();
		String databaseName = String.valueOf(connectionConfig.get("database"));

		String additionalString = String.valueOf(connectionConfig.get("addtionalString"));
		additionalString = null == additionalString ? "" : additionalString.trim();
		if (additionalString.startsWith("?")) {
			additionalString = additionalString.substring(1);
		}

		String protocolType = connectionConfig.get("protocolType") == null ? null : String.valueOf(connectionConfig.get("protocolType"));
		if (EmptyKit.isEmpty(protocolType)) {
			protocolType = type;
		}
		Map<String, String> properties = new HashMap<>();
		StringBuilder sbURL = new StringBuilder("jdbc:").append(protocolType).append("://").append(host).append(":").append(port).append("/").append(databaseName);

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

	public String getMysqlVersion() throws Throwable {
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

	public boolean tableExists(String tableName) throws Throwable {
		AtomicBoolean exists = new AtomicBoolean();
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		query(String.format(CHECK_TABLE_EXISTS_SQL, database, tableName), rs -> {
			exists.set(rs.next());
		});
		return exists.get();
	}

	public void dropTable(String tableName) throws Throwable {
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		String sql = String.format(DROP_TABLE_IF_EXISTS_SQL, database, tableName);
		execute(sql);
	}

	public void clearTable(String tableName) throws Throwable {
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		String sql = String.format(TRUNCATE_TABLE_SQL, database, tableName);
		execute(sql);
	}

	public MysqlBinlogPosition readBinlogPosition() throws Throwable {
		AtomicReference<MysqlBinlogPosition> mysqlBinlogPositionAtomicReference = new AtomicReference<>();
		query("SHOW MASTER STATUS", rs -> {
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
		query("SHOW VARIABLES LIKE 'SERVER_ID'", rs -> {
			if (rs.next()) {
				serverId.set(rs.getString("Value"));
			}
		});
		return serverId.get();
	}

	public DataMap getTableInfo(String tableName) throws Throwable {
		DataMap  dataMap = DataMap.create();
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		List  list  = new ArrayList();
		list.add("TABLE_ROWS");
		list.add("DATA_LENGTH");
		try {
			query(String.format(CHECK_TABLE_EXISTS_SQL, database, tableName),resultSet -> {
				while (resultSet.next()) {
					dataMap.putAll(DbKit.getRowFromResultSet(resultSet, list));
				}
			});

		}catch (Throwable e) {
			TapLogger.error(TAG, "Execute getTableInfo failed, error: " + e.getMessage(), e);
		}
		return dataMap;
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
		//Corrections -07:59:59 to GMT-08:00
		int m = Integer.parseInt(split[1]);
		if (m != 0) {
			split[1] = "00";
			int h = Math.abs(Integer.parseInt(str)) + 1;
			if (h < 10) {
				str = "0" + h;
			} else {
				str = h + "";
			}
			if (split[0].contains("-")) {
				str = "-" + str;
			}
		}
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
		return sb.append(":").append(split[1]).toString();
	}

	@Override
	public void close() throws Exception {
		JdbcUtil.closeQuietly(hikariDataSource);
	}

	public TapConnectionContext getTapConnectionContext() {
		return tapConnectionContext;
	}

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
			hikariDataSource.setKeepaliveTime(60 * 1000L);
			return hikariDataSource;
		}
	}
}

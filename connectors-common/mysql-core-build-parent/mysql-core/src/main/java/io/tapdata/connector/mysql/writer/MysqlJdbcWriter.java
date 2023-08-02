package io.tapdata.connector.mysql.writer;

import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.util.JdbcUtil;
import io.tapdata.connector.mysql.util.MysqlUtil;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.runtime.TapPdkSkippableDataEx;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-10-25 14:30
 **/
public abstract class MysqlJdbcWriter extends MysqlWriter {
	private final static String TAG = MysqlJdbcWriter.class.getSimpleName();
	protected static final String INSERT_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values(%s)";
	protected static final String UPDATE_SQL_TEMPLATE = "UPDATE `%s`.`%s` SET %s WHERE %s";
	protected static final String DELETE_SQL_TEMPLATE = "DELETE FROM `%s`.`%s` WHERE %s";
	protected static final String CHECK_ROW_EXISTS_TEMPLATE = "SELECT COUNT(1) as count FROM `%s`.`%s` WHERE %s";
	private static final String LARGE_INSERT_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values %s";
	private static final String INSERT_IGNORE_SQL_TEMPLATE = "INSERT IGNORE INTO `%s`.`%s`(%s) values %s";
	private static final String REPLACE_INTO_SQL_TEMPLATE = "REPLACE INTO `%s`.`%s`(%s) values %s";
	private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
	private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
	protected final Map<String, JdbcCache> jdbcCacheMap;

	public MysqlJdbcWriter(MysqlJdbcContextV2 mysqlJdbcContext) throws Throwable {
		super(mysqlJdbcContext);
		this.jdbcCacheMap = new ConcurrentHashMap<>();
	}

	public MysqlJdbcWriter(MysqlJdbcContextV2 mysqlJdbcContext, Map<String, JdbcCache> jdbcCacheMap) throws Throwable {
		super(mysqlJdbcContext);
		this.jdbcCacheMap = jdbcCacheMap;
	}

	public JdbcCache getJdbcCache() {
		String name = Thread.currentThread().getName();
		JdbcCache jdbcCache = jdbcCacheMap.get(name);
		if (null == jdbcCache) {
			synchronized (this.jdbcCacheMap) {
				jdbcCache = jdbcCacheMap.get(name);
				if (null == jdbcCache) {
					try {
						Connection connection = mysqlJdbcContext.getConnection();
						jdbcCache = new JdbcCache(connection);
						jdbcCacheMap.put(name, jdbcCache);
					} catch (SQLException e) {
						throw new RuntimeException(String.format("Create jdbc connection failed, error: %s", e.getMessage()), e);
					}
				}
			}
		}
		return jdbcCache;
	}

	protected PreparedStatement getInsertPreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		JdbcCache jdbcCache = getJdbcCache();
		Map<String, PreparedStatement> insertMap = jdbcCache.getInsertMap();
		String key = getKey(tapTable, tapRecordEvent);
		PreparedStatement preparedStatement = insertMap.get(key);
		if (null == preparedStatement) {
			DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
			String database = connectionConfig.getString("database");
			String name = connectionConfig.getString("name");
			String tableId = tapTable.getId();
			LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
			if (MapUtils.isEmpty(nameFieldMap)) {
				throw new Exception("Create insert prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
			}
			List<String> fields = new ArrayList<>();
			nameFieldMap.forEach((fieldName, field) -> {
				if (!needAddIntoPreparedStatementValues(field, tapRecordEvent)) {
					return;
				}
				fields.add("`" + fieldName + "`");
			});
			List<String> questionMarks = fields.stream().map(f -> "?").collect(Collectors.toList());
			String sql = String.format(INSERT_SQL_TEMPLATE, database, tableId, String.join(",", fields), String.join(",", questionMarks));
			try {
				preparedStatement = jdbcCache.getConnection().prepareStatement(sql);
			} catch (SQLException e) {
				throw new Exception("Create insert prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
			} catch (Exception e) {
				throw new Exception("Create insert prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
			}
			insertMap.put(key, preparedStatement);
		}
		return preparedStatement;
	}

	protected PreparedStatement getUpdatePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		JdbcCache jdbcCache = getJdbcCache();
		Map<String, PreparedStatement> updateMap = jdbcCache.getUpdateMap();
		String key = getKey(tapTable, tapRecordEvent);
		PreparedStatement preparedStatement = updateMap.get(key);
		if (null == preparedStatement) {
			DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
			String database = connectionConfig.getString("database");
			String name = connectionConfig.getString("name");
			String tableId = tapTable.getId();
			LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
			if (MapUtils.isEmpty(nameFieldMap)) {
				throw new Exception("Create update prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
			}
			List<String> setList = new ArrayList<>();
			nameFieldMap.forEach((fieldName, field) -> {
				if (!needAddIntoPreparedStatementValues(field, tapRecordEvent)) {
					return;
				}
				setList.add("`" + fieldName + "`=?");
			});
			List<String> whereList = new ArrayList<>();
			Collection<String> uniqueKeys = getUniqueKeys(tapTable);
			for (String uniqueKey : uniqueKeys) {
				whereList.add("`" + uniqueKey + "`<=>?");
			}
			String sql = String.format(UPDATE_SQL_TEMPLATE, database, tableId, String.join(",", setList), String.join(" AND ", whereList));
			try {
				preparedStatement = jdbcCache.getConnection().prepareStatement(sql);
			} catch (SQLException e) {
				throw new Exception("Create update prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
			} catch (Exception e) {
				throw new Exception("Create update prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
			}
			updateMap.put(key, preparedStatement);
		}
		return preparedStatement;
	}

	protected PreparedStatement getDeletePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		JdbcCache jdbcCache = getJdbcCache();
		Map<String, PreparedStatement> deleteMap = jdbcCache.getDeleteMap();
		String key = getKey(tapTable, tapRecordEvent);
		PreparedStatement preparedStatement = deleteMap.get(key);
		if (null == preparedStatement) {
			DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
			String database = connectionConfig.getString("database");
			String name = connectionConfig.getString("name");
			String tableId = tapTable.getId();
			LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
			if (MapUtils.isEmpty(nameFieldMap)) {
				throw new Exception("Create delete prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
			}
			List<String> whereList = new ArrayList<>();
			Collection<String> uniqueKeys = getUniqueKeys(tapTable);
			for (String uniqueKey : uniqueKeys) {
				whereList.add("`" + uniqueKey + "`<=>?");
			}
			String sql = String.format(DELETE_SQL_TEMPLATE, database, tableId, String.join(" AND ", whereList));
			try {
				preparedStatement = jdbcCache.getConnection().prepareStatement(sql);
			} catch (SQLException e) {
				throw new Exception("Create delete prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
			} catch (Exception e) {
				throw new Exception("Create delete prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
			}
			deleteMap.put(key, preparedStatement);
		}
		return preparedStatement;
	}

	protected PreparedStatement getCheckRowExistsPreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		JdbcCache jdbcCache = getJdbcCache();
		Map<String, PreparedStatement> checkExistsMap = jdbcCache.getCheckExistsMap();
		String key = getKey(tapTable, tapRecordEvent);
		PreparedStatement preparedStatement = checkExistsMap.get(key);
		if (null == preparedStatement) {
			DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
			String database = connectionConfig.getString("database");
			String name = connectionConfig.getString("name");
			String tableId = tapTable.getId();
			LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
			if (MapUtils.isEmpty(nameFieldMap)) {
				throw new Exception("Create check row exists prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
			}
			List<String> whereList = new ArrayList<>();
			Collection<String> uniqueKeys = getUniqueKeys(tapTable);
			for (String uniqueKey : uniqueKeys) {
				whereList.add("`" + uniqueKey + "`<=>?");
			}
			String sql = String.format(CHECK_ROW_EXISTS_TEMPLATE, database, tableId, String.join(" AND ", whereList));
			try {
				preparedStatement = jdbcCache.getConnection().prepareStatement(sql);
			} catch (SQLException e) {
				throw new Exception("Create check row exists prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
			} catch (Exception e) {
				throw new Exception("Create check row exists prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
			}
			checkExistsMap.put(key, preparedStatement);
		}
		return preparedStatement;
	}

	protected int setPreparedStatementValues(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, PreparedStatement preparedStatement) throws Throwable {
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		int parameterIndex = 1;
		Map<String, Object> after = getAfter(tapRecordEvent);
		if (MapUtils.isEmpty(after)) {
			throw new Exception("Set prepared statement values failed, after is empty: " + tapRecordEvent);
		}
		List<String> afterKeys = new ArrayList<>(after.keySet());
		for (String fieldName : nameFieldMap.keySet()) {
			try {
				TapField tapField = nameFieldMap.get(fieldName);
				if (!needAddIntoPreparedStatementValues(tapField, tapRecordEvent)) {
					continue;
				}
				preparedStatement.setObject(parameterIndex++, after.get(fieldName));
				afterKeys.remove(fieldName);
			} catch (SQLException e) {
				throw new TapPdkSkippableDataEx(String.format("Set prepared statement values failed: %s, field: '%s', value '%s', record: %s"
						, e.getMessage()
						, fieldName
						, after.get(fieldName)
						, tapRecordEvent
				), tapConnectorContext.getSpecification().getId(), e);
			}
		}
		if (CollectionUtils.isNotEmpty(afterKeys)) {
			Map<String, Object> missingAfter = new HashMap<>();
			afterKeys.forEach(k -> missingAfter.put(k, after.get(k)));
			TapLogger.warn(TAG, "Found fields in after data not exists in schema fields, will skip it: " + missingAfter);
		}
		return parameterIndex;
	}

	protected void setPreparedStatementWhere(TapTable tapTable, TapRecordEvent tapRecordEvent, PreparedStatement preparedStatement, int parameterIndex) throws Throwable {
		if (parameterIndex <= 1) {
			parameterIndex = 1;
		}
		Map<String, Object> before = getBefore(tapRecordEvent);
		Map<String, Object> after = getAfter(tapRecordEvent);
		if (MapUtils.isEmpty(before) && MapUtils.isEmpty(after)) {
			throw new Exception("Set prepared statement where clause failed, before and after both empty: " + tapRecordEvent);
		}
		Collection<String> uniqueKeys = getUniqueKeys(tapTable);
		for (String uniqueKey : uniqueKeys) {
			if (!after.containsKey(uniqueKey) && !(EmptyKit.isNotEmpty(before) && before.containsKey(uniqueKey))) {
				throw new Exception("Set prepared statement where clause failed, unique key \"" + uniqueKey + "\" not exists in data: " + tapRecordEvent);
			}
			Object value = (EmptyKit.isNotEmpty(before) && before.containsKey(uniqueKey)) ? before.get(uniqueKey) : after.get(uniqueKey);
			preparedStatement.setObject(parameterIndex++, value);
		}
	}

	protected String appendLargeInsertSql(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) {
		String valueString = getValueString(tapTable, tapRecordEvents);
		DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		return String.format(LARGE_INSERT_SQL_TEMPLATE, database, tapTable.getId(), getFieldString(tapTable), valueString);
	}

	protected String appendLargeInsertOnDuplicateUpdateSql(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) {
		return appendLargeInsertSql(tapConnectorContext, tapTable, tapRecordEvents) + appendOnDuplicateKeyUpdate(tapTable);
	}

	protected String appendLargeInsertIgnoreSql(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) {
		String valueString = getValueString(tapTable, tapRecordEvents);
		DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		return String.format(INSERT_IGNORE_SQL_TEMPLATE, database, tapTable.getId(), getFieldString(tapTable), valueString);
	}

	protected String appendReplaceIntoSql(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) {
		String valueString = getValueString(tapTable, tapRecordEvents);
		String database = tapConnectorContext.getConnectionConfig().getString("database");
		return String.format(REPLACE_INTO_SQL_TEMPLATE, database, tapTable.getId(), getFieldString(tapTable), valueString);
	}

	protected String getFieldString(TapTable tapTable) {
		Set<String> fieldSet = tapTable.getNameFieldMap().keySet();
		return "`" + String.join("`,`", fieldSet) + "`";
	}

	protected String getValueString(TapTable tapTable, List<TapRecordEvent> tapRecordEvents) {
		List<String> valueList = new ArrayList<>();
		Set<String> fieldSet = tapTable.getNameFieldMap().keySet();
		for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
			List<String> oneRowValueList = new ArrayList<>();
			Map<String, Object> after = getAfter(tapRecordEvent);
			for (String fieldName : fieldSet) {
				Object obj = after.get(fieldName);
				oneRowValueList.add(MysqlUtil.object2String(obj));
			}
			valueList.add("(" + String.join(",", oneRowValueList) + ")");
		}
		return String.join(",", valueList);
	}

	protected String appendOnDuplicateKeyUpdate(TapTable tapTable) {
		String sql = "\nON DUPLICATE KEY UPDATE\n  ";
		List<String> list = new ArrayList<>();
		Set<String> fieldSet = tapTable.getNameFieldMap().keySet();
		for (String fieldName : fieldSet) {
			list.add("`" + fieldName + "`=VALUES(`" + fieldName + "`)");
		}
		sql += String.join(",", list);
		return sql;
	}

	@Override
	public void selfCheck() {
		synchronized (this.jdbcCacheMap) {
			jdbcCacheMap.values().removeIf(jdbcCache -> !jdbcCache.checkAlive());
		}
	}

	protected static class JdbcCache {
		private Connection connection;
		private final Map<String, PreparedStatement> insertMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
		private final Map<String, PreparedStatement> updateMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
		private final Map<String, PreparedStatement> deleteMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
		private final Map<String, PreparedStatement> checkExistsMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));

		public JdbcCache(Connection connection) {
			this.connection = connection;
		}

		public Map<String, PreparedStatement> getInsertMap() {
			return insertMap;
		}

		public Map<String, PreparedStatement> getUpdateMap() {
			return updateMap;
		}

		public Map<String, PreparedStatement> getDeleteMap() {
			return deleteMap;
		}

		public Map<String, PreparedStatement> getCheckExistsMap() {
			return checkExistsMap;
		}

		public Statement getStatement() throws SQLException {
			if (null == connection)
				throw new IllegalArgumentException("Cannot create sql statement when connection is null");
			if (!connection.isValid(5)) throw new RuntimeException("Connection is invalid");
			return connection.createStatement();
		}

		public boolean checkAlive() {
			try {
				return connection.isValid(5);
			} catch (SQLException ignored) {
				return false;
			}
		}

		public Connection getConnection() {
			return connection;
		}

		public void clear() {
			this.insertMap.clear();
			this.updateMap.clear();
			this.deleteMap.clear();
			this.checkExistsMap.clear();
			try {
				connection.close();
			} catch (Throwable e) {
				TapLogger.warn(TAG, "Close JdbcCache connection failed: " + e.getMessage());
			}
		}
	}
}

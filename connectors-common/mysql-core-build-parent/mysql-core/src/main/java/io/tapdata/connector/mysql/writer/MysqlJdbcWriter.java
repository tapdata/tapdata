package io.tapdata.connector.mysql.writer;

import io.tapdata.connector.mysql.MysqlJdbcContext;
import io.tapdata.connector.mysql.util.JdbcUtil;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-10-25 14:30
 **/
public abstract class MysqlJdbcWriter extends MysqlWriter{
	private final static String TAG = MysqlJdbcWriter.class.getSimpleName();
	protected static final String INSERT_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values(%s)";
	protected static final String UPDATE_SQL_TEMPLATE = "UPDATE `%s`.`%s` SET %s WHERE %s";
	protected static final String DELETE_SQL_TEMPLATE = "DELETE FROM `%s`.`%s` WHERE %s";
	protected static final String CHECK_ROW_EXISTS_TEMPLATE = "SELECT COUNT(1) as count FROM `%s`.`%s` WHERE %s";
	protected final Map<String, JdbcCache> jdbcCacheMap = new ConcurrentHashMap<>();

	public MysqlJdbcWriter(MysqlJdbcContext mysqlJdbcContext) throws Throwable {
		super(mysqlJdbcContext);
	}

	protected JdbcCache getJdbcCache() {
		String name = Thread.currentThread().getName();
		JdbcCache jdbcCache = jdbcCacheMap.get(name);
		if (null == jdbcCache) {
			jdbcCache = new JdbcCache();
			jdbcCacheMap.put(name, jdbcCache);
		}
		return jdbcCache;
	}

	protected PreparedStatement getInsertPreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		Map<String, PreparedStatement> insertMap = getJdbcCache().getInsertMap();
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
				preparedStatement = this.connection.prepareStatement(sql);
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
		Map<String, PreparedStatement> updateMap = getJdbcCache().getUpdateMap();
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
				preparedStatement = this.connection.prepareStatement(sql);
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
		Map<String, PreparedStatement> deleteMap = getJdbcCache().getDeleteMap();
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
				preparedStatement = this.connection.prepareStatement(sql);
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
		Map<String, PreparedStatement> checkExistsMap = getJdbcCache().getCheckExistsMap();
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
				preparedStatement = this.connection.prepareStatement(sql);
			} catch (SQLException e) {
				throw new Exception("Create check row exists prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
			} catch (Exception e) {
				throw new Exception("Create check row exists prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
			}
			checkExistsMap.put(key, preparedStatement);
		}
		return preparedStatement;
	}

	protected int setPreparedStatementValues(TapTable tapTable, TapRecordEvent tapRecordEvent, PreparedStatement preparedStatement) throws Throwable {
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		int parameterIndex = 1;
		Map<String, Object> after = getAfter(tapRecordEvent);
		if (MapUtils.isEmpty(after)) {
			throw new Exception("Set prepared statement values failed, after is empty: " + tapRecordEvent);
		}
		List<String> afterKeys = new ArrayList<>(after.keySet());
		for (String fieldName : nameFieldMap.keySet()) {
			TapField tapField = nameFieldMap.get(fieldName);
			if (!needAddIntoPreparedStatementValues(tapField, tapRecordEvent)) {
				continue;
			}
			preparedStatement.setObject(parameterIndex++, after.get(fieldName));
			afterKeys.remove(fieldName);
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
		Map<String, Object> data;
		if (MapUtils.isNotEmpty(before)) {
			data = before;
		} else {
			data = after;
		}
		Collection<String> uniqueKeys = getUniqueKeys(tapTable);
		for (String uniqueKey : uniqueKeys) {
			if (!data.containsKey(uniqueKey)) {
				throw new Exception("Set prepared statement where clause failed, unique key \"" + uniqueKey + "\" not exists in data: " + tapRecordEvent);
			}
			Object value = data.get(uniqueKey);
			preparedStatement.setObject(parameterIndex++, value);
		}
	}

	protected static class JdbcCache {
		private final Map<String, PreparedStatement> insertMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
		private final Map<String, PreparedStatement> updateMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
		private final Map<String, PreparedStatement> deleteMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
		private final Map<String, PreparedStatement> checkExistsMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));

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

		public void clear() {
			this.insertMap.clear();
			this.updateMap.clear();
			this.deleteMap.clear();
			this.checkExistsMap.clear();
		}
	}
}

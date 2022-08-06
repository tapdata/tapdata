package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.util.JdbcUtil;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-05-06 10:36
 **/
public class MysqlJdbcOneByOneWriter extends MysqlWriter {

	private static final String TAG = MysqlJdbcOneByOneWriter.class.getSimpleName();
	private final Map<String, JdbcCache> jdbcCacheMap = new ConcurrentHashMap<>();
	private final AtomicBoolean running = new AtomicBoolean(true);

	public MysqlJdbcOneByOneWriter(MysqlJdbcContext mysqlJdbcContext) throws Throwable {
		super(mysqlJdbcContext);
	}

	@Override
	public WriteListResult<TapRecordEvent> write(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
		WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
		TapRecordEvent errorRecord = null;
		try {
			for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
				if (!running.get()) break;
				try {
					if (tapRecordEvent instanceof TapInsertRecordEvent) {
						int insertRow = doInsertOne(tapConnectorContext, tapTable, tapRecordEvent);
						writeListResult.incrementInserted(insertRow);
					} else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
						int updateRow = doUpdateOne(tapConnectorContext, tapTable, tapRecordEvent);
						writeListResult.incrementModified(updateRow);
					} else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
						int deleteRow = doDeleteOne(tapConnectorContext, tapTable, tapRecordEvent);
						writeListResult.incrementRemove(deleteRow);
					} else {
						writeListResult.addError(tapRecordEvent, new Exception("Event type \"" + tapRecordEvent.getClass().getSimpleName() + "\" not support: " + tapRecordEvent));
					}
				} catch (Throwable e) {
					errorRecord = tapRecordEvent;
					throw e;
				}
			}
			MysqlJdbcContext.tryCommit(connection);
		} catch (Throwable e) {
			writeListResult.setInsertedCount(0);
			writeListResult.setModifiedCount(0);
			writeListResult.setRemovedCount(0);
			if (null != errorRecord) writeListResult.addError(errorRecord, e);
			MysqlJdbcContext.tryRollBack(connection);
			throw e;
		}
		return writeListResult;
	}

	@Override
	public void onDestroy() {
		this.running.set(false);
		this.jdbcCacheMap.values().forEach(JdbcCache::clear);
	}

	private int doInsertOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		String insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
		if (null != tapConnectorContext.getConnectorCapabilities()
				&& null != tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY)) {
			insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
		}
		if (
				CollectionUtils.isEmpty(tapTable.primaryKeys())
						&& (CollectionUtils.isEmpty(tapTable.getIndexList()) || null == tapTable.getIndexList().stream().filter(TapIndex::isUnique).findFirst().orElse(null))
						&& ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertDmlPolicy)
						&& rowExists(tapConnectorContext, tapTable, tapRecordEvent)
		) {
			return 0;
		}
		int row;
		try {
			row = doInsert(tapConnectorContext, tapTable, tapRecordEvent);
		} catch (Throwable e) {
			if (e.getCause() instanceof SQLIntegrityConstraintViolationException) {
				if (rowExists(tapConnectorContext, tapTable, tapRecordEvent)) {
					if (ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertDmlPolicy)) {
						return 0;
					} else {
						TapLogger.warn(TAG, "Execute insert failed, will retry update after check record exists");
						row = doUpdate(tapConnectorContext, tapTable, tapRecordEvent);
					}
				} else {
					throw e;
				}
			} else {
				throw e;
			}
		}
		return row;
	}

	private int doInsert(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		PreparedStatement insertPreparedStatement = getInsertPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent);
		setPreparedStatementValues(tapTable, tapRecordEvent, insertPreparedStatement);
		try {
			return insertPreparedStatement.executeUpdate();
		} catch (Throwable e) {
			throw new RuntimeException("Insert data failed, sql: " + insertPreparedStatement + ", message: " + e.getMessage(), e);
		}
	}

	private int doUpdateOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		String updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
		if (null != tapConnectorContext.getConnectorCapabilities()
				&& null != tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY)) {
			updateDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
		}
		PreparedStatement updatePreparedStatement = getUpdatePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent);
		int parameterIndex = setPreparedStatementValues(tapTable, tapRecordEvent, updatePreparedStatement);
		setPreparedStatementWhere(tapTable, tapRecordEvent, updatePreparedStatement, parameterIndex);
		int row = doUpdate(tapConnectorContext, tapTable, tapRecordEvent);
		if (row <= 0 && ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS.equals(updateDmlPolicy)) {
			doInsert(tapConnectorContext, tapTable, tapRecordEvent);
		}
		return row;
	}

	private int doUpdate(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		PreparedStatement updatePreparedStatement = getUpdatePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent);
		int parameterIndex = setPreparedStatementValues(tapTable, tapRecordEvent, updatePreparedStatement);
		setPreparedStatementWhere(tapTable, tapRecordEvent, updatePreparedStatement, parameterIndex);
		try {
			return updatePreparedStatement.executeUpdate();
		} catch (Exception e) {
			throw new RuntimeException("Update data failed, sql: " + updatePreparedStatement + ", message: " + e.getMessage(), e);
		}
	}

	private int doDeleteOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		PreparedStatement deletePreparedStatement = getDeletePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent);
		setPreparedStatementWhere(tapTable, tapRecordEvent, deletePreparedStatement, 1);
		int row;
		try {
			row = deletePreparedStatement.executeUpdate();
		} catch (Throwable e) {
			throw new Exception("Delete data failed, sql: " + deletePreparedStatement + ", message: " + e.getMessage(), e);
		}
		return row;
	}

	private boolean rowExists(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		if (CollectionUtils.isEmpty(tapTable.primaryKeys(true))) {
			// not have any logic primary key(s)
			return false;
		}
		PreparedStatement checkRowExistsPreparedStatement = getCheckRowExistsPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent);
		setPreparedStatementWhere(tapTable, tapRecordEvent, checkRowExistsPreparedStatement, 1);
		AtomicBoolean result = new AtomicBoolean(false);
		try {
			this.mysqlJdbcContext.query(checkRowExistsPreparedStatement, rs -> {
				if (rs.next()) {
					int count = rs.getInt("count");
					result.set(count > 0);
				}
			});
		} catch (Throwable e) {
			throw new Exception("Check row exists failed, sql: " + checkRowExistsPreparedStatement + ", message: " + e.getMessage(), e);
		}
		return result.get();
	}

	private JdbcCache getJdbcCache() {
		String name = Thread.currentThread().getName();
		JdbcCache jdbcCache = jdbcCacheMap.get(name);
		if (null == jdbcCache) {
			jdbcCache = new JdbcCache();
			jdbcCacheMap.put(name, jdbcCache);
		}
		return jdbcCache;
	}

	private PreparedStatement getInsertPreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
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

	private PreparedStatement getUpdatePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
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

	private PreparedStatement getDeletePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
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

	private PreparedStatement getCheckRowExistsPreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
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

	private static class JdbcCache {
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

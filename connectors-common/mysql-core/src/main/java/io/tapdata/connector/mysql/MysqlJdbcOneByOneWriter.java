package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.util.JdbcUtil;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;

import java.sql.PreparedStatement;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author samuel
 * @Description
 * @create 2022-05-06 10:36
 **/
public class MysqlJdbcOneByOneWriter extends MysqlWriter {

	private static final String TAG = MysqlJdbcOneByOneWriter.class.getSimpleName();
	private final Map<String, PreparedStatement> insertMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
	private final Map<String, PreparedStatement> updateMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
	private final Map<String, PreparedStatement> deleteMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
	private final Map<String, PreparedStatement> checkExistsMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
	private AtomicBoolean running = new AtomicBoolean(true);

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
		this.insertMap.clear();
		this.updateMap.clear();
		this.deleteMap.clear();
		this.checkExistsMap.clear();
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
			if (null != e.getCause() && e.getCause() instanceof SQLIntegrityConstraintViolationException) {
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
		PreparedStatement insertPreparedStatement = getInsertPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, insertMap);
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
		PreparedStatement updatePreparedStatement = getUpdatePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, updateMap);
		int parameterIndex = setPreparedStatementValues(tapTable, tapRecordEvent, updatePreparedStatement);
		setPreparedStatementWhere(tapTable, tapRecordEvent, updatePreparedStatement, parameterIndex);
		int row = doUpdate(tapConnectorContext, tapTable, tapRecordEvent);
		if (row <= 0 && ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS.equals(updateDmlPolicy)) {
			doInsert(tapConnectorContext, tapTable, tapRecordEvent);
		}
		return row;
	}

	private int doUpdate(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		PreparedStatement updatePreparedStatement = getUpdatePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, updateMap);
		int parameterIndex = setPreparedStatementValues(tapTable, tapRecordEvent, updatePreparedStatement);
		setPreparedStatementWhere(tapTable, tapRecordEvent, updatePreparedStatement, parameterIndex);
		try {
			return updatePreparedStatement.executeUpdate();
		} catch (Exception e) {
			throw new RuntimeException("Update data failed, sql: " + updatePreparedStatement + ", message: " + e.getMessage(), e);
		}
	}

	private int doDeleteOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		PreparedStatement deletePreparedStatement = getDeletePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, deleteMap);
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
		PreparedStatement checkRowExistsPreparedStatement = getCheckRowExistsPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, checkExistsMap);
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
}

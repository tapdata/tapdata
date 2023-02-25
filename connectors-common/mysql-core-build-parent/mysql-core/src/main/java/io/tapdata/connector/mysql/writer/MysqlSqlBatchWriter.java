package io.tapdata.connector.mysql.writer;

import io.debezium.util.HexConverter;
import io.tapdata.connector.mysql.MysqlJdbcContext;
import io.tapdata.connector.mysql.util.JdbcUtil;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author samuel
 * @Description
 * @create 2022-10-25 16:13
 **/
public class MysqlSqlBatchWriter extends MysqlWriter {
	private final static String TAG = MysqlSqlBatchWriter.class.getSimpleName();
	private static final String INSERT_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values %s";
	private static final String INSERT_IGNORE_SQL_TEMPLATE = "INSERT IGNORE INTO `%s`.`%s`(%s) values %s";
	private static final String REPLACE_INTO_SQL_TEMPLATE = "REPLACE INTO `%s`.`%s`(%s) values %s";
	private static final String DELETE_FROM_SQL_TEMPLATE = "DELETE FROM `%s`.`%s` WHERE %s";
	private final MysqlJdbcOneByOneWriter mysqlJdbcOneByOneWriter;
	private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
	private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
	private final Map<String, Statement> statementMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));

	public MysqlSqlBatchWriter(MysqlJdbcContext mysqlJdbcContext) throws Throwable {
		super(mysqlJdbcContext);
		this.mysqlJdbcOneByOneWriter = new MysqlJdbcOneByOneWriter(mysqlJdbcContext);
	}

	@Override
	public WriteListResult<TapRecordEvent> write(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
		AtomicReference<WriteListResult<TapRecordEvent>> writeListResult = new AtomicReference<>(new WriteListResult<>(0L, 0L, 0L, new HashMap<>()));
		try {
			dispatch(tapRecordEvents, consumeEvents -> {
				if (!isAlive()) return;
				try {
					if (consumeEvents.get(0) instanceof TapInsertRecordEvent) {
						if (canLargeInsertSql(tapTable)) {
							writeListResult.get().incrementInserted(doInsert(tapConnectorContext, tapTable, consumeEvents));
						} else {
							doOneByOne(tapConnectorContext, tapTable, writeListResult, consumeEvents);
						}
					} else if (consumeEvents.get(0) instanceof TapUpdateRecordEvent) {
						String dmlUpdatePolicy = getDmlUpdatePolicy(tapConnectorContext);
						if (canReplaceInto(tapTable, dmlUpdatePolicy)) {
							writeListResult.get().incrementModified(doUpdate(tapConnectorContext, tapTable, consumeEvents));
						} else {
							doOneByOne(tapConnectorContext, tapTable, writeListResult, consumeEvents);
						}
					} else if (consumeEvents.get(0) instanceof TapDeleteRecordEvent) {
						writeListResult.get().incrementRemove(doDelete(tapConnectorContext, tapTable, consumeEvents));
					}
				} catch (Throwable e) {
					this.connection.rollback();
					if (isAlive()) {
						TapLogger.warn(TAG, "Do batch operation failed: " + e.getMessage() + "\n Will try one by one mode");
						doOneByOne(tapConnectorContext, tapTable, writeListResult, consumeEvents);
					}
				}
			});
		} catch (Throwable e) {
			if (isAlive()) {
				throw e;
			}
		}
		return writeListResult.get();
	}

	private void doOneByOne(TapConnectorContext tapConnectorContext, TapTable tapTable, AtomicReference<WriteListResult<TapRecordEvent>> writeListResult, List<TapRecordEvent> tapRecordEvents) throws Throwable {
		WriteListResult<TapRecordEvent> oneByOneInsertResult = mysqlJdbcOneByOneWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
		writeListResult.get().incrementInserted(oneByOneInsertResult.getInsertedCount());
		writeListResult.get().incrementModified(oneByOneInsertResult.getModifiedCount());
		writeListResult.get().incrementRemove(oneByOneInsertResult.getRemovedCount());
		writeListResult.get().addErrors(oneByOneInsertResult.getErrorMap());
	}

	private boolean canLargeInsertSql(TapTable tapTable) {
		Collection<String> pkOrUniqueIndex = tapTable.primaryKeys();
		if (CollectionUtils.isEmpty(pkOrUniqueIndex)) {
			return false;
		}
		return true;
	}

	private boolean canReplaceInto(TapTable tapTable, String updatePolicy) {
		Collection<String> pkOrUniqueIndex = tapTable.primaryKeys();
		if (CollectionUtils.isEmpty(pkOrUniqueIndex)) {
			return false;
		}
		if (ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS.equals(updatePolicy)) {
			return false;
		}
		return true;
	}

	private int doInsert(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
		if (CollectionUtils.isEmpty(tapRecordEvents)) {
			return 0;
		}
		int result;
		String sql;
		String dmlInsertPolicy = getDmlInsertPolicy(tapConnectorContext);
		if (ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(dmlInsertPolicy)) {
			sql = appendLargeInsertIgnoreSql(tapConnectorContext, tapTable, tapRecordEvents);
		} else {
			sql = appendLargeInsertOnDuplicateUpdateSql(tapConnectorContext, tapTable, tapRecordEvents);
		}
		TapLogger.debug(TAG, "Execute insert sql: " + sql);
		Statement statement = getStatement();
		try {
			result = statement.executeUpdate(sql);
		} catch (SQLException e) {
			throw new RuntimeException("Execute insert sql failed: " + e.getMessage() + "\nSql: " + sql, e);
		}
		this.connection.commit();
		return result;
	}

	private int doUpdate(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
		if (CollectionUtils.isEmpty(tapRecordEvents)) {
			return 0;
		}
		int result;
//		String sql = appendReplaceIntoSql(tapConnectorContext, tapTable, tapRecordEvents);
		String sql = appendLargeInsertOnDuplicateUpdateSql(tapConnectorContext, tapTable, tapRecordEvents);
		TapLogger.debug(TAG, "Execute update sql: " + sql);
		Statement statement = getStatement();
		try {
			statement.execute(sql);
			result = tapRecordEvents.size();
		} catch (SQLException e) {
			throw new RuntimeException("Execute update sql failed: " + e.getMessage() + "\nSql: " + sql, e);
		}
		this.connection.commit();
		return result;
	}

	private int doDelete(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
		if (CollectionUtils.isEmpty(tapRecordEvents)) {
			return 0;
		}
		Collection<String> primaryKeys = tapTable.primaryKeys(true);
		List<String> whereList = new ArrayList<>();
		for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
			Map<String, Object> before = ((TapDeleteRecordEvent) tapRecordEvent).getBefore();
			List<String> subWhereList = new ArrayList<>();
			for (String primaryKey : primaryKeys) {
				if (!before.containsKey(primaryKey)) {
					throw new RuntimeException(String.format("Append delete sql failed, before data not contains key '%s', cannot append where clause in delete sql\nBefore data: %s", primaryKey, before));
				}
				subWhereList.add("`" + primaryKey + "`<=>" + object2String(before.get(primaryKey)));
			}
			whereList.add("(" + String.join(" AND ", subWhereList) + ")");
		}
		String whereClause = String.join(" OR ", whereList);
		String database = tapConnectorContext.getConnectionConfig().getString("database");
		String sql = String.format(DELETE_FROM_SQL_TEMPLATE, database, tapTable.getId(), whereClause);
		TapLogger.debug(TAG, "Execute delete sql: " + sql);
		Statement statement = getStatement();
		int deleted;
		try {
			deleted = statement.executeUpdate(sql);
		} catch (SQLException e) {
			throw new RuntimeException("Execute delete sql failed: " + e.getMessage() + "\nSql: " + sql, e);
		}
		this.connection.commit();
		return deleted;
	}

	private String appendLargeInsertSql(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) {
		String valueString = getValueString(tapTable, tapRecordEvents);
		DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		return String.format(INSERT_SQL_TEMPLATE, database, tapTable.getId(), getFieldString(tapTable), valueString);
	}

	private String appendLargeInsertOnDuplicateUpdateSql(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) {
		return appendLargeInsertSql(tapConnectorContext, tapTable, tapRecordEvents) + appendOnDuplicateKeyUpdate(tapTable);
	}

	private String appendLargeInsertIgnoreSql(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) {
		String valueString = getValueString(tapTable, tapRecordEvents);
		DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		return String.format(INSERT_IGNORE_SQL_TEMPLATE, database, tapTable.getId(), getFieldString(tapTable), valueString);
	}

	private String appendReplaceIntoSql(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) {
		String valueString = getValueString(tapTable, tapRecordEvents);
		String database = tapConnectorContext.getConnectionConfig().getString("database");
		return String.format(REPLACE_INTO_SQL_TEMPLATE, database, tapTable.getId(), getFieldString(tapTable), valueString);
	}

	private String getFieldString(TapTable tapTable) {
		Set<String> fieldSet = tapTable.getNameFieldMap().keySet();
		return "`" + String.join("`,`", fieldSet) + "`";
	}

	private String getValueString(TapTable tapTable, List<TapRecordEvent> tapRecordEvents) {
		List<String> valueList = new ArrayList<>();
		Set<String> fieldSet = tapTable.getNameFieldMap().keySet();
		for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
			List<String> oneRowValueList = new ArrayList<>();
			Map<String, Object> after = getAfter(tapRecordEvent);
			for (String fieldName : fieldSet) {
				Object obj = after.get(fieldName);
				oneRowValueList.add(object2String(obj));
			}
			valueList.add("(" + String.join(",", oneRowValueList) + ")");
		}
		return String.join(",", valueList);
	}

	private String appendOnDuplicateKeyUpdate(TapTable tapTable) {
		String sql = "\nON DUPLICATE KEY UPDATE\n  ";
		List<String> list = new ArrayList<>();
		Set<String> fieldSet = tapTable.getNameFieldMap().keySet();
		for (String fieldName : fieldSet) {
			list.add("`" + fieldName + "`=VALUES(`" + fieldName + "`)");
		}
		sql += String.join(",", list);
		return sql;
	}

	private String object2String(Object obj) {
		String result;
		if (null == obj) {
			result = "null";
		} else if (obj instanceof String) {
			result = "'" + ((String) obj).replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'").replaceAll("\\(", "\\\\(").replaceAll("\\)", "\\\\)") + "'";
		} else if (obj instanceof Number) {
			result = obj.toString();
		} else if (obj instanceof Date) {
			result = "'" + dateFormat.format(obj) + "'";
		} else if (obj instanceof Instant) {
			result = "'" + LocalDateTime.ofInstant((Instant) obj, ZoneId.of("GMT")).format(dateTimeFormatter) + "'";
		} else if (obj instanceof byte[]) {
			String hexString = HexConverter.convertToHexString((byte[]) obj);
			return "X'" + hexString + "'";
		}else if(obj instanceof Boolean){
			if("true".equalsIgnoreCase(obj.toString())){
				return "1";
			}
			return "0";
		}else {
			return "'" + obj + "'";
		}
		return result;
	}

	private Statement getStatement() {
		String threadName = Thread.currentThread().getName();
		if (!statementMap.containsKey(threadName)) {
			synchronized (this.statementMap) {
				if (!statementMap.containsKey(threadName)) {
					try {
						statementMap.put(threadName, this.connection.createStatement());
					} catch (SQLException e) {
						throw new RuntimeException("Create statement failed: " + e.getMessage(), e);
					}
				}
			}
		}
		return statementMap.get(threadName);
	}
}

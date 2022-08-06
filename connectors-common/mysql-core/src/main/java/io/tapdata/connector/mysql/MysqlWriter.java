package io.tapdata.connector.mysql;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.LRUMap;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-05-05 21:18
 **/
public abstract class MysqlWriter {

	private static final String TAG = MysqlWriter.class.getSimpleName();
	protected static final String INSERT_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values(%s)";
	protected static final String UPDATE_SQL_TEMPLATE = "UPDATE `%s`.`%s` SET %s WHERE %s";
	protected static final String DELETE_SQL_TEMPLATE = "DELETE FROM `%s`.`%s` WHERE %s";
	protected static final String CHECK_ROW_EXISTS_TEMPLATE = "SELECT COUNT(1) as count FROM `%s`.`%s` WHERE %s";
	protected MysqlJdbcContext mysqlJdbcContext;
	protected Connection connection;

	public MysqlWriter(MysqlJdbcContext mysqlJdbcContext) throws Throwable {
		this.mysqlJdbcContext = mysqlJdbcContext;
		this.connection = mysqlJdbcContext.getConnection();
	}

	abstract public WriteListResult<TapRecordEvent> write(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable;

	abstract public void onDestroy();

	protected String getKey(TapTable tapTable, TapRecordEvent tapRecordEvent) {
		Map<String, Object> after = getAfter(tapRecordEvent);
		Map<String, Object> before = getBefore(tapRecordEvent);
		Map<String, Object> data;
		if (MapUtils.isNotEmpty(after)) {
			data = after;
		} else {
			data = before;
		}
		Set<String> keys = data.keySet();
		String keyString = String.join("-", keys);
		return tapTable.getId() + "-" + keyString;
	}

	protected Collection<String> getUniqueKeys(TapTable tapTable) {
		return tapTable.primaryKeys(true);
	}

	protected boolean needAddIntoPreparedStatementValues(TapField field, TapRecordEvent tapRecordEvent) {
		Map<String, Object> after = getAfter(tapRecordEvent);
		if (null == after) {
			return false;
		}
		if (!after.containsKey(field.getName())) {
			TapLogger.warn(TAG, "Found schema field not exists in after data, will skip it: " + field.getName());
			return false;
		}
		return true;
	}

	protected Map<String, Object> getBefore(TapRecordEvent tapRecordEvent) {
		Map<String, Object> before = null;
		if (tapRecordEvent instanceof TapUpdateRecordEvent) {
			before = ((TapUpdateRecordEvent) tapRecordEvent).getBefore();
		} else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
			before = ((TapDeleteRecordEvent) tapRecordEvent).getBefore();
		}
		return before;
	}

	protected Map<String, Object> getAfter(TapRecordEvent tapRecordEvent) {
		Map<String, Object> after = null;
		if (tapRecordEvent instanceof TapInsertRecordEvent) {
			after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
		} else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
			after = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
		}
		return after;
	}

	protected static class LRUOnRemoveMap<K, V> extends LRUMap<K, V> {

		private Consumer<Entry<K, V>> onRemove;

		public LRUOnRemoveMap(int maxSize, Consumer<Entry<K, V>> onRemove) {
			super(maxSize);
			this.onRemove = onRemove;
		}

		@Override
		protected boolean removeLRU(LinkEntry<K, V> entry) {
			onRemove.accept(entry);
			return super.removeLRU(entry);
		}

		@Override
		public void clear() {
			Set<Entry<K, V>> entries = this.entrySet();
			for (Entry<K, V> entry : entries) {
				onRemove.accept(entry);
			}
			super.clear();
		}

		@Override
		protected void removeEntry(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
			onRemove.accept(entry);
			super.removeEntry(entry, hashIndex, previous);
		}

		@Override
		protected void removeMapping(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
			onRemove.accept(entry);
			super.removeMapping(entry, hashIndex, previous);
		}
	}
}

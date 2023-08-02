package io.tapdata.connector.mysql.writer;

import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.connector.mysql.MysqlExceptionCollector;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.util.ExceptionWrapper;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.LRUMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-05-05 21:18
 **/
public abstract class MysqlWriter {

	private static final String TAG = MysqlWriter.class.getSimpleName();
	protected MysqlJdbcContextV2 mysqlJdbcContext;
	protected ExceptionWrapper exceptionWrapper;
	protected ExceptionCollector exceptionCollector;
	private final AtomicBoolean running;

	public MysqlWriter(MysqlJdbcContextV2 mysqlJdbcContext) throws Throwable {
		this.mysqlJdbcContext = mysqlJdbcContext;
		this.exceptionWrapper = new ExceptionWrapper();
		this.exceptionCollector = new MysqlExceptionCollector();
		this.running = new AtomicBoolean(true);
	}

	public void setExceptionWrapper(ExceptionWrapper exceptionWrapper) {
		this.exceptionWrapper = exceptionWrapper;
	}

	protected String getDmlInsertPolicy(TapConnectorContext tapConnectorContext) {
		String dmlInsertPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
		if (null != tapConnectorContext.getConnectorCapabilities()
				&& null != tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY)) {
			dmlInsertPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
		}
		return dmlInsertPolicy;
	}

	protected String getDmlUpdatePolicy(TapConnectorContext tapConnectorContext) {
		String dmlUpdatePolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
		if (null != tapConnectorContext.getConnectorCapabilities()
				&& null != tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY)) {
			dmlUpdatePolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
		}
		return dmlUpdatePolicy;
	}

	abstract public WriteListResult<TapRecordEvent> write(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable;

	public void onDestroy() {
		this.running.set(false);
	}

	public void selfCheck() {

	}

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
		Collection<String> primaryKeys = tapTable.primaryKeys(true);
		if (EmptyKit.isEmpty(primaryKeys)) {
			return tapTable.getNameFieldMap().keySet();
		}
		return tapTable.primaryKeys(true);
	}

	protected boolean needAddIntoPreparedStatementValues(TapField field, TapRecordEvent tapRecordEvent) {
		Map<String, Object> after = getAfter(tapRecordEvent);
		if (null == after) {
			return false;
		}
		if (!after.containsKey(field.getName())) {
			TapLogger.debug(TAG, "Found schema field not exists in after data, will skip it: " + field.getName());
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
		} else {
			after = new HashMap<>();
		}
		return after;
	}

	protected void dispatch(List<TapRecordEvent> tapRecordEvents, AnyErrorConsumer<List<TapRecordEvent>> consumer) throws Throwable {
		if (CollectionUtils.isEmpty(tapRecordEvents)) return;
		TapRecordEvent preEvent = null;
		List<TapRecordEvent> consumeList = new ArrayList<>();
		for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
			if (!isAlive()) break;
			if (null != preEvent && !tapRecordEvent.getClass().getName().equals(preEvent.getClass().getName())) {
				consumer.accept(consumeList);
				consumeList.clear();
			}
			consumeList.add(tapRecordEvent);
			preEvent = tapRecordEvent;
		}
		if (CollectionUtils.isNotEmpty(consumeList)) {
			consumer.accept(consumeList);
		}
	}

	public static class LRUOnRemoveMap<K, V> extends LRUMap<K, V> {

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

	protected interface AnyErrorConsumer<T> {
		void accept(T t) throws Throwable;
	}


	protected boolean isAlive() {
		return running.get();
	}

}

package io.tapdata.aspect;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class BatchReadFuncAspect extends DataFunctionAspect<BatchReadFuncAspect> {
	private static final String TAG = BatchReadFuncAspect.class.getSimpleName();
	private TapConnectorContext connectorContext;

	public static final int STATE_ENQUEUED = 10;

	public static final int STATE_READ_COMPLETE = 11;
	public static final int STATE_PROCESS_COMPLETE = 12;

	public BatchReadFuncAspect connectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
		return this;
	}

	private TapTable table;

	public BatchReadFuncAspect table(TapTable table) {
		this.table = table;
		return this;
	}

	private Object offsetState;

	public BatchReadFuncAspect offsetState(Object offsetState) {
		this.offsetState = offsetState;
		return this;
	}

	private int eventBatchSize;

	public BatchReadFuncAspect eventBatchSize(int eventBatchSize) {
		this.eventBatchSize = eventBatchSize;
		return this;
	}

	private List<Consumer<List<TapdataEvent>>> enqueuedConsumers = null;
	public BatchReadFuncAspect enqueuedConsumer(Consumer<List<TapdataEvent>> listConsumer) {
		if (null == enqueuedConsumers) {
			enqueuedConsumers = new CopyOnWriteArrayList<>();
		}
		this.enqueuedConsumers.add(tapdataEvents -> {
			try {
				listConsumer.accept(tapdataEvents);
			} catch(Throwable throwable) {
				TapLogger.warn(TAG, "Consume tapdataEvents from table {} failed on enqueued consumer {}, {}",
						table, listConsumer, InstanceFactory.instance(TapUtils.class).getStackTrace(throwable));
			}
		});
		return this;
	}

	private List<Consumer<List<TapEvent>>> readCompleteConsumers = null;
	public BatchReadFuncAspect readCompleteConsumer(Consumer<List<TapEvent>> listConsumer) {
		if (null == readCompleteConsumers) {
			readCompleteConsumers = new CopyOnWriteArrayList<>();
		}
		this.readCompleteConsumers.add(tapEvents -> {
			try {
				listConsumer.accept(tapEvents);
			} catch(Throwable throwable) {
				TapLogger.warn(TAG, "Consume tapdataEvents from table {} failed on read complete consumer {}, {}",
						table, listConsumer, InstanceFactory.instance(TapUtils.class).getStackTrace(throwable));
			}
		});
		return this;
	}

	private List<Consumer<List<TapdataEvent>>> processCompleteConsumers = null;
	public BatchReadFuncAspect processCompleteConsumer(Consumer<List<TapdataEvent>> listConsumer) {
		if (null == processCompleteConsumers) {
			processCompleteConsumers = new CopyOnWriteArrayList<>();
		}
		this.processCompleteConsumers.add(tapdataEvents -> {
			try {
				listConsumer.accept(tapdataEvents);
			} catch(Throwable throwable) {
				TapLogger.warn(TAG, "Consume tapdataEvents from table {} failed on read complete consumer {}, {}",
						table, listConsumer, InstanceFactory.instance(TapUtils.class).getStackTrace(throwable));
			}
		});
		return this;
	}

	public TapConnectorContext getConnectorContext() {
		return connectorContext;
	}

	public void setConnectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
	}

	public TapTable getTable() {
		return table;
	}

	public void setTable(TapTable table) {
		this.table = table;
	}

	public Object getOffsetState() {
		return offsetState;
	}

	public void setOffsetState(Object offsetState) {
		this.offsetState = offsetState;
	}

	public int getEventBatchSize() {
		return eventBatchSize;
	}

	public void setEventBatchSize(int eventBatchSize) {
		this.eventBatchSize = eventBatchSize;
	}

	public List<Consumer<List<TapdataEvent>>> getEnqueuedConsumers() {
		return enqueuedConsumers;
	}

	public void setEnqueuedConsumers(List<Consumer<List<TapdataEvent>>> enqueuedConsumers) {
		this.enqueuedConsumers = enqueuedConsumers;
	}

	public List<Consumer<List<TapEvent>>> getReadCompleteConsumers() {
		return readCompleteConsumers;
	}

	public void setReadCompleteConsumers(List<Consumer<List<TapEvent>>> readCompleteConsumers) {
		this.readCompleteConsumers = readCompleteConsumers;
	}

	public List<Consumer<List<TapdataEvent>>> getProcessCompleteConsumers() {
		return processCompleteConsumers;
	}

	public void setProcessCompleteConsumers(List<Consumer<List<TapdataEvent>>> processCompleteConsumers) {
		this.processCompleteConsumers = processCompleteConsumers;
	}
}

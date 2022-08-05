package io.tapdata.aspect;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class BatchReadFuncAspect extends DataFunctionAspect<BatchReadFuncAspect> {
	private static final String TAG = BatchReadFuncAspect.class.getSimpleName();
	private TapConnectorContext connectorContext;

	public static final int STATE_BATCHING = 10;

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

	private List<Consumer<List<TapdataEvent>>> consumers = new CopyOnWriteArrayList<>();

	public BatchReadFuncAspect consumer(Consumer<List<TapdataEvent>> listConsumer) {
		this.consumers.add(tapdataEvents -> {
			try {
				listConsumer.accept(tapdataEvents);
			} catch(Throwable throwable) {
				TapLogger.warn(TAG, "Consume tapdataEvents from table {} failed on consumer {}, {}", table, listConsumer, ExceptionUtils.getStackTrace(throwable));
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

	public List<Consumer<List<TapdataEvent>>> getConsumers() {
		return consumers;
	}

	public void setConsumers(List<Consumer<List<TapdataEvent>>> consumers) {
		this.consumers = consumers;
	}
}

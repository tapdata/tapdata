package io.tapdata.aspect;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class WriteRecordFuncAspect extends DataFunctionAspect<WriteRecordFuncAspect> {
	private static final String TAG = WriteRecordFuncAspect.class.getSimpleName();
	private List<BiConsumer<List<TapRecordEvent>, WriteListResult<TapRecordEvent>>> consumers = new CopyOnWriteArrayList<>();
	public static final int STATE_WRITING = 10;
	public WriteRecordFuncAspect consumer(BiConsumer<List<TapRecordEvent>, WriteListResult<TapRecordEvent>> resultConsumer) {
		this.consumers.add((theRecordEvents, writeListResult) -> {
			try {
				resultConsumer.accept(theRecordEvents, writeListResult);
			} catch(Throwable throwable) {
				TapLogger.warn(TAG, "Consume writeListResult {} for recordEvents size {} table {} failed on consumer {}, {}", writeListResult, recordEvents != null ? recordEvents.size() : 0, table, resultConsumer, ExceptionUtils.getStackTrace(throwable));
			}
		});
		return this;
	}
	private List<TapRecordEvent> recordEvents;
	public WriteRecordFuncAspect recordEvents(List<TapRecordEvent> recordEvents) {
		this.recordEvents = recordEvents;
		return this;
	}
	private TapTable table;
	public WriteRecordFuncAspect table(TapTable table) {
		this.table = table;
		return this;
	}
	private TapConnectorContext connectorContext;

	public WriteRecordFuncAspect connectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
		return this;
	}

	public TapConnectorContext getConnectorContext() {
		return connectorContext;
	}

	public void setConnectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
	}

	public List<TapRecordEvent> getRecordEvents() {
		return recordEvents;
	}

	public void setRecordEvents(List<TapRecordEvent> recordEvents) {
		this.recordEvents = recordEvents;
	}

	public TapTable getTable() {
		return table;
	}

	public void setTable(TapTable table) {
		this.table = table;
	}

	public List<BiConsumer<List<TapRecordEvent>, WriteListResult<TapRecordEvent>>> getConsumers() {
		return consumers;
	}

	public void setConsumers(List<BiConsumer<List<TapRecordEvent>, WriteListResult<TapRecordEvent>>> consumers) {
		this.consumers = consumers;
	}
}

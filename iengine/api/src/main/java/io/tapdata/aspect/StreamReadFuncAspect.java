package io.tapdata.aspect;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class StreamReadFuncAspect extends DataFunctionAspect<StreamReadFuncAspect> {
	private static final String TAG = StreamReadFuncAspect.class.getSimpleName();
	private boolean waitRawData = true;
	private Throwable errorDuringWait;
	private StreamReadConsumer streamReadConsumer;
	public StreamReadFuncAspect streamReadConsumer(StreamReadConsumer streamReadConsumer) {
		this.streamReadConsumer = streamReadConsumer;
		return this;
	}
	private String streamReadFunction;
	public StreamReadFuncAspect streamReadFunction(String streamReadFunction) {
		this.streamReadFunction = streamReadFunction;
		return this;
	}
	private Long streamStartedTime;
	public StreamReadFuncAspect streamStartedTime(Long streamStartedTime) {
		this.streamStartedTime = streamStartedTime;
		return this;
	}
	public static final int STATE_STREAM_STARTED = 5;
	public static final int STATE_STREAMING_READ_COMPLETED = 10;
	public static final int STATE_STREAMING_PROCESS_COMPLETED = 11;
	public static final int STATE_STREAMING_ENQUEUED = 12;
	public static final int STATE_CALLBACK_RAW_DATA = 15;
	private TapConnectorContext connectorContext;

	public StreamReadFuncAspect connectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
		return this;
	}

	private List<String> tables;

	public StreamReadFuncAspect tables(List<String> tables) {
		this.tables = tables;
		return this;
	}

	private Object offsetState;

	public StreamReadFuncAspect offsetState(Object offsetState) {
		this.offsetState = offsetState;
		return this;
	}

	private int eventBatchSize;

	public StreamReadFuncAspect eventBatchSize(int eventBatchSize) {
		this.eventBatchSize = eventBatchSize;
		return this;
	}

	private List<Consumer<List<TapEvent>>> streamingReadCompleteConsumers = null;
	public StreamReadFuncAspect streamingReadCompleteConsumers(Consumer<List<TapEvent>> listConsumer) {
		if (null == streamingReadCompleteConsumers) {
			streamingReadCompleteConsumers = new CopyOnWriteArrayList<>();
		}
		streamingReadCompleteConsumers.add(tapEvents -> {
			try {
				listConsumer.accept(tapEvents);
			} catch(Throwable throwable) {
				TapLogger.warn(TAG, "Consume tapdataEvents from table {} failed on streaming read complete consumer {}, {}", tables, listConsumer, ExceptionUtils.getStackTrace(throwable));
			}
		});
		return this;
	}

	private List<Consumer<List<TapdataEvent>>> streamingProcessCompleteConsumers = null;
	public StreamReadFuncAspect streamingProcessCompleteConsumers(Consumer<List<TapdataEvent>> listConsumer) {
		if (null == streamingProcessCompleteConsumers) {
			streamingProcessCompleteConsumers = new CopyOnWriteArrayList<>();
		}
		streamingProcessCompleteConsumers.add(tapdataEvents -> {
			try {
				listConsumer.accept(tapdataEvents);
			} catch(Throwable throwable) {
				TapLogger.warn(TAG, "Consume tapdataEvents from table {} failed on streaming read complete consumer {}, {}", tables, listConsumer, ExceptionUtils.getStackTrace(throwable));
			}
		});
		return this;
	}

	private List<Consumer<List<TapdataEvent>>> streamingEnqueuedConsumers = null;
	public StreamReadFuncAspect streamingEnqueuedConsumers(Consumer<List<TapdataEvent>> listConsumer) {
		if (null == streamingEnqueuedConsumers) {
			streamingEnqueuedConsumers = new CopyOnWriteArrayList<>();
		}
		streamingEnqueuedConsumers.add(tapdataEvents -> {
			try {
				listConsumer.accept(tapdataEvents);
			} catch(Throwable throwable) {
				TapLogger.warn(TAG, "Consume tapdataEvents from table {} failed on streaming enqueued consumer {}, {}", tables, listConsumer, ExceptionUtils.getStackTrace(throwable));
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

	public Long getStreamStartedTime() {
		return streamStartedTime;
	}

	public void setStreamStartedTime(Long streamStartedTime) {
		this.streamStartedTime = streamStartedTime;
	}

	public List<String> getTables() {
		return tables;
	}

	public void setTables(List<String> tables) {
		this.tables = tables;
	}

	public List<Consumer<List<TapEvent>>> getStreamingReadCompleteConsumers() {
		return streamingReadCompleteConsumers;
	}

	public void setStreamingReadCompleteConsumers(List<Consumer<List<TapEvent>>> streamingReadCompleteConsumers) {
		this.streamingReadCompleteConsumers = streamingReadCompleteConsumers;
	}

	public List<Consumer<List<TapdataEvent>>> getStreamingProcessCompleteConsumers() {
		return streamingProcessCompleteConsumers;
	}

	public void setStreamingProcessCompleteConsumers(List<Consumer<List<TapdataEvent>>> streamingProcessCompleteConsumers) {
		this.streamingProcessCompleteConsumers = streamingProcessCompleteConsumers;
	}

	public List<Consumer<List<TapdataEvent>>> getStreamingEnqueuedConsumers() {
		return streamingEnqueuedConsumers;
	}

	public void setStreamingEnqueuedConsumers(List<Consumer<List<TapdataEvent>>> streamingEnqueuedConsumers) {
		this.streamingEnqueuedConsumers = streamingEnqueuedConsumers;
	}

	public String getStreamReadFunction() {
		return streamReadFunction;
	}

	public void setStreamReadFunction(String streamReadFunction) {
		this.streamReadFunction = streamReadFunction;
	}

	public StreamReadConsumer getStreamReadConsumer() {
		return streamReadConsumer;
	}

	public void setStreamReadConsumer(StreamReadConsumer streamReadConsumer) {
		this.streamReadConsumer = streamReadConsumer;
	}

	public boolean waitRawData() {
		synchronized (this) {
			try {
				this.wait();
			} catch (InterruptedException ignored) {
				TapLogger.debug(TAG, "waitRawData interrupted {}", ignored.getMessage());
			}
		}
		return waitRawData;
	}

	public void noMoreWaitRawData() {
		noMoreWaitRawData(null);
	}
	public void noMoreWaitRawData(Throwable errorDuringWait) {
		TapLogger.debug(TAG, "noMoreWaitRawData errorDuringWait {}", errorDuringWait);
		synchronized (this) {
			this.errorDuringWait = errorDuringWait;
			waitRawData = false;
			this.notifyAll();
		}
	}

	public Throwable getErrorDuringWait() {
		return errorDuringWait;
	}
}

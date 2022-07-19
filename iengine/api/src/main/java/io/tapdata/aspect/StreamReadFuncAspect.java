package io.tapdata.aspect;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;

public class StreamReadFuncAspect extends DataFunctionAspect<StreamReadFuncAspect> {
	private Long acceptTime;
	public StreamReadFuncAspect acceptTime(Long acceptTime) {
		this.acceptTime = acceptTime;
		return this;
	}
	private Long streamStartedTime;
	public StreamReadFuncAspect streamStartedTime(Long streamStartedTime) {
		this.streamStartedTime = streamStartedTime;
		return this;
	}
	public static final int STATE_STREAM_STARTED = 5;
	public static final int STATE_ACCEPT = 10;
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

	private List<TapdataEvent> events;

	public StreamReadFuncAspect events(List<TapdataEvent> events) {
		this.events = events;
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

	public List<TapdataEvent> getEvents() {
		return events;
	}

	public void setEvents(List<TapdataEvent> events) {
		this.events = events;
	}

	public Long getAcceptTime() {
		return acceptTime;
	}

	public void setAcceptTime(Long acceptTime) {
		this.acceptTime = acceptTime;
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
}

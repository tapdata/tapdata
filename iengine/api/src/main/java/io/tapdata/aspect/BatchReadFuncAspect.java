package io.tapdata.aspect;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;

public class BatchReadFuncAspect extends FunctionAspect<BatchReadFuncAspect> {
	private Long acceptTime;
	public BatchReadFuncAspect acceptTime(Long acceptTime) {
		this.acceptTime = acceptTime;
		return this;
	}
	private Long endTime;
	public BatchReadFuncAspect endTime(Long endTime) {
		this.endTime = endTime;
		return this;
	}
	public static final int STATE_START = 1;
	public static final int STATE_ACCEPT = 10;
	public static final int STATE_END = 20;
	private int state;
	public BatchReadFuncAspect state(int state) {
		this.state = state;
		return this;
	}
	private TapConnectorContext connectorContext;

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

	private List<TapdataEvent> events;

	public BatchReadFuncAspect events(List<TapdataEvent> events) {
		this.events = events;
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

	public List<TapdataEvent> getEvents() {
		return events;
	}

	public void setEvents(List<TapdataEvent> events) {
		this.events = events;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	public Long getAcceptTime() {
		return acceptTime;
	}

	public void setAcceptTime(Long acceptTime) {
		this.acceptTime = acceptTime;
	}

	public Long getEndTime() {
		return endTime;
	}

	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}
}

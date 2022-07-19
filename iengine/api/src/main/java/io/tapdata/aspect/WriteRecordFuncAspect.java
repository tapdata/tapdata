package io.tapdata.aspect;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.List;

public class WriteRecordFuncAspect extends DataFunctionAspect<WriteRecordFuncAspect> {
	private WriteListResult<TapRecordEvent> writeListResult;
	public WriteRecordFuncAspect writeListResult(WriteListResult<TapRecordEvent> writeListResult) {
		this.writeListResult = writeListResult;
		return this;
	}
	public static final int STATE_WRITE_RESULT = 10;
	private Long writeResultTime;
	public WriteRecordFuncAspect writeResultTime(Long writeResultTime) {
		this.writeResultTime = writeResultTime;
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

	public WriteListResult<TapRecordEvent> getWriteListResult() {
		return writeListResult;
	}

	public void setWriteListResult(WriteListResult<TapRecordEvent> writeListResult) {
		this.writeListResult = writeListResult;
	}

	public Long getWriteResultTime() {
		return writeResultTime;
	}

	public void setWriteResultTime(Long writeResultTime) {
		this.writeResultTime = writeResultTime;
	}
}

package io.tapdata.aspect;

import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public class ClearTableFuncAspect extends FunctionAspect<ClearTableFuncAspect> {
	private TapClearTableEvent clearTableEvent;
	public ClearTableFuncAspect clearTableEvent(TapClearTableEvent clearTableEvent) {
		this.clearTableEvent = clearTableEvent;
		return this;
	}
	private TapConnectorContext connectorContext;

	public ClearTableFuncAspect connectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
		return this;
	}

	public TapConnectorContext getConnectorContext() {
		return connectorContext;
	}

	public void setConnectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
	}

	public TapClearTableEvent getClearTableEvent() {
		return clearTableEvent;
	}

	public void setClearTableEvent(TapClearTableEvent clearTableEvent) {
		this.clearTableEvent = clearTableEvent;
	}
}

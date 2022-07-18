package io.tapdata.aspect;

import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public class CreateTableFuncAspect extends FunctionAspect<CreateTableFuncAspect> {
	private TapCreateTableEvent createTableEvent;
	public CreateTableFuncAspect createTableEvent(TapCreateTableEvent createTableEvent) {
		this.createTableEvent = createTableEvent;
		return this;
	}
	private TapConnectorContext connectorContext;

	public CreateTableFuncAspect connectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
		return this;
	}

	public TapConnectorContext getConnectorContext() {
		return connectorContext;
	}

	public void setConnectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
	}

	public TapCreateTableEvent getCreateTableEvent() {
		return createTableEvent;
	}

	public void setCreateTableEvent(TapCreateTableEvent createTableEvent) {
		this.createTableEvent = createTableEvent;
	}
}

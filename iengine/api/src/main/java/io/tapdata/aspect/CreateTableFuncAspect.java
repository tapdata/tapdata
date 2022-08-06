package io.tapdata.aspect;

import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;

public class CreateTableFuncAspect extends DataFunctionAspect<CreateTableFuncAspect> {
	private CreateTableOptions createTableOptions;
	public CreateTableFuncAspect createTableOptions(CreateTableOptions createTableOptions) {
		this.createTableOptions = createTableOptions;
		return this;
	}
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

	public CreateTableOptions getCreateTableOptions() {
		return createTableOptions;
	}

	public void setCreateTableOptions(CreateTableOptions createTableOptions) {
		this.createTableOptions = createTableOptions;
	}
}

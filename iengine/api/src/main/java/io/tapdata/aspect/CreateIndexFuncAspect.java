package io.tapdata.aspect;

import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public class CreateIndexFuncAspect extends DataFunctionAspect<CreateIndexFuncAspect> {
	private TapCreateIndexEvent createIndexEvent;
	public CreateIndexFuncAspect createIndexEvent(TapCreateIndexEvent createIndexEvent) {
		this.createIndexEvent = createIndexEvent;
		return this;
	}
	private TapConnectorContext connectorContext;

	public CreateIndexFuncAspect connectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
		return this;
	}

	private TapTable table;

	public CreateIndexFuncAspect table(TapTable table) {
		this.table = table;
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

	public TapCreateIndexEvent getCreateIndexEvent() {
		return createIndexEvent;
	}

	public void setCreateIndexEvent(TapCreateIndexEvent createIndexEvent) {
		this.createIndexEvent = createIndexEvent;
	}
}

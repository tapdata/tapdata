package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;

public interface CreateTableV2Function extends TapConnectorFunction {
    CreateTableOptions createTable(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws Throwable;
}

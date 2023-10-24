package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

@Deprecated
public interface CreateTableFunction extends TapConnectorFunction {
    void createTable(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws Throwable;
}

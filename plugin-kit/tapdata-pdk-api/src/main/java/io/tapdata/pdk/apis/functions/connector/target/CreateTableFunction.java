package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface CreateTableFunction extends TapFunction {
    void createTable(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws Throwable;
}

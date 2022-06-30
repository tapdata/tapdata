package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface DropTableFunction extends TapFunction {
    void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) throws Throwable;
}

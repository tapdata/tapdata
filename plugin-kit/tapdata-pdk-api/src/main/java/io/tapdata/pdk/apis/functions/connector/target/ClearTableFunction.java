package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface ClearTableFunction extends TapFunction {
    void clearTable(TapConnectorContext connectorContext, TapClearTableEvent clearTableEvent) throws Throwable;
}

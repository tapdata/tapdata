package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface DropFieldFunction extends TapConnectorFunction {
    void dropField(TapConnectorContext connectorContext, TapDropFieldEvent dropFieldEvent) throws Throwable;
}

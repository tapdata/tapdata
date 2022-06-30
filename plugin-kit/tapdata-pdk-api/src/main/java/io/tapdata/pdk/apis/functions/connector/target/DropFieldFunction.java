package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface DropFieldFunction {
    void dropField(TapConnectorContext connectorContext, TapDropFieldEvent dropFieldEvent) throws Throwable;
}

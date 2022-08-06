package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface NewFieldFunction extends TapConnectorFunction {
    void newField(TapConnectorContext connectorContext, TapNewFieldEvent newFieldEvent) throws Throwable;
}

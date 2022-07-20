package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface NewFieldFunction extends TapFunction {
    void newField(TapConnectorContext connectorContext, TapNewFieldEvent newFieldEvent) throws Throwable;
}

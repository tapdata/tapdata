package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface NewFieldFunction {
    void newField(TapConnectorContext connectorContext, TapNewFieldEvent newFieldEvent) throws Throwable;
}

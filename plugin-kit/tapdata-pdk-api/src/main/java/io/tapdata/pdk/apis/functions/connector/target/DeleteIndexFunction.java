package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.index.TapDeleteIndexEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface DeleteIndexFunction extends TapConnectorFunction {
    void deleteIndex(TapConnectorContext connectorContext, TapTable table, TapDeleteIndexEvent deleteIndexEvent) throws Throwable;
}

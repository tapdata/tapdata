package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface CreateIndexFunction extends TapConnectorFunction {
    void createIndex(TapConnectorContext connectorContext, TapTable table, TapCreateIndexEvent createIndexEvent) throws Throwable;
}

package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;
import java.util.function.Consumer;

public interface QueryIndexesFunction extends TapConnectorFunction {
    void query(TapConnectorContext connectorContext, TapTable table, Consumer<List<TapIndex>> consumer) throws Throwable;
}

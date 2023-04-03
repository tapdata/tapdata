package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;

import java.util.List;
import java.util.function.Consumer;

public interface RunRawCommandFunction extends TapConnectorFunction {

    void run(TapConnectorContext connectorContext, String command, TapTable table, int eventBatchSize, Consumer<List<TapEvent>> eventsOffsetConsumer) throws Throwable;
}



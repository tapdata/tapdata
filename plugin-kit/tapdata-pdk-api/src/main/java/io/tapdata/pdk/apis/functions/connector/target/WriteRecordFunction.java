package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;
import java.util.function.Consumer;

public interface WriteRecordFunction extends TapConnectorFunction {

    /**
     * insert, update, delete events.
     *
     * @param connectorContext
     * @param recordEvents
     * @param consumer
     */
    void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> recordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable;

}

package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;
import java.util.function.Consumer;

public interface TransactionFunction extends TapFunction {

    /**
     * insert, update, delete events.
     *
     * @param nodeContext
     * @param recordEvents
     * @param consumer
     */
    void executeTransaction(TapProcessorContext nodeContext, List<TapRecordEvent> recordEvents, Consumer<List<TapRecordEvent>> consumer) throws Throwable;

}

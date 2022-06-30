package io.tapdata.pdk.apis.functions.processor;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.context.TapProcessorContext;

import java.util.List;
import java.util.function.Consumer;

/**
 * Process every record
 */
public interface ProcessRecordFunction {
    void process(TapProcessorContext tapProcessorContext, List<TapEvent> recordEvents, Consumer<List<TapEvent>> consumer) throws Throwable;
}

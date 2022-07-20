package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;
import java.util.function.Consumer;

/**
 * Will be used when upsert function not implemented. for query update/insert model
 *
 */
public interface QueryByFilterFunction extends TapConnectorFunction {
    void query(TapConnectorContext nodeContext, List<TapFilter> filters, TapTable table, Consumer<List<FilterResult>> consumer) throws Throwable;
}

package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapQueryStringFilter;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;

import java.util.List;
import java.util.function.Function;

public interface ExecuteQueryStringFunction extends TapConnectorFunction {
    /**
     * @param connectorContext the node context in a DAG
     */
    void execute(TapConnectorContext connectorContext, TapQueryStringFilter queryStringFilter, Function<List<TapEvent>, Boolean> function) throws Throwable;
}

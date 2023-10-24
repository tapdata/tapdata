package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;

public interface TableCountFunction extends TapConnectorFunction {
    /**
     * @param nodeContext the node context in a DAG
     */
    long count(TapConnectorContext nodeContext) throws Throwable;
}

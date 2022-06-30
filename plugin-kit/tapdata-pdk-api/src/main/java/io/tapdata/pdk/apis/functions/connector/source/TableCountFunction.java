package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface TableCountFunction extends TapFunction {
    /**
     * @param nodeContext the node context in a DAG
     */
    long count(TapConnectorContext nodeContext) throws Throwable;
}

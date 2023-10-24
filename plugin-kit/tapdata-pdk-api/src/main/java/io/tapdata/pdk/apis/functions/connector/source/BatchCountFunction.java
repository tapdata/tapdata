package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;

public interface BatchCountFunction extends TapConnectorFunction {
    /**
     * @param nodeContext the node context in a DAG
     * @param table the table to count
     */
    long count(TapConnectorContext nodeContext, TapTable table) throws Throwable;
}

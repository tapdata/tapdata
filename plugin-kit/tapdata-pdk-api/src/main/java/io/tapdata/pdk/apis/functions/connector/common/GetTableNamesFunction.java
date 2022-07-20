package io.tapdata.pdk.apis.functions.connector.common;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;
import java.util.function.Consumer;

public interface GetTableNamesFunction extends TapFunction {
    /**
     * @param nodeContext the node context in a DAG
     */
    void tableNames(TapConnectorContext nodeContext, int batchSize, Consumer<List<String>> consumer) throws Throwable;
}

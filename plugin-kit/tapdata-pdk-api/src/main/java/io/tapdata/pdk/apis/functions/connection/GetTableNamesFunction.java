package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectionFunction;

import java.util.List;
import java.util.function.Consumer;

public interface GetTableNamesFunction extends TapConnectionFunction {
    /**
     * @param nodeContext the node context in a DAG
     */
    void tableNames(TapConnectionContext nodeContext, int batchSize, Consumer<List<String>> consumer) throws Throwable;
}

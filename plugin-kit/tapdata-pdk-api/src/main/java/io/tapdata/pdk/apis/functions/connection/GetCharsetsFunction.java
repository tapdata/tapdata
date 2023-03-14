package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectionFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;
import java.util.function.Consumer;

public interface GetCharsetsFunction extends TapConnectionFunction {
    /**
     * @param nodeContext the node context in a DAG
     */
    CharsetResult charsets(TapConnectionContext nodeContext) throws Throwable;
}

package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface ErrorHandleFunction extends TapFunction {
    /**
     * @param nodeContext the node context in a DAG
     */
    RetryOptions needRetry(TapConnectionContext nodeContext, PDKMethod method, Throwable throwable) throws Throwable;
}

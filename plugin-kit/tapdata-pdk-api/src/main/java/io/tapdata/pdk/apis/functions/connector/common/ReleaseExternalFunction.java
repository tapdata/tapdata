package io.tapdata.pdk.apis.functions.connector.common;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;

public interface ReleaseExternalFunction extends TapConnectorFunction {
    void release(TapConnectorContext connectorContext) throws Throwable;
}

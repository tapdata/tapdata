package io.tapdata.pdk.apis.functions.connector.common;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface ReleaseExternalFunction extends TapConnectorFunction {
    void release(TapConnectorContext connectorContext) throws Throwable;
}

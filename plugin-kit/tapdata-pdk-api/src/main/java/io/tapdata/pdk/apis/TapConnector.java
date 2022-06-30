package io.tapdata.pdk.apis;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

public interface TapConnector extends TapConnectorNode {
    void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry);
}

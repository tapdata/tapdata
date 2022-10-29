package io.tapdata.pdk.core.api;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.TapConnectorNode;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectionFunctions;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.function.Consumer;

public class ConnectionNode extends Node {
    TapConnector connector;
    TapConnectionContext connectionContext;

    ConnectionFunctions<?> connectionFunctions;

    public void discoverSchema(List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        connector.discoverSchema(connectionContext, tables, tableSize, consumer);
    }
    public int tableCount() throws Throwable {
        return connector.tableCount(connectionContext);
    }
    public ConnectionOptions connectionTest(Consumer<TestItem> consumer) throws Throwable {
        return connector.connectionTest(connectionContext, consumer);
    }
    public void connectorInit() throws Throwable {
        connector.init(connectionContext);
    }

    public void connectorStop() throws Throwable {
        try {
            connector.stop(connectionContext);
        } finally {
            PDKIntegration.unregisterMemoryFetcher(id() + "_" + associateId);
        }
    }

    public TapConnector getConnector() {
        return connector;
    }

    public TapConnectionContext getConnectionContext() {
        return connectionContext;
    }

    public ConnectionFunctions<?> getConnectionFunctions() {
        return connectionFunctions;
    }

    public void init(TapConnector tapNode) {
        connector = tapNode;
        connectionFunctions = new ConnectorFunctions();
    }

    public void registerCapabilities() {
        connector.registerCapabilities((ConnectorFunctions) connectionFunctions, new TapCodecsRegistry());
    }
}

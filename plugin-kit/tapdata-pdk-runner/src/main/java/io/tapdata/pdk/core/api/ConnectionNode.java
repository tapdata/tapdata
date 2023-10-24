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
import io.tapdata.pdk.apis.functions.common.MemoryFetcherFunctionV2;

import java.util.List;
import java.util.function.Consumer;

public class ConnectionNode extends Node {
    TapConnector connector;
    TapConnectionContext connectionContext;

    ConnectionFunctions<?> connectionFunctions;

    public void registerMemoryFetcher() {
        MemoryFetcherFunctionV2 memoryFetcherFunctionV2 = connectionFunctions.getMemoryFetcherFunctionV2();
        if(memoryFetcherFunctionV2 != null)
            PDKIntegration.registerMemoryFetcher(id() + "_" + associateId, memoryFetcherFunctionV2::memory);
    }

    public void unregisterMemoryFetcher() {
        PDKIntegration.unregisterMemoryFetcher(id() + "_" + associateId);
    }

    public void discoverSchema(List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        connector.discoverSchema(connectionContext, tables, tableSize, consumer);
    }
    public int tableCount() throws Throwable {
        return connector.tableCount(connectionContext);
    }

    public ConnectionOptions connectionTest(Consumer<TestItem> consumer) throws Throwable {
        String gitBuildTime = connectionContext.getSpecification().getManifest().get("Git-Build-Time");
        consumer.accept(new TestItem(PDK_VERSION_TEST, TestItem.RESULT_SUCCESSFULLY,
                String.format(PDK_VERSION_MESSAGE, "",
                        gitBuildTime.substring(0, gitBuildTime.length() - 5).replace("T", " "))));
        return connector.connectionTest(connectionContext, consumer);
    }

    private static final String PDK_VERSION_TEST = "PDK Connector version";
    private static final String PDK_VERSION_MESSAGE = "%s (build: %s)";

    public void connectorInit() throws Throwable {
        connector.init(connectionContext);
    }

    public void connectorStop() throws Throwable {
        try {
            connector.stop(connectionContext);
        } finally {
            unregisterMemoryFetcher();
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

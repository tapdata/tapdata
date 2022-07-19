package io.tapdata.connector.rabbitmq;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.rabbitmq.config.RabbitmqConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.function.Consumer;

public class RabbitmqConnector extends ConnectorBase {

    private RabbitmqService rabbitmqService;
    private RabbitmqConfig rabbitmqConfig;

    private void initConnection(TapConnectionContext connectorContext) {
        rabbitmqConfig = (RabbitmqConfig) new RabbitmqConfig().load(connectorContext.getConnectionConfig());
        rabbitmqService = new RabbitmqService(rabbitmqConfig);
        rabbitmqService.init();
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        rabbitmqService.close();
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {

    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {

    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        rabbitmqConfig = (RabbitmqConfig) new RabbitmqConfig().load(connectionContext.getConnectionConfig());
        RabbitmqService rabbitmqService = new RabbitmqService(rabbitmqConfig);
        rabbitmqService.testConnection(consumer);
        rabbitmqService.close();
        return null;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return 0;
    }
}

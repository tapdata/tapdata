package io.tapdata.connector.rabbitmq;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.rabbitmq.config.RabbitmqConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec_rabbitmq.json")
public class RabbitmqConnector extends ConnectorBase {

    private RabbitmqService rabbitmqService;
    private RabbitmqConfig rabbitmqConfig;

    private void initConnection(TapConnectionContext connectorContext) throws Throwable {
        rabbitmqConfig = (RabbitmqConfig) new RabbitmqConfig().load(connectorContext.getConnectionConfig());
        rabbitmqService = new RabbitmqService(rabbitmqConfig);
        rabbitmqService.init();
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        rabbitmqService.close();
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "STRING", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportConnectionCheckFunction(this::checkConnection);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        rabbitmqService.loadTables(tableSize, consumer);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        rabbitmqConfig = (RabbitmqConfig) new RabbitmqConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(rabbitmqConfig.getConnectionString());
        try (
                RabbitmqService rabbitmqService = new RabbitmqService(rabbitmqConfig)
        ) {
            TestItem testHostAndPort = rabbitmqService.testHostAndPort();
            consumer.accept(testHostAndPort);
            if (testHostAndPort.getResult() == TestItem.RESULT_FAILED) {
                return connectionOptions;
            }
            TestItem testConnect = rabbitmqService.testConnect();
            consumer.accept(testConnect);
            if (testConnect.getResult() == TestItem.RESULT_FAILED) {
                return connectionOptions;
            } else {
                consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));
            }
        }
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return rabbitmqService.countTables();
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        rabbitmqService.produce(tapRecordEvents, tapTable, writeListResultConsumer, this::isAlive);
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        rabbitmqService.consumeOne(tapTable, eventBatchSize, eventsOffsetConsumer);
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        rabbitmqService.streamConsume(tableList, recordSize, consumer);
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        return TapSimplify.list();
    }

    private void checkConnection(TapConnectionContext connectionContext, List<String> items, Consumer<ConnectionCheckItem> consumer) {
        ConnectionCheckItem testPing = rabbitmqService.testPing();
        consumer.accept(testPing);
        if (testPing.getResult() == ConnectionCheckItem.RESULT_FAILED) {
            return;
        }
        ConnectionCheckItem testConnection = rabbitmqService.testConnection();
        consumer.accept(testConnection);
    }
}

package io.tapdata.connector.kafka;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
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
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.tapdata.connector.kafka.util.SchemaRegisterUtil.*;
import static io.tapdata.pdk.apis.entity.ConnectionOptions.*;

@TapConnectorClass("spec_kafka.json")
public class KafkaConnector extends ConnectorBase {
    public static final String TAG = KafkaConnector.class.getSimpleName();

    private KafkaService kafkaService;
    private KafkaSRService kafkaSRService;
    private KafkaConfig kafkaConfig;
    private Boolean isSchemaRegister;

    private void initConnection(TapConnectionContext connectorContext) {
        kafkaConfig = (KafkaConfig) new KafkaConfig().load(connectorContext.getConnectionConfig());
        this.isSchemaRegister = kafkaConfig.getSchemaRegister();
        kafkaService = new KafkaService(kafkaConfig);
        kafkaService.setTapLogger(connectorContext.getLog());
        kafkaService.setConnectorId(connectorContext.getId());
        kafkaService.init();
        if (this.isSchemaRegister) {
            kafkaSRService = new KafkaSRService(kafkaConfig, connectorContext);
            kafkaSRService.setTapLogger(connectorContext.getLog());
            kafkaSRService.setConnectorId(connectorContext.getId());
            kafkaSRService.init();
        }
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        kafkaService.close();
        if (this.isSchemaRegister) {
            kafkaSRService.close();
        }
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

        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
    }

    private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws ExecutionException, InterruptedException, IOException {
        TapTable tapTable = tapCreateTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        if (checkTopicExists(kafkaConfig.getConnectionString(), tapTable.getId())) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }
        try {
            if (this.isSchemaRegister) {
                int numPartitions = 3;
                short replicationFactor = 1;
                Properties properties = new Properties();
                properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getConnectionString());
                AdminClient adminClient = AdminClient.create(properties);
                NewTopic newTopic = new NewTopic(tapTable.getId(), numPartitions, replicationFactor);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) {
        kafkaService.produce(tapFieldBaseEvent);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        if (!this.isSchemaRegister) {
            kafkaService.loadTables(tableSize, consumer);
        } else {
            kafkaSRService.loadTables(tableSize, consumer);
        }
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        kafkaConfig = (KafkaConfig) new KafkaConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(kafkaConfig.getConnectionString());
        try {
            onStart(connectionContext);
            CommonDbConfig config = new CommonDbConfig();
            config.set__connectionType(kafkaConfig.get__connectionType());
            KafkaTest kafkaTest = new KafkaTest(kafkaConfig, consumer, this.kafkaService, config, isSchemaRegister, kafkaSRService);
            kafkaTest.testOneByOne();
        } catch (Throwable throwable) {
            TapLogger.error(TAG, throwable.getMessage());
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Failed, " + throwable.getMessage()));
        } finally {
            onStop(connectionContext);
        }
        List<Capability> ddlCapabilities = Arrays.asList(
                Capability.create(DDL_NEW_FIELD_EVENT).type(Capability.TYPE_DDL),
                Capability.create(DDL_ALTER_FIELD_NAME_EVENT).type(Capability.TYPE_DDL),
                Capability.create(DDL_ALTER_FIELD_ATTRIBUTES_EVENT).type(Capability.TYPE_DDL),
                Capability.create(DDL_DROP_FIELD_EVENT).type(Capability.TYPE_DDL));
        ddlCapabilities.forEach(connectionOptions::capability);
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return kafkaService.countTables();
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        if (!this.isSchemaRegister) {
            kafkaService.produce(tapRecordEvents, tapTable, writeListResultConsumer, this::isAlive);
        } else {
            kafkaSRService.produce(tapRecordEvents, tapTable, writeListResultConsumer, this::isAlive);
        }
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        kafkaService.consumeOne(tapTable, eventBatchSize, eventsOffsetConsumer);
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        kafkaService.streamConsume(tableList, recordSize, consumer);
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        return TapSimplify.list();
    }

    private void checkConnection(TapConnectionContext connectionContext, List<String> items, Consumer<ConnectionCheckItem> consumer) {
        ConnectionCheckItem testPing = kafkaService.testPing();
        consumer.accept(testPing);
        if (testPing.getResult() == ConnectionCheckItem.RESULT_FAILED) {
            return;
        }
        ConnectionCheckItem testConnection = kafkaService.testConnection();
        consumer.accept(testConnection);
    }

}

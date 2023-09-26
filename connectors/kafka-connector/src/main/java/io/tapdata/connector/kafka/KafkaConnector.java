package io.tapdata.connector.kafka;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.kafka.admin.Admin;
import io.tapdata.connector.kafka.admin.DefaultAdmin;
import io.tapdata.connector.kafka.config.AdminConfiguration;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
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
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
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
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.tapdata.pdk.apis.entity.ConnectionOptions.*;

@TapConnectorClass("spec_kafka.json")
public class KafkaConnector extends ConnectorBase {
    public static final String TAG = KafkaConnector.class.getSimpleName();

    private KafkaService kafkaService;
    private KafkaConfig kafkaConfig;

    private void initConnection(TapConnectionContext connectorContext) {
        kafkaConfig = (KafkaConfig) new KafkaConfig().load(connectorContext.getConnectionConfig());
        kafkaService = new KafkaService(kafkaConfig, connectorContext.getLog());
        kafkaService.setConnectorId(connectorContext.getId());
        kafkaService.init();
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        if (kafkaService != null) {
            kafkaService.close();
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
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);

        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
    }

    private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws Exception {
        DataMap nodeConfig = tapConnectorContext.getNodeConfig();
        Integer replicasSize = (Integer) nodeConfig.get("ReplicasSize");
        Integer partitionNum = (Integer) nodeConfig.get("partitionNum");
        CreateTableOptions createTableOptions = new CreateTableOptions();
        AdminConfiguration configuration = new AdminConfiguration(kafkaConfig, tapConnectorContext.getId());
        String nameSrvAddr = kafkaConfig.getNameSrvAddr();
        Admin admin = new DefaultAdmin(configuration);
        Set<String> existTopics = admin.listTopics();
        if (!existTopics.contains(tapCreateTableEvent.getTableId())) {
            String[] nameSrvAddrs = nameSrvAddr.split(",");
            if (nameSrvAddrs.length < replicasSize) {
                throw new RuntimeException(String.format(TAG,"Cluster size is {} ,can not create topic replicasSize is {} ", String.valueOf(nameSrvAddrs.length), replicasSize.toString()));
            }
            admin.createTopics(tapCreateTableEvent.getTableId(), partitionNum, replicasSize.shortValue());
        } else {
            List<TopicPartitionInfo> topicPartitionInfos = admin.getTopicPartitionInfo(tapCreateTableEvent.getTableId());
            int existTopicPartition = topicPartitionInfos.size();
            int existReplicasSize = topicPartitionInfos.get(0).replicas().size();
            if (existReplicasSize != replicasSize) {
                TapLogger.warn(TAG,"Topic {} ReplicasSize is {} , can not resize", tapCreateTableEvent.getTableId(), existReplicasSize);
            }
            if (partitionNum < existTopicPartition) {
                TapLogger.warn(TAG,"Topic {} partitionNum is {} , can not lower than original one", tapCreateTableEvent.getTableId(), existTopicPartition);
            }
            admin.addTopicPartitions(tapCreateTableEvent.getTableId(), partitionNum);
        }
        admin.close();

        return createTableOptions;
    }

    private void fieldDDLHandler(TapConnectorContext tapConnectorContext, TapFieldBaseEvent tapFieldBaseEvent) {
        kafkaService.produce(tapFieldBaseEvent);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        kafkaService.loadTables(tableSize, consumer);
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
            KafkaTest kafkaTest = new KafkaTest(kafkaConfig, consumer, kafkaService, config);
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
        String enableScript = connectorContext.getNodeConfig().getString("enableScript");
        if ("true".equals(enableScript)) {
            kafkaService.produce(connectorContext, tapRecordEvents, tapTable, writeListResultConsumer, this::isAlive);
        } else {
            kafkaService.produce(tapRecordEvents, tapTable, writeListResultConsumer, this::isAlive);
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

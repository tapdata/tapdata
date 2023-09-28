package io.tapdata.connector.kafka;

import com.google.common.collect.Lists;
import io.tapdata.common.constant.MqOp;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.connector.kafka.config.SchemaConfiguration;
import io.tapdata.connector.kafka.util.Krb5Util;
import io.tapdata.connector.kafka.util.SchemaRegisterUtil;
import io.tapdata.constant.MqTestItem;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import okhttp3.Response;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.tapdata.base.ConnectorBase.table;
import static io.tapdata.connector.kafka.util.SchemaRegisterUtil.parseJsonArray;

/**
 * Author:Skeet
 * Date: 2023/6/29
 **/
public class KafkaSRService extends KafkaService {
    private KafkaConfig kafkaConfig;
    private String connectorId;
    private KafkaProducer<String, GenericRecord> kafkaProducer;
    private TapConnectionContext tapConnectionContext;
    private Boolean isBasicAuth;

    public KafkaSRService() {
        super();
    }

    public KafkaSRService(KafkaConfig kafkaConfig, TapConnectionContext connectionContext, KafkaProducer<String, GenericRecord> kafkaProducer) {
        this.kafkaConfig = kafkaConfig;
        this.isBasicAuth = kafkaConfig.getBasicAuth();
        this.tapConnectionContext = connectionContext;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void setConnectorId(String connectorId) {
        this.connectorId = connectorId;
    }

    @Override
    public String getConnectorId() {
        return super.getConnectorId();
    }

    @Override
    protected <T> Map<String, Object> analyzeTable(Object object, T topic, TapTable tapTable) {
        return super.analyzeTable(object, topic, tapTable);
    }

    @Override
    public TestItem testConnect() {
        if ((kafkaConfig).getKrb5()) {
            try {
                Krb5Util.checkKDCDomainsBase64((kafkaConfig).getKrb5Conf());
                return new TestItem(MqTestItem.KAFKA_BASE64_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null);
            } catch (Exception e) {
                return new TestItem(MqTestItem.KAFKA_BASE64_CONNECTION.getContent(), TestItem.RESULT_FAILED, e.getMessage());
            }
        }
        String[] schemaRegisterUrls = kafkaConfig.getSchemaRegisterUrl().split(",");
        try {
            if (kafkaConfig.getBasicAuth()) {
                for (String schemaRegisterUrl : schemaRegisterUrls) {
                    Response reschemaRegisterResponse = SchemaRegisterUtil.sendBasicAuthRequest("http://" + schemaRegisterUrl + "/subjects",
                            kafkaConfig.getAuthUserName(),
                            kafkaConfig.getAuthPassword());
                    if (reschemaRegisterResponse.code() != 200) {
                        return new TestItem(MqTestItem.KAFKA_SCHEMA_REGISTER_CONNECTION.getContent(), TestItem.RESULT_FAILED, reschemaRegisterResponse.toString());
                    }
                }
                return new TestItem(MqTestItem.KAFKA_SCHEMA_REGISTER_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null);
            } else {
                for (String schemaRegisterUrl : schemaRegisterUrls) {
                    Response reschemaRegisterResponse = SchemaRegisterUtil.sendBasicAuthRequest("http://" + schemaRegisterUrl + "/subjects",
                            null,
                            null);
                    if (reschemaRegisterResponse.code() != 200) {
                        return new TestItem(MqTestItem.KAFKA_SCHEMA_REGISTER_CONNECTION.getContent(), TestItem.RESULT_FAILED, reschemaRegisterResponse.toString());
                    }
                }
                return new TestItem(MqTestItem.KAFKA_SCHEMA_REGISTER_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new TestItem(MqTestItem.KAFKA_SCHEMA_REGISTER_CONNECTION.getContent(), TestItem.RESULT_FAILED, "Please check the service address. " + e.getMessage());
        }
    }

    @Override
    public ConnectionCheckItem testConnection() {
        return super.testConnection();
    }

    @Override
    public void init() {
        super.init();
    }

    @Override
    public int countTables() throws Throwable {
        return super.countTables();
    }

    @Override
    public void loadTables(int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        BufferedReader reader = null;
        HttpURLConnection connection = null;
        String schemaRegistryUrl = "http://" + kafkaConfig.getSchemaRegisterUrl() + "/subjects";

        try {
            URL url = new URL(schemaRegistryUrl);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                response.append(line);
            }

            List<String> tableList = parseJsonArray(response.toString());
            List<TapTable> tapTableList = TapSimplify.list();
            List<String> list = new ArrayList<>();
            if (tableList.size() > 0) {
                for (String s : tableList) {
                    if (!list.contains(s))
                        list.add(s);
                }
                for (String k : list) {
                    tapTableList.add(table(k));
                }
            }
            consumer.accept(tapTableList);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    @Override
    protected void submitPageTables(int tableSize, Consumer<List<TapTable>> consumer, SchemaConfiguration schemaConfiguration, Set<String> destinationSet) {
        super.submitPageTables(tableSize, consumer, schemaConfiguration, destinationSet);
    }

    @Override
    public void produce(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) {
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);

        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        CountDownLatch countDownLatch = new CountDownLatch(tapRecordEvents.size());
        try {
            for (TapRecordEvent event : tapRecordEvents) {
                if (null != isAlive && !isAlive.get()) {
                    break;
                }
                Map<String, Object> data;
                MqOp mqOp = MqOp.INSERT;
                if (event instanceof TapInsertRecordEvent) {
                    data = ((TapInsertRecordEvent) event).getAfter();
                } else if (event instanceof TapUpdateRecordEvent) {
                    data = ((TapUpdateRecordEvent) event).getAfter();
                    mqOp = MqOp.UPDATE;
                } else if (event instanceof TapDeleteRecordEvent) {
                    data = ((TapDeleteRecordEvent) event).getBefore();
                    mqOp = MqOp.DELETE;
                } else {
                    data = new HashMap<>();
                }

                SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(tapTable.getId());
                SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();

                final List<String> fieldStrs = Lists.newArrayList();
                final Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
                for (final String columnName : nameFieldMap.keySet()) {
                    final String columnType = nameFieldMap.get(columnName).getDataType();
                    if (StringUtils.isBlank(columnType)) {
                        continue;
                    }
                    // 根据列的类型映射为 Avro 模式的类型
                    Schema.Field field = createAvroField(columnName, columnType);
                    fieldAssembler.name(columnName).type(field.schema()).noDefault();
                }
                Schema.Parser parser = new Schema.Parser();
                Schema avroSchema = parser.parse(fieldAssembler.endRecord().toString());
                GenericRecord record = new GenericData.Record(avroSchema);
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    String fieldName = entry.getKey();
                    Object value = entry.getValue();
                    record.put(fieldName, value);
                }

                MqOp finalMqOp = mqOp;
                Callback callback = (metadata, exception) -> {
                    try {
                        if (EmptyKit.isNotNull(exception)) {
                            listResult.addError(event, exception);
                        }
                        switch (finalMqOp) {
                            case INSERT:
                                insert.incrementAndGet();
                                break;
                            case UPDATE:
                                update.incrementAndGet();
                                break;
                            case DELETE:
                                delete.incrementAndGet();
                                break;
                        }
                    } finally {
                        countDownLatch.countDown();
                    }
                };
                ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(
                        tapTable.getId(),
                        record
                );
                kafkaProducer.send(producerRecord, callback);
            }
        } catch (RejectedExecutionException e) {
            tapLogger.warn("task stopped, some data produce failed!", e);
        } catch (Exception e) {
            tapLogger.error("produce error, or task interrupted!", e);
        }
        try {
            while (null != isAlive && isAlive.get()) {
                if (countDownLatch.await(500L, TimeUnit.MILLISECONDS)) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            tapLogger.error("error occur when await", e);
        } finally {
            writeListResultConsumer.accept(listResult.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
        }
    }

    private static Schema.Field createAvroField(String columnName, String columnType) {
        Schema avroType;
        switch (columnType) {
            case "BOOLEAN":
                avroType = SchemaBuilder.builder().booleanType();
                break;
            case "NUMBER":
                avroType = SchemaBuilder.builder().doubleType();
                break;
            case "INTEGER":
                avroType = SchemaBuilder.builder().longType();
                break;
            case "STRING":
            case "ARRAY":
            case "TEXT":
            default:
                avroType = SchemaBuilder.builder().stringType();
                break;
        }

        return new Schema.Field(columnName, avroType, null, null);
    }

    @Override
    public void produce(TapFieldBaseEvent tapFieldBaseEvent) {
        super.produce(tapFieldBaseEvent);
    }

    @Override
    public void consumeOne(TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        throw new CoreException("The schemaRegister function is not supported as the source for the time being. ");
    }

    @Override
    public void streamConsume(List<String> tableList, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        throw new CoreException("The schemaRegister function is not supported as the source for the time being. ");
    }

    @Override
    public void close() {
        super.close();
        if (EmptyKit.isNotNull(kafkaProducer)) {
            kafkaProducer.close();
        }
    }

    @Override
    public void setTapLogger(Log tapLogger) {
        super.setTapLogger(tapLogger);
    }

    @Override
    public TestItem testHostAndPort() {
        return super.testHostAndPort();
    }

    @Override
    public ConnectionCheckItem testPing() {
        return super.testPing();
    }

    @Override
    protected <T> void submitTables(int tableSize, Consumer<List<TapTable>> consumer, Object object, Set<T> destinationSet) {
        super.submitTables(tableSize, consumer, object, destinationSet);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }
}

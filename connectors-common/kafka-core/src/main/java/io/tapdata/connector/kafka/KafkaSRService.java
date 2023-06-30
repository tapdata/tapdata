package io.tapdata.connector.kafka;

import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.constant.MqOp;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.connector.kafka.config.ProducerConfiguration;
import io.tapdata.connector.kafka.config.SchemaConfiguration;
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
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

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
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.table;

/**
 * Author:Skeet
 * Date: 2023/6/29
 **/
public class KafkaSRService extends KafkaService {
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
    private KafkaConfig kafkaConfig;
    private String connectorId;
    private KafkaProducer<byte[], GenericRecord> kafkaProducer;
    private TapConnectionContext tapConnectionContext;


    public KafkaSRService() {
        super();
    }

    public KafkaSRService(KafkaConfig mqConfig, TapConnectionContext connectionContext) {
        this.mqConfig = mqConfig;
        this.tapConnectionContext = connectionContext;
        ProducerConfiguration producerConfiguration = new ProducerConfiguration(mqConfig, connectorId);
        try {
            kafkaProducer = new KafkaProducer<>(producerConfiguration.build());
        } catch (Exception e) {
            e.printStackTrace();
            tapLogger.error("Kafka producer error: " + ErrorKit.getLastCause(e).getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setConnectorId(String connectorId) {
        super.setConnectorId(connectorId);
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
        return super.testConnect();
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
        String schemaRegistryUrl = "http://" + this.kafkaConfig.getSchemaRegisterUrl() + "/subjects";
        BufferedReader reader = null;
        HttpURLConnection connection = null;
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

    private static List<String> parseJsonArray(String jsonArray) {
        List<String> result = new ArrayList<>();
        jsonArray = jsonArray.trim();
        if (jsonArray.startsWith("[") && jsonArray.endsWith("]")) {
            jsonArray = jsonArray.substring(1, jsonArray.length() - 1);
            String[] elements = jsonArray.split(",");
            for (String element : elements) {
                String subject = element.trim().replaceAll("\"", "");
                result.add(subject);
            }
        }
        return result;
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
        Properties properties = new Properties();
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
                properties.put("bootstrap.servers", kafkaConfig.getNameSrvAddr());
                //
                properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                //
                properties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
                //
                properties.put("schema.registry.url", kafkaConfig.getSchemaRegisterUrl());

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
                Schema schema = fieldAssembler.endRecord();
                String avroSchemaString = "{\"schema\":" + schema.toString() + "}";

                Schema.Parser parser = new Schema.Parser();
                Schema avroSchema = parser.parse(avroSchemaString);
                Producer<String, GenericRecord> producer = new KafkaProducer<>(properties);
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

                ProducerRecord<byte[], GenericRecord> producerRecord = new ProducerRecord<>(tapTable.getId(),
                        null, event.getTime(), getKafkaMessageKey(data, tapTable), record,
                        new RecordHeaders().add("mqOp", mqOp.getOp().getBytes()));
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

    private byte[] getKafkaMessageKey(Map<String, Object> data, TapTable tapTable) {
        if (EmptyKit.isEmpty(tapTable.primaryKeys(true))) {
            return null;
        } else {
            return jsonParser.toJsonBytes(tapTable.primaryKeys(true).stream().map(key -> String.valueOf(data.get(key))).collect(Collectors.joining("_")));
        }
    }

    private static Schema.Field createAvroField(String columnName, String columnType) {
        Schema avroType;


        switch (columnType) {
            case "TapBoolean":
                avroType = SchemaBuilder.builder().booleanType();
                break;
            case "TapNumber":
                avroType = SchemaBuilder.builder().doubleType();
                break;
            case "TapString":
            case "TapArray":
            case "TapMap":
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

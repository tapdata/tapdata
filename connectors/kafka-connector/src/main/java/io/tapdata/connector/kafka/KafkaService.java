package io.tapdata.connector.kafka;

import com.google.common.collect.Lists;
import io.tapdata.common.AbstractMqService;
import io.tapdata.common.constant.MqOp;
import io.tapdata.connector.kafka.admin.Admin;
import io.tapdata.connector.kafka.admin.DefaultAdmin;
import io.tapdata.connector.kafka.config.*;
import io.tapdata.connector.kafka.util.Krb5Util;
import io.tapdata.constant.MqTestItem;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class KafkaService extends AbstractMqService {

    private static final String TAG = KafkaService.class.getSimpleName();
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
    private final KafkaConfig kafkaConfig;
    private String connectorId;
    private ExecutorService produceService;

    public KafkaService(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        produceService = Executors.newFixedThreadPool(concurrency);
    }

    public void setConnectorId(String connectorId) {
        this.connectorId = connectorId;
    }

    @Override
    protected <T> Map<String, Object> analyzeTable(Object object, T topic, TapTable tapTable) throws Exception {
        return null;
    }

    @Override
    public void testConnection(Consumer<TestItem> consumer) {
        if (kafkaConfig.getKrb5()) {
            try {
                Krb5Util.checkKDCDomainsBase64(kafkaConfig.getKrb5Conf());
                consumer.accept(new TestItem(MqTestItem.KAFKA_BASE64_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null));
            } catch (Exception e) {
                consumer.accept(new TestItem(MqTestItem.KAFKA_BASE64_CONNECTION.getContent(), TestItem.RESULT_FAILED, e.getMessage()));
            }
        }
        AdminConfiguration configuration = new AdminConfiguration(kafkaConfig, connectorId);
        try (Admin admin = new DefaultAdmin(configuration)) {
            if (admin.isClusterConnectable()) {
                consumer.accept(new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null));
            } else {
                consumer.accept(new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, "cluster is not connectable"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void init() {

    }

    @Override
    public int countTables() throws Throwable {
        int tableCount;
        if (EmptyKit.isEmpty(kafkaConfig.getMqTopicSet())) {
            AdminConfiguration configuration = new AdminConfiguration(kafkaConfig, connectorId);
            Admin admin = new DefaultAdmin(configuration);
            tableCount = admin.listTopics().size();
            admin.close();
        } else {
            tableCount = kafkaConfig.getMqTopicSet().size();
        }
        return tableCount;
    }

    @Override
    public void loadTables(int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        AdminConfiguration configuration = new AdminConfiguration(kafkaConfig, connectorId);
        Admin admin = new DefaultAdmin(configuration);
        Set<String> existTopicSet = admin.listTopics();
        Set<String> destinationSet = new HashSet<>();
        Set<String> existTopicNameSet = new HashSet<>();
        if (EmptyKit.isEmpty(kafkaConfig.getMqTopicSet())) {
            destinationSet.addAll(existTopicSet);
        } else {
            //query queue which exists
            for (String topic : existTopicSet) {
                if (kafkaConfig.getMqTopicSet().contains(topic)) {
                    destinationSet.add(topic);
                    existTopicNameSet.add(topic);
                }
            }
            //create queue which not exists
            Set<String> needCreateTopicSet = kafkaConfig.getMqTopicSet().stream()
                    .filter(i -> !existTopicNameSet.contains(i)).collect(Collectors.toSet());
            if (EmptyKit.isNotEmpty(needCreateTopicSet)) {
                admin.createTopics(needCreateTopicSet);
                destinationSet.addAll(needCreateTopicSet);
            }
        }
        admin.close();
        SchemaConfiguration schemaConfiguration = new SchemaConfiguration(kafkaConfig, connectorId);
        submitPageTables(tableSize, consumer, schemaConfiguration, destinationSet);
    }

    protected void submitPageTables(int tableSize, Consumer<List<TapTable>> consumer, SchemaConfiguration schemaConfiguration, Set<String> destinationSet) throws Exception {
        List<List<String>> tablesList = Lists.partition(new ArrayList<>(destinationSet), tableSize);
        Map<String, Object> config = schemaConfiguration.build();
        tablesList.forEach(tables -> {
            List<TapTable> tableList = new CopyOnWriteArrayList<>();
            final Set<String> topicSet = new CopyOnWriteArraySet<>();
            CountDownLatch countDownLatch = new CountDownLatch(tables.size());
            executorService = Executors.newFixedThreadPool(tables.size());
            tables.forEach(table -> executorService.submit(() -> {
                KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(config);
                kafkaConsumer.subscribe(Collections.singleton(table));
                ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2L));
                for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    if (topicSet.contains(record.topic())) {
                        continue;
                    }
                    Map<String, Object> messageBody;
                    try {
                        messageBody = jsonParser.fromJsonBytes(record.value(), Map.class);
                    } catch (Exception e) {
                        TapLogger.error(TAG, "topic[{}] value [{}] can not parse to json, ignore...", record.topic(), record.value());
                        continue;
                    }
                    if (messageBody == null) {
                        TapLogger.warn(TAG, "messageBody not allow null...");
                        continue;
                    }
                    if (messageBody.containsKey("mqOp")) {
                        messageBody = (Map<String, Object>) messageBody.get("data");
                    }
                    try {
                        TapTable tapTable = new TapTable(table);
                        SCHEMA_PARSER.parse(tapTable, messageBody);
                        tableList.add(tapTable);
                    } catch (Throwable t) {
                        TapLogger.error(TAG, String.format("%s parse topic invalid json object: %s", record.topic(), t.getMessage()), t);
                    }
                    topicSet.add(record.topic());
                }
                kafkaConsumer.close();
                countDownLatch.countDown();
            }));
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            executorService.shutdown();
            tables.stream().filter(v -> !topicSet.contains(v)).map(TapTable::new).forEach(tableList::add);
            consumer.accept(tableList);
        });
    }

    @Override
    public void produce(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        ProducerConfiguration producerConfiguration = new ProducerConfiguration(kafkaConfig, connectorId);
        KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<>(producerConfiguration.build());
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        List<List<TapRecordEvent>> subEventLists = Lists.partition(tapRecordEvents, (tapRecordEvents.size() - 1) / concurrency + 1);
        CountDownLatch countDownLatch = new CountDownLatch(concurrency);
        subEventLists.forEach(subEventList -> produceService.submit(() -> {
            subEventList.forEach(event -> {
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
                byte[] body = jsonParser.toJsonBytes(data);
                MqOp finalMqOp = mqOp;
                Callback callback = (metadata, exception) -> {
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
                };
                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapTable.getId(),
                        null, event.getTime(), getKafkaMessageKey(data, tapTable), body,
                        new RecordHeaders().add("mqOp", mqOp.getOp().getBytes()));
                kafkaProducer.send(producerRecord, callback);
            });
            countDownLatch.countDown();
        }));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            TapLogger.error(TAG, "error occur when await", e);
        }
        kafkaProducer.close();
        writeListResultConsumer.accept(listResult.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
    }

    private byte[] getKafkaMessageKey(Map<String, Object> data, TapTable tapTable) {
        if (EmptyKit.isEmpty(tapTable.primaryKeys(true))) {
            return null;
        } else {
            return jsonParser.toJsonBytes(tapTable.primaryKeys(true).stream().map(key -> data.get(key).toString()).collect(Collectors.joining("_")));
        }
    }

    @Override
    public void consumeOne(TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        consuming.set(true);
        List<TapEvent> list = TapSimplify.list();
        String tableName = tapTable.getId();
        ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration(kafkaConfig, connectorId, true);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerConfiguration.build());
        kafkaConsumer.subscribe(Collections.singleton(tapTable.getId()));
        while (consuming.get()) {
            ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(6L));
            if (consumerRecords.isEmpty()) {
                break;
            }
            for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
                makeMessage(consumerRecord, list, tableName);
                if (list.size() >= eventBatchSize) {
                    eventsOffsetConsumer.accept(list, TapSimplify.list());
                    list = TapSimplify.list();
                }
            }
        }
        kafkaConsumer.close();
        if (EmptyKit.isNotEmpty(list)) {
            eventsOffsetConsumer.accept(list, TapSimplify.list());
        }
    }

    @Override
    public void streamConsume(List<String> tableList, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        consuming.set(true);
        ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration(kafkaConfig, connectorId, true);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerConfiguration.build());
        kafkaConsumer.subscribe(tableList);
        List<TapEvent> list = TapSimplify.list();
        while (consuming.get()) {
            ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2L));
            if (consumerRecords.isEmpty()) {
                continue;
            }
            for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
                makeMessage(consumerRecord, list, consumerRecord.topic());
                if (list.size() >= eventBatchSize) {
                    eventsOffsetConsumer.accept(list, TapSimplify.list());
                    list = TapSimplify.list();
                }
            }
        }
        if (EmptyKit.isNotEmpty(list)) {
            eventsOffsetConsumer.accept(list, TapSimplify.list());
        }
        kafkaConsumer.close();
    }

    private void makeMessage(ConsumerRecord<byte[], byte[]> consumerRecord, List<TapEvent> list, String tableName) {
        Map<String, Object> data = jsonParser.fromJsonBytes(consumerRecord.value(), Map.class);
        AtomicReference<String> mqOpReference = new AtomicReference<>();
        mqOpReference.set(MqOp.INSERT.getOp());
        consumerRecord.headers().headers("mqOp").forEach(header -> {
            mqOpReference.set(new String(header.value()));
        });
        switch (MqOp.fromValue(mqOpReference.get())) {
            case INSERT:
                list.add(new TapInsertRecordEvent().init().table(tableName).after(data).referenceTime(System.currentTimeMillis()));
                break;
            case UPDATE:
                list.add(new TapUpdateRecordEvent().init().table(tableName).after(data).referenceTime(System.currentTimeMillis()));
                break;
            case DELETE:
                list.add(new TapDeleteRecordEvent().init().table(tableName).before(data).referenceTime(System.currentTimeMillis()));
                break;
        }
    }

    @Override
    public void close() {
        super.close();
        produceService.shutdown();
    }
}

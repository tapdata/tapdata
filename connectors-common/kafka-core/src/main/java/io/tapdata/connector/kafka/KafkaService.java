package io.tapdata.connector.kafka;

import com.google.common.collect.Lists;
import io.tapdata.common.AbstractMqService;
import io.tapdata.common.constant.MqOp;
import io.tapdata.connector.kafka.admin.Admin;
import io.tapdata.connector.kafka.admin.DefaultAdmin;
import io.tapdata.connector.kafka.config.*;
import io.tapdata.connector.kafka.util.Krb5Util;
import io.tapdata.connector.kafka.util.ObjectUtils;
import io.tapdata.constant.MqTestItem;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.exception.StopException;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class KafkaService extends AbstractMqService {

    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
    private String connectorId;
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private KafkaConfig kafkaConfig;
    private static final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "tapdata"); //script factory


    public KafkaService() {

    }

    public KafkaService(KafkaConfig mqConfig, Log tapLogger) {
        this.mqConfig = mqConfig;
        this.tapLogger = tapLogger;
        ProducerConfiguration producerConfiguration = new ProducerConfiguration(mqConfig, connectorId);
        try {
            kafkaProducer = new KafkaProducer<>(producerConfiguration.build());
        } catch (Exception e) {
            e.printStackTrace();
            tapLogger.error("Kafka producer error: " + ErrorKit.getLastCause(e).getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void setConnectorId(String connectorId) {
        this.connectorId = connectorId;
    }

    public String getConnectorId() {
        return connectorId;
    }

    @Override
    protected <T> Map<String, Object> analyzeTable(Object object, T topic, TapTable tapTable) {
        return null;
    }

    @Override
    public TestItem testConnect() {
        if (((KafkaConfig) mqConfig).getKrb5()) {
            try {
                Krb5Util.checkKDCDomainsBase64(((KafkaConfig) mqConfig).getKrb5Conf());
                return new TestItem(MqTestItem.KAFKA_BASE64_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null);
            } catch (Exception e) {
                return new TestItem(MqTestItem.KAFKA_BASE64_CONNECTION.getContent(), TestItem.RESULT_FAILED, e.getMessage());
            }
        }
        AdminConfiguration configuration = new AdminConfiguration(((KafkaConfig) mqConfig), connectorId);
        try (Admin admin = new DefaultAdmin(configuration)) {
            if (admin.isClusterConnectable()) {
                return new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null);
            } else {
                return new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, "cluster is not connectable");
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, "when connect to cluster, error occurred " + e.getMessage());
        }
    }

    @Override
    public ConnectionCheckItem testConnection() {
        long start = System.currentTimeMillis();
        ConnectionCheckItem connectionCheckItem = ConnectionCheckItem.create();
        connectionCheckItem.item(ConnectionCheckItem.ITEM_CONNECTION);
        AdminConfiguration configuration = new AdminConfiguration(((KafkaConfig) mqConfig), connectorId);
        try (
                Admin admin = new DefaultAdmin(configuration)
        ) {
            if (admin.isClusterConnectable()) {
                connectionCheckItem.result(ConnectionCheckItem.RESULT_SUCCESSFULLY);
            } else {
                connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED).information("cluster is not connectable");
            }
        } catch (Exception e) {
            connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED).information(e.getMessage());
        }
        connectionCheckItem.takes(System.currentTimeMillis() - start);
        return connectionCheckItem;
    }

    @Override
    public void init() {

    }

    @Override
    public int countTables() throws Throwable {
        AdminConfiguration configuration = new AdminConfiguration(((KafkaConfig) mqConfig), connectorId);
        try (
                Admin admin = new DefaultAdmin(configuration)
        ) {
            Set<String> topicSet = admin.listTopics();
            if (EmptyKit.isEmpty(mqConfig.getMqTopicSet())) {
                return topicSet.size();
            } else {
                return (int) topicSet.stream().filter(topic -> mqConfig.getMqTopicSet().stream().anyMatch(reg -> StringKit.matchReg(topic, reg))).count();
            }
        }
    }

    @Override
    public void loadTables(int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        AdminConfiguration configuration = new AdminConfiguration(((KafkaConfig) mqConfig), connectorId);
        Set<String> destinationSet = new HashSet<>();
        try (
                Admin admin = new DefaultAdmin(configuration)
        ) {
            Set<String> existTopicSet = admin.listTopics();
            if (EmptyKit.isEmpty(mqConfig.getMqTopicSet())) {
                destinationSet.addAll(existTopicSet);
            } else {
                //query queue which exists
                for (String topic : existTopicSet) {
                    if (mqConfig.getMqTopicSet().stream().anyMatch(reg -> StringKit.matchReg(topic, reg))) {
                        destinationSet.add(topic);
                    }
                }
            }
        }
        SchemaConfiguration schemaConfiguration = new SchemaConfiguration(((KafkaConfig) mqConfig), connectorId);
        submitPageTables(tableSize, consumer, schemaConfiguration, destinationSet);
    }

    protected void submitPageTables(int tableSize, Consumer<List<TapTable>> consumer, SchemaConfiguration schemaConfiguration, Set<String> destinationSet) {
        List<List<String>> tablesList = Lists.partition(new ArrayList<>(destinationSet), tableSize);
        Map<String, Object> config = schemaConfiguration.build();
        try (
                KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(config)
        ) {
            tablesList.forEach(tables -> {
                List<TapTable> tableList = new ArrayList<>();
                List<String> topics = new ArrayList<>(tables);
                kafkaConsumer.subscribe(topics);
                ConsumerRecords<byte[], byte[]> consumerRecords;
                while (!(consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2L))).isEmpty()) {
                    for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                        if (!topics.contains(record.topic())) {
                            continue;
                        }
                        Map<String, Object> messageBody;
                        try {
                            messageBody = jsonParser.fromJsonBytes(record.value(), Map.class);
                        } catch (Exception e) {
                            tapLogger.error("topic[{}] value [{}] can not parse to json, ignore...", record.topic(), record.value());
                            continue;
                        }
                        if (messageBody == null) {
                            tapLogger.warn("messageBody not allow null...");
                            continue;
                        }
                        if (messageBody.containsKey("mqOp")) {
                            messageBody = (Map<String, Object>) messageBody.get("data");
                        }
                        try {
                            TapTable tapTable = new TapTable(record.topic());
                            SCHEMA_PARSER.parse(tapTable, messageBody);
                            tableList.add(tapTable);
                        } catch (Throwable t) {
                            tapLogger.error(String.format("%s parse topic invalid json object: %s", record.topic(), t.getMessage()), t);
                        }
                        topics.remove(record.topic());
                    }
                    if (EmptyKit.isEmpty(topics)) {
                        break;
                    }
                    kafkaConsumer.subscribe(topics);
                }
                topics.stream().map(TapTable::new).forEach(tableList::add);
                consumer.accept(tableList);
            });
        }
    }
    public static Object executeScript(ScriptEngine scriptEngine, String function, Object ... params) {
        if (scriptEngine != null) {
            Invocable invocable = (Invocable) scriptEngine;
            try {
                return invocable.invokeFunction(function, params);
            } catch (StopException e) {
//                TapLogger.info(TAG, "Get data and stop script.");
                throw new RuntimeException(e);
            } catch (ScriptException | NoSuchMethodException | RuntimeException e) {
//                TapLogger.error(TAG, "Run script error, message: {}", e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
        return null;
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
                byte[] body = jsonParser.toJsonBytes(data, JsonParser.ToJsonFeature.WriteMapNullValue);
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
                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapTable.getId(),
                        null, event.getTime(), getKafkaMessageKey(data, tapTable), body,
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
//            this.produce(null,tapRecordEvents,tapTable,writeListResultConsumer,isAlive);
    }

    @Override
    public void produce(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive){
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        CountDownLatch countDownLatch = new CountDownLatch(tapRecordEvents.size());
        ScriptEngine scriptEngine;
        String script = connectorContext.getNodeConfig().getString("script");
        Map<String,Object> record = new HashMap();
        try {
            scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT,
                    new ScriptOptions().engineName("graal.js"));
            scriptEngine.eval(script);
        } catch (Exception e) {
            throw new CoreException("Engine initialization failed!");
        }
        try {
            for (TapRecordEvent event : tapRecordEvents) {
                if (null != isAlive && !isAlive.get()) {
                    break;
                }
                Map<String, Object> data;
                Map<String,Map<String,Object>> allData = new HashMap();
                MqOp mqOp = MqOp.INSERT;
                if (event instanceof TapInsertRecordEvent) {
                    data = ((TapInsertRecordEvent) event).getAfter();
                    allData.put("before",new HashMap<String,Object>());
                    allData.put("after",data);
                } else if (event instanceof TapUpdateRecordEvent) {
                    data = ((TapUpdateRecordEvent) event).getAfter();
                    Map<String, Object> before = ((TapUpdateRecordEvent) event).getBefore();
                    allData.put("before",null == before ? new HashMap<>(): before);
                    allData.put("after",data);
                    mqOp = MqOp.UPDATE;
                } else if (event instanceof TapDeleteRecordEvent) {
                    data = ((TapDeleteRecordEvent) event).getBefore();
                    allData.put("before",data);
                    allData.put("after",new HashMap<String,Object>());
                    mqOp = MqOp.DELETE;
                } else {
                    data = new HashMap<>();
                }
                byte[] kafkaMessageKey = getKafkaMessageKey(data, tapTable);
                record.put("data",allData);
                Map<String, Object> header = new HashMap();
                header.put("mqOp",mqOp.getOp());
                record.put("header",header);
                String op = mqOp.getOp();
                Collection<String> conditionKeys = tapTable.primaryKeys(true);
                kafkaConfig = (KafkaConfig)new KafkaConfig().load(connectorContext.getConnectionConfig());
                RecordHeaders recordHeaders = new RecordHeaders();
                byte[] body = {};
                Object eventObj = ObjectUtils.covertData(executeScript(scriptEngine, "process", record, op, conditionKeys));
                if(null==eventObj){
                    continue;
                }else {
                    Map<String,Object> res = (Map<String,Object>) eventObj;
                    if (null==res.get("data")) {
                        throw new RuntimeException("data cannot be null");
                    }else {
                        Object obj = res.get("data");
                        if(obj instanceof Map){
                            Map<String, Map<String,Object>> map = (Map<String, Map<String,Object>>) res.get("data");
                            if (map.containsKey("before")&&map.get("before").isEmpty()){
                                map.remove("before");
                            }
                            if (map.containsKey("after")&&map.get("after").isEmpty()){
                                map.remove("after");
                            }
                            res.put("data",map);
                            body = jsonParser.toJsonBytes(res.get("data"));
                        }else {
                            body = obj.toString().getBytes();
                        }
                    }
                    if(res.containsKey("header")){
                        Object obj = res.get("header");
                        if(obj instanceof Map){
                            Map<String, Object> head = (Map<String, Object>) res.get("header");
                            for (String s : head.keySet()) {
                                recordHeaders.add(s,head.get(s).toString().getBytes());
                            }
                        }else {
                            throw new RuntimeException("header must be a collection type");
                        }
                    }else {
                        recordHeaders.add("mqOp",mqOp.toString().getBytes());
                    }
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
                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapTable.getId(),
                        null, event.getTime(), kafkaMessageKey, body,
                        recordHeaders);
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

    @Override
    public void produce(TapFieldBaseEvent tapFieldBaseEvent) {
        AtomicReference<Throwable> reference = new AtomicReference<>();
        byte[] body = jsonParser.toJsonBytes(tapFieldBaseEvent);
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapFieldBaseEvent.getTableId(),
                null, tapFieldBaseEvent.getTime(), null, body,
                new RecordHeaders()
                        .add("mqOp", MqOp.DDL.getOp().getBytes())
                        .add("eventClass", tapFieldBaseEvent.getClass().getName().getBytes()));
        Callback callback = (metadata, exception) -> reference.set(exception);
        kafkaProducer.send(producerRecord, callback);
        if (EmptyKit.isNotNull(reference.get())) {
            throw new RuntimeException(reference.get());
        }
    }

    private byte[] getKafkaMessageKey(Map<String, Object> data, TapTable tapTable) {
        if (EmptyKit.isEmpty(tapTable.primaryKeys(true))) {
            return null;
        } else {
            return jsonParser.toJsonBytes(tapTable.primaryKeys(true).stream().map(key -> String.valueOf(data.get(key))).collect(Collectors.joining("_")));
        }
    }

    @Override
    public void consumeOne(TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        consuming.set(true);
        List<TapEvent> list = TapSimplify.list();
        String tableName = tapTable.getId();
        ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration(((KafkaConfig) mqConfig), connectorId, true);
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
    public void streamConsume(List<String> tableList, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        consuming.set(true);
        ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration(((KafkaConfig) mqConfig), connectorId, true);
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
        AtomicReference<String> mqOpReference = new AtomicReference<>();
        mqOpReference.set(MqOp.INSERT.getOp());
        consumerRecord.headers().headers("mqOp").forEach(header -> mqOpReference.set(new String(header.value())));
        if (MqOp.fromValue(mqOpReference.get()) == MqOp.DDL) {
            consumerRecord.headers().headers("eventClass").forEach(eventClass -> {
                TapFieldBaseEvent tapFieldBaseEvent;
                try {
                    tapFieldBaseEvent = (TapFieldBaseEvent) jsonParser.fromJsonBytes(consumerRecord.value(), Class.forName(new String(eventClass.value())));
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                list.add(tapFieldBaseEvent);
            });
        } else {
            Map<String, Object> data = jsonParser.fromJsonBytes(consumerRecord.value(), Map.class);
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
    }

    @Override
    public void close() {
        super.close();
        if (EmptyKit.isNotNull(kafkaProducer)) {
            kafkaProducer.close();
        }
    }
}

package io.tapdata.connector.activemq;

import com.google.common.collect.Lists;
import io.tapdata.common.AbstractMqService;
import io.tapdata.common.constant.MqOp;
import io.tapdata.connector.activemq.config.ActivemqConfig;
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
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.commons.lang3.StringUtils;

import javax.jms.Queue;
import javax.jms.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ActivemqService extends AbstractMqService {

    private static final String TAG = ActivemqService.class.getSimpleName();
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
    private final ActivemqConfig activemqConfig;
    private final ActiveMQConnectionFactory activeMQConnectionFactory;
    private ActiveMQConnection activemqConnection;

    public ActivemqService(ActivemqConfig activemqConfig) {
        this.activemqConfig = activemqConfig;
        activeMQConnectionFactory = new ActiveMQConnectionFactory();
        activeMQConnectionFactory.setBrokerURL(activemqConfig.getBrokerURL());
        if (StringUtils.isNotEmpty(activemqConfig.getMqUsername())) {
            activeMQConnectionFactory.setUserName(activemqConfig.getMqUsername());
        }
        if (StringUtils.isNotEmpty(activemqConfig.getMqPassword())) {
            activeMQConnectionFactory.setPassword(activemqConfig.getMqPassword());
        }
    }

    @Override
    public void testConnection(Consumer<TestItem> consumer) {
        try {
            activemqConnection = (ActiveMQConnection) activeMQConnectionFactory.createConnection();
            activemqConnection.start();
            consumer.accept(new TestItem(MqTestItem.ACTIVE_MQ_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null));
        } catch (Throwable t) {
            consumer.accept(new TestItem(MqTestItem.ACTIVE_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, t.getMessage()));
        } finally {
            close();
        }
    }

    @Override
    public void init() throws Throwable {
        activemqConnection = (ActiveMQConnection) activeMQConnectionFactory.createConnection();
        activemqConnection.start();
    }

    @Override
    public void close() {
        try {
            consuming.set(false);
            Thread.sleep(2000);
            if (EmptyKit.isNotNull(activemqConnection)) {
                activemqConnection.stop();
                activemqConnection.close();
            }
        } catch (Exception e) {
            TapLogger.error(TAG, "close connection error", e);
        }
    }

    @Override
    public void loadTables(int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        long deadline = System.currentTimeMillis() + MAX_LOAD_TIMEOUT;
        Map<String, Map<String, Object>> destinationRecordMap = new HashMap<>();
        Set<ActiveMQTopic> existTopicSet = activemqConnection.getDestinationSource().getTopics();
        Set<ActiveMQQueue> existQueueSet = activemqConnection.getDestinationSource().getQueues();
        Set<Destination> destinationSet = new HashSet<>();
        Set<String> existQueueNameSet = new HashSet<>();
        Set<String> existTopicNameSet = new HashSet<>();
        //query topic and queue which exists
        for (Queue queue : existQueueSet) {
            if (activemqConfig.getMqQueueSet().contains(queue.getQueueName())) {
                destinationSet.add(queue);
                existQueueNameSet.add(queue.getQueueName());
            }
        }
        for (ActiveMQTopic topic : existTopicSet) {
            if (activemqConfig.getMqTopicSet().contains(topic.getTopicName())) {
                destinationSet.add(topic);
                existTopicNameSet.add(topic.getTopicName());
            }
        }
        //create topic and queue which not exists
        Set<String> needCreateQueueSet = activemqConfig.getMqQueueSet().stream()
                .filter(i -> !existQueueNameSet.contains(i)).collect(Collectors.toSet());
        Set<String> needCreateTopicSet = activemqConfig.getMqTopicSet().stream()
                .filter(i -> !existTopicNameSet.contains(i)).collect(Collectors.toSet());
        if (EmptyKit.isNotEmpty(needCreateQueueSet) || EmptyKit.isNotEmpty(needCreateTopicSet)) {
            Session session = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try {
                needCreateQueueSet.forEach(item -> {
                    try {
                        destinationSet.add(session.createQueue(item));
                    } catch (JMSException e) {
                        TapLogger.error(TAG, "create queue error", e);
                    }
                });
                needCreateTopicSet.forEach(item -> {
                    try {
                        destinationSet.add(session.createTopic(item));
                    } catch (JMSException e) {
                        TapLogger.error(TAG, "create topic error", e);
                    }
                });
            } finally {
                session.close();
            }
        }
        Session session = activemqConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Iterator<Destination> iterator = destinationSet.iterator();
        while (iterator.hasNext() && System.currentTimeMillis() < deadline) {
            Destination destination = iterator.next();
            MessageConsumer messageConsumer = session.createConsumer(destination);
            TextMessage textMessage = (TextMessage) messageConsumer.receive(SINGLE_MAX_LOAD_TIMEOUT);
            if (textMessage == null) {
                destinationRecordMap.put(getDestinationName(destination), new HashMap<>());
                continue;
            }
            destinationRecordMap.put(getDestinationName(destination), jsonParser.fromJson(textMessage.getText(), Map.class));
        }
        TreeMap<String, Map<String, Object>> treeMap = new TreeMap<>(Comparator.naturalOrder());
        treeMap.putAll(destinationRecordMap);
        List<TapTable> tableList = TapSimplify.list();
        for (Map.Entry<String, Map<String, Object>> e : treeMap.entrySet()) {
            TapTable table = TapSimplify.table(e.getKey());
            SCHEMA_PARSER.parse(table, e.getValue());
            tableList.add(table);
            if (tableList.size() >= tableSize) {
                consumer.accept(tableList);
                tableList = TapSimplify.list();
            }
        }
        if (EmptyKit.isNotEmpty(tableList)) {
            consumer.accept(tableList);
        }
    }

    @Override
    public void produce(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        Session session = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);
        String tableName = tapTable.getId();
        Destination destination;
        if (tableName.startsWith("Topic_")) {
            destination = session.createTopic(getRealTableName(tableName));
        } else {
            destination = session.createQueue(getRealTableName(tableName));
        }
        for (TapRecordEvent event : tapRecordEvents) {
            ActivemqMessage activemqMessage = new ActivemqMessage();
            TextMessage textMessage = new ActiveMQTextMessage();
            if (event instanceof TapInsertRecordEvent) {
                textMessage.setStringProperty("mqOp", MqOp.INSERT.getOp());
                textMessage.setText(jsonParser.toJson(((TapInsertRecordEvent) event).getAfter()));
            } else if (event instanceof TapUpdateRecordEvent) {
                textMessage.setStringProperty("mqOp", MqOp.UPDATE.getOp());
                textMessage.setText(jsonParser.toJson(((TapUpdateRecordEvent) event).getAfter()));
            } else if (event instanceof TapDeleteRecordEvent) {
                textMessage.setStringProperty("mqOp", MqOp.DELETE.getOp());
                textMessage.setText(jsonParser.toJson(((TapDeleteRecordEvent) event).getBefore()));
            }
            activemqMessage.setMessage(textMessage);
            producer.send(destination, activemqMessage.getMessage());
        }
    }

    @Override
    public void consumeOne(TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        consuming.set(true);
        List<TapEvent> list = TapSimplify.list();
        Session session = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        String tableName = tapTable.getId();
        String realTableName = getRealTableName(tableName);
        Destination destination;
        if (tableName.startsWith("Topic_")) {
            destination = session.createTopic(realTableName);
        } else {
            destination = session.createQueue(realTableName);
        }
        MessageConsumer messageConsumer = session.createConsumer(destination);
        while (consuming.get()) {
            Message message = messageConsumer.receive(SINGLE_MAX_LOAD_TIMEOUT * 3);
            if (EmptyKit.isNull(message)) {
                break;
            }
            makeMessage(message, list, realTableName);
            if (list.size() >= eventBatchSize) {
                eventsOffsetConsumer.accept(list, TapSimplify.list());
                list = TapSimplify.list();
            }
        }
        if (EmptyKit.isNotEmpty(list)) {
            eventsOffsetConsumer.accept(list, TapSimplify.list());
        }
    }

    @Override
    public void streamConsume(List<String> tableList, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        consuming.set(true);
        Session session = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        List<List<String>> tablesList = Lists.partition(tableList, concurrency);
        executorService = Executors.newFixedThreadPool(concurrency);
        CountDownLatch countDownLatch = new CountDownLatch(concurrency);
        tablesList.forEach(tables -> executorService.submit(() -> {
            List<TapEvent> list = TapSimplify.list();
            Map<String, MessageConsumer> consumerMap = new HashMap<>();
            while (consuming.get()) {
                for (String tableName : tables) {
                    try {
                        String realTableName = getRealTableName(tableName);
                        MessageConsumer messageConsumer = consumerMap.get(realTableName);
                        if (EmptyKit.isNull(messageConsumer)) {
                            Destination destination;
                            if (tableName.startsWith("Topic_")) {
                                destination = session.createTopic(realTableName);
                            } else {
                                destination = session.createQueue(realTableName);
                            }
                            messageConsumer = session.createConsumer(destination);
                            consumerMap.put(realTableName, messageConsumer);
                        }
                        Message message = messageConsumer.receive(SINGLE_MAX_LOAD_TIMEOUT / 2);
                        if (EmptyKit.isNull(message)) {
                            continue;
                        }
                        makeMessage(message, list, realTableName);
                        if (list.size() >= eventBatchSize) {
                            eventsOffsetConsumer.accept(list, TapSimplify.list());
                            list = TapSimplify.list();
                        }
                    } catch (JMSException e) {
                        TapLogger.error(TAG, "error occur when consume topic or queue: {}", tableName, e);
                    }
                }
            }
            if (EmptyKit.isNotEmpty(list)) {
                eventsOffsetConsumer.accept(list, TapSimplify.list());
            }
            countDownLatch.countDown();
        }));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            TapLogger.error(TAG, "error occur when await", e);
        }
    }

    private void makeMessage(Message message, List<TapEvent> list, String tableName) throws JMSException {
        TextMessage textMessage = (TextMessage) message;
        Map<String, Object> data = jsonParser.fromJson(textMessage.getText(), Map.class);
        switch (MqOp.fromValue(textMessage.getStringProperty("mqOp"))) {
            case INSERT:
                list.add(new TapInsertRecordEvent().init().table(tableName).after(data));
                break;
            case UPDATE:
                list.add(new TapUpdateRecordEvent().init().table(tableName).after(data));
                break;
            case DELETE:
                list.add(new TapDeleteRecordEvent().init().table(tableName).before(data));
                break;
        }
    }

    private String getDestinationName(Destination destination) {
        try {
            if (destination instanceof Queue) {
                return "Queue_" + ((Queue) destination).getQueueName();
            } else if (destination instanceof Topic) {
                return "Topic_" + ((Topic) destination).getTopicName();
            }
        } catch (JMSException e) {
            TapLogger.error(TAG, "error", e);
        }
        return "";
    }

    private String getRealTableName(String tableName) {
        if (tableName.startsWith("Queue_") || tableName.startsWith("Topic_")) {
            return tableName.substring(tableName.indexOf("_"));
        } else {
            return tableName;
        }
    }

}

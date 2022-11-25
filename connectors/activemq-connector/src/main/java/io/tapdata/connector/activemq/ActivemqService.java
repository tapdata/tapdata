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
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.Queue;
import javax.jms.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ActivemqService extends AbstractMqService {

    private static final String TAG = ActivemqService.class.getSimpleName();
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
    private final ActiveMQConnectionFactory activeMQConnectionFactory;
    private ActiveMQConnection activemqConnection;

    public ActivemqService(ActivemqConfig activemqConfig) {
        this.mqConfig = activemqConfig;
        activeMQConnectionFactory = new ActiveMQConnectionFactory();
        activeMQConnectionFactory.setBrokerURL(activemqConfig.getBrokerURL());
        if (EmptyKit.isNotEmpty(activemqConfig.getMqUsername())) {
            activeMQConnectionFactory.setUserName(activemqConfig.getMqUsername());
        }
        if (EmptyKit.isNotEmpty(activemqConfig.getMqPassword())) {
            activeMQConnectionFactory.setPassword(activemqConfig.getMqPassword());
        }
    }

    @Override
    public TestItem testConnect() {
        try {
            activemqConnection = (ActiveMQConnection) activeMQConnectionFactory.createConnection();
            activemqConnection.start();
            return new TestItem(MqTestItem.ACTIVE_MQ_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null);
        } catch (Throwable t) {
            return new TestItem(MqTestItem.ACTIVE_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, t.getMessage());
        }
    }

    @Override
    public ConnectionCheckItem testConnection() {
        long start = System.currentTimeMillis();
        ConnectionCheckItem connectionCheckItem = ConnectionCheckItem.create();
        connectionCheckItem.item(ConnectionCheckItem.ITEM_CONNECTION);
        try {
            activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE).close();
            connectionCheckItem.result(ConnectionCheckItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED).information(e.getMessage());
        }
        connectionCheckItem.takes(System.currentTimeMillis() - start);
        return connectionCheckItem;
    }

    @Override
    public void init() throws Throwable {
        activemqConnection = (ActiveMQConnection) activeMQConnectionFactory.createConnection();
        activemqConnection.start();
    }

    @Override
    public void close() {
        super.close();
        try {
            if (EmptyKit.isNotNull(activemqConnection)) {
                activemqConnection.stop();
                activemqConnection.close();
            }
        } catch (Exception e) {
            TapLogger.error(TAG, "close connection error", e);
        }
    }

    @Override
    protected <T> Map<String, Object> analyzeTable(Object object, T topic, TapTable tapTable) throws Exception {
        Session session = (Session) object;
        tapTable.setId(((Queue) topic).getQueueName());
        tapTable.setName(((Queue) topic).getQueueName());
        MessageConsumer messageConsumer = session.createConsumer((Destination) topic);
        TextMessage textMessage = (TextMessage) messageConsumer.receive(SINGLE_MAX_LOAD_TIMEOUT);
        if (textMessage == null) {
            return new HashMap<>();
        }
        return jsonParser.fromJson(textMessage.getText(), Map.class);
    }

    @Override
    public int countTables() throws Throwable {
        if (EmptyKit.isEmpty(mqConfig.getMqQueueSet())) {
            return activemqConnection.getDestinationSource().getQueues().size();
        } else {
            return mqConfig.getMqQueueSet().size();
        }
    }

    @Override
    public void loadTables(int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        Set<ActiveMQQueue> existQueueSet = activemqConnection.getDestinationSource().getQueues();
        Set<Destination> destinationSet = new HashSet<>();
        Set<String> existQueueNameSet = new HashSet<>();
        if (EmptyKit.isEmpty(mqConfig.getMqQueueSet())) {
            destinationSet.addAll(existQueueSet);
        } else {
            //query queue which exists
            for (Queue queue : existQueueSet) {
                if (mqConfig.getMqQueueSet().contains(queue.getQueueName())) {
                    destinationSet.add(queue);
                    existQueueNameSet.add(queue.getQueueName());
                }
            }
            //create queue which not exists
            Set<String> needCreateQueueSet = mqConfig.getMqQueueSet().stream()
                    .filter(i -> !existQueueNameSet.contains(i)).collect(Collectors.toSet());
            if (EmptyKit.isNotEmpty(needCreateQueueSet)) {
                Session session = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                try {
                    needCreateQueueSet.forEach(item -> {
                        try {
                            destinationSet.add(session.createQueue(item));
                        } catch (JMSException e) {
                            TapLogger.error(TAG, "create queue error", e);
                        }
                    });
                } finally {
                    session.close();
                }
            }
        }
        Session session = activemqConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        submitTables(tableSize, consumer, session, destinationSet);
        session.close();
    }

    @Override
    public void produce(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) throws Throwable {
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        Session session = activemqConnection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);
        String tableName = tapTable.getId();
        Destination destination = session.createQueue(tableName);
        for (TapRecordEvent event : tapRecordEvents) {
            if (null != isAlive && !isAlive.get()) {
                break;
            }
            TextMessage textMessage = new ActiveMQTextMessage();
            MqOp mqOp = MqOp.INSERT;
            if (event instanceof TapInsertRecordEvent) {
                textMessage.setStringProperty("mqOp", MqOp.INSERT.getOp());
                textMessage.setText(jsonParser.toJson(((TapInsertRecordEvent) event).getAfter()));
            } else if (event instanceof TapUpdateRecordEvent) {
                textMessage.setStringProperty("mqOp", MqOp.UPDATE.getOp());
                textMessage.setText(jsonParser.toJson(((TapUpdateRecordEvent) event).getAfter()));
                mqOp = MqOp.UPDATE;
            } else if (event instanceof TapDeleteRecordEvent) {
                textMessage.setStringProperty("mqOp", MqOp.DELETE.getOp());
                textMessage.setText(jsonParser.toJson(((TapDeleteRecordEvent) event).getBefore()));
                mqOp = MqOp.DELETE;
            }
            try {
                producer.send(destination, textMessage);
                switch (mqOp) {
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
            } catch (Exception e) {
                listResult.addError(event, e);
            }
        }
        session.commit();
        writeListResultConsumer.accept(listResult.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
    }

    @Override
    public void consumeOne(TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        consuming.set(true);
        List<TapEvent> list = TapSimplify.list();
        Session session = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        String tableName = tapTable.getId();
        Destination destination = session.createQueue(tableName);
        MessageConsumer messageConsumer = session.createConsumer(destination);
        while (consuming.get()) {
            Message message = messageConsumer.receive(SINGLE_MAX_LOAD_TIMEOUT * 3);
            if (EmptyKit.isNull(message)) {
                break;
            }
            makeMessage(message, list, tableName);
            if (list.size() >= eventBatchSize) {
                eventsOffsetConsumer.accept(list, TapSimplify.list());
                list = TapSimplify.list();
            }
        }
        session.close();
        if (EmptyKit.isNotEmpty(list)) {
            eventsOffsetConsumer.accept(list, TapSimplify.list());
        }
    }

    @Override
    public void streamConsume(List<String> tableList, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        consuming.set(true);
        Session session = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        List<List<String>> tablesList = Lists.partition(tableList, (tableList.size() - 1) / concurrency + 1);
        executorService = Executors.newFixedThreadPool(tablesList.size());
        CountDownLatch countDownLatch = new CountDownLatch(tablesList.size());
        tablesList.forEach(tables -> executorService.submit(() -> {
            List<TapEvent> list = TapSimplify.list();
            Map<String, MessageConsumer> consumerMap = new HashMap<>();
            int err = 0;
            while (consuming.get() && err < 10) {
                for (String tableName : tables) {
                    try {
                        MessageConsumer messageConsumer = consumerMap.get(tableName);
                        if (EmptyKit.isNull(messageConsumer)) {
                            Destination destination = session.createQueue(tableName);
                            messageConsumer = session.createConsumer(destination);
                            consumerMap.put(tableName, messageConsumer);
                        }
                        Message message = messageConsumer.receive(SINGLE_MAX_LOAD_TIMEOUT / 2);
                        if (EmptyKit.isNull(message)) {
                            continue;
                        }
                        makeMessage(message, list, tableName);
                        if (list.size() >= eventBatchSize) {
                            eventsOffsetConsumer.accept(list, TapSimplify.list());
                            list = TapSimplify.list();
                        }
                    } catch (Exception e) {
                        TapLogger.error(TAG, "error occur when consume queue: {}", tableName, e);
                        err++;
                    }
                }
                TapSimplify.sleep(50);
            }
            if (EmptyKit.isNotEmpty(list)) {
                eventsOffsetConsumer.accept(list, TapSimplify.list());
            }
            consumerMap.forEach((key, value) -> {
                try {
                    value.close();
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
            });
            countDownLatch.countDown();
            if (err >= 10) {
                consuming.set(false);
                throw new RuntimeException("Stream Read error!");
            }
        }));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            TapLogger.error(TAG, "error occur when await", e);
        }
        session.close();
    }

    private void makeMessage(Message message, List<TapEvent> list, String tableName) throws JMSException {
        TextMessage textMessage = (TextMessage) message;
        Map<String, Object> data = jsonParser.fromJson(textMessage.getText(), Map.class);
        switch (MqOp.fromValue(textMessage.getStringProperty("mqOp"))) {
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

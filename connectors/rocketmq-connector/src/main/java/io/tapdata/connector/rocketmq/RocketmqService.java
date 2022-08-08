package io.tapdata.connector.rocketmq;

import com.google.common.collect.Lists;
import io.tapdata.common.AbstractMqService;
import io.tapdata.common.constant.MqOp;
import io.tapdata.connector.rocketmq.config.RocketmqConfig;
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
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RocketmqService extends AbstractMqService {

    private static final String TAG = RocketmqService.class.getSimpleName();
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
    private final RocketmqConfig rocketmqConfig;
    private final DefaultMQProducer defaultMQProducer;

    public RocketmqService(RocketmqConfig rocketmqConfig) {
        this.rocketmqConfig = rocketmqConfig;
        this.defaultMQProducer = new DefaultMQProducer(getRPCHook());
        defaultMQProducer.setNamesrvAddr(rocketmqConfig.getNameSrvAddr());
        defaultMQProducer.setProducerGroup(rocketmqConfig.getProducerGroup());
    }

    @Override
    public void testConnection(Consumer<TestItem> consumer) {
        try {
            defaultMQProducer.start();
            consumer.accept(new TestItem(MqTestItem.ROCKET_MQ_CONNECTION.getContent(), TestItem.RESULT_SUCCESSFULLY, null));
        } catch (Throwable t) {
            consumer.accept(new TestItem(MqTestItem.ROCKET_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, t.getMessage()));
        }
    }

    @Override
    public void init() throws Throwable {
        defaultMQProducer.start();
    }

    public RPCHook getRPCHook() {
        if (EmptyKit.isNotBlank(rocketmqConfig.getMqUsername()) && EmptyKit.isNotBlank(rocketmqConfig.getMqPassword())) {
            return new AclClientRPCHook(new SessionCredentials(rocketmqConfig.getMqUsername(), rocketmqConfig.getMqPassword()));
        }
        return null;
    }

    @Override
    public void close() {
        super.close();
        if (EmptyKit.isNotNull(defaultMQProducer)) {
            defaultMQProducer.shutdown();
        }
    }

    @Override
    protected <T> Map<String, Object> analyzeTable(Object object, T topic, TapTable tapTable) throws Exception {
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer(rocketmqConfig.getConsumerGroup(), getRPCHook());
        litePullConsumer.setNamesrvAddr(rocketmqConfig.getNameSrvAddr());
        litePullConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        litePullConsumer.start();
        tapTable.setId((String) topic);
        tapTable.setName((String) topic);
        litePullConsumer.subscribe((String) topic, "*");
        List<MessageExt> messageExts = litePullConsumer.poll(2000);
        if (EmptyKit.isEmpty(messageExts)) {
            return new HashMap<>();
        }
        MessageExt messageExt = messageExts.get(0);
        litePullConsumer.shutdown();
        return jsonParser.fromJsonBytes(messageExt.getBody(), Map.class);
    }

    @Override
    public int countTables() throws Throwable {
        if (EmptyKit.isEmpty(rocketmqConfig.getMqTopicSet())) {
            DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
            defaultMQAdminExt.setNamesrvAddr(rocketmqConfig.getNameSrvAddr());
            defaultMQAdminExt.start();
            Set<String> list = defaultMQAdminExt.fetchAllTopicList().getTopicList();
            defaultMQAdminExt.shutdown();
            return (int) list.stream().filter(topic -> !topic.startsWith("%RETRY%")).count();
        } else {
            return rocketmqConfig.getMqTopicSet().size();
        }
    }

    @Override
    public void loadTables(int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setNamesrvAddr(rocketmqConfig.getNameSrvAddr());
        defaultMQAdminExt.start();
        Set<String> existTopicSet = defaultMQAdminExt.fetchAllTopicList().getTopicList()
                .stream().filter(topic -> !topic.startsWith("%RETRY%")).collect(Collectors.toSet());
        Set<String> destinationSet = new HashSet<>();
        Set<String> existTopicNameSet = new HashSet<>();
        if (EmptyKit.isEmpty(rocketmqConfig.getMqTopicSet())) {
            destinationSet.addAll(existTopicSet);
        } else {
            //query queue which exists
            for (String topic : existTopicSet) {
                if (rocketmqConfig.getMqTopicSet().contains(topic)) {
                    destinationSet.add(topic);
                    existTopicNameSet.add(topic);
                }
            }
            //create queue which not exists
            Set<String> needCreateTopicSet = rocketmqConfig.getMqTopicSet().stream()
                    .filter(i -> !existTopicNameSet.contains(i)).collect(Collectors.toSet());
            if (EmptyKit.isNotEmpty(needCreateTopicSet)) {
                for (String topic : needCreateTopicSet) {
                    defaultMQAdminExt.createTopic("tapdata", topic, 1);
                    destinationSet.add(topic);
                }
            }
        }
        submitTables(tableSize, consumer, null, destinationSet);
        defaultMQAdminExt.shutdown();
    }

    @Override
    public void produce(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        List<Message> sendList = TapSimplify.list();
        int i = 0;
        int u = 0;
        int d = 0;
        for (TapRecordEvent event : tapRecordEvents) {
            Message message = new Message();
            message.setTopic(tapTable.getId());
            byte[] body = null;
            MqOp mqOp = MqOp.INSERT;
            if (event instanceof TapInsertRecordEvent) {
                body = jsonParser.toJsonBytes(((TapInsertRecordEvent) event).getAfter());
            } else if (event instanceof TapUpdateRecordEvent) {
                body = jsonParser.toJsonBytes(((TapUpdateRecordEvent) event).getAfter());
                mqOp = MqOp.UPDATE;
            } else if (event instanceof TapDeleteRecordEvent) {
                body = jsonParser.toJsonBytes(((TapDeleteRecordEvent) event).getBefore());
                mqOp = MqOp.DELETE;
            }
            message.putUserProperty("mqOp", mqOp.getOp());
            message.setBody(body);
            sendList.add(message);
            switch (mqOp) {
                case INSERT:
                    i++;
                    break;
                case UPDATE:
                    u++;
                    break;
                case DELETE:
                    d++;
                    break;
            }
            if (sendList.size() >= 100) {
                try {
                    defaultMQProducer.send(sendList);
                    sendList.clear();
                    insert.addAndGet(i);
                    update.addAndGet(u);
                    delete.addAndGet(d);
                    i = 0;
                    u = 0;
                    d = 0;
                } catch (Exception e) {
                    listResult.addError(event, e);
                }
            }
        }
        if (EmptyKit.isNotEmpty(sendList)) {
            if (sendList.size() == 1) {
                defaultMQProducer.send(sendList.get(0));
            } else {
                defaultMQProducer.send(sendList);
            }
            insert.addAndGet(i);
            update.addAndGet(u);
            delete.addAndGet(d);
        }
        writeListResultConsumer.accept(listResult.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
    }

    @Override
    public void consumeOne(TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        consuming.set(true);
        List<TapEvent> list = TapSimplify.list();
        String tableName = tapTable.getId();
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer(rocketmqConfig.getConsumerGroup(), getRPCHook());
        litePullConsumer.setNamesrvAddr(rocketmqConfig.getNameSrvAddr());
        litePullConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        litePullConsumer.start();
        litePullConsumer.subscribe(tableName, "*");
        try {
            while (consuming.get()) {
                List<MessageExt> messageList = litePullConsumer.poll(SINGLE_MAX_LOAD_TIMEOUT * 3);
                if (EmptyKit.isEmpty(messageList)) {
                    break;
                }
                for (MessageExt message : messageList) {
                    makeMessage(message, list, tableName);
                    if (list.size() >= eventBatchSize) {
                        eventsOffsetConsumer.accept(list, TapSimplify.list());
                        list = TapSimplify.list();
                    }
                }
            }
        } finally {
            litePullConsumer.shutdown();
        }
        if (EmptyKit.isNotEmpty(list)) {
            eventsOffsetConsumer.accept(list, TapSimplify.list());
        }
    }

    private void makeMessage(MessageExt message, List<TapEvent> list, String tableName) {
        Map<String, Object> data = jsonParser.fromJsonBytes(message.getBody(), Map.class);
        switch (MqOp.fromValue(message.getUserProperty("mqOp"))) {
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
    public void streamConsume(List<String> tableList, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        consuming.set(true);
        List<List<String>> tablesList = Lists.partition(tableList, (tableList.size() - 1) / concurrency + 1);
        executorService = Executors.newFixedThreadPool(tablesList.size());
        CountDownLatch countDownLatch = new CountDownLatch(tablesList.size());
        tablesList.forEach(tables -> executorService.submit(() -> {
            List<TapEvent> list = TapSimplify.list();
            Map<String, DefaultLitePullConsumer> consumerMap = new HashMap<>();
            int err = 0;
            while (consuming.get() && err < 10) {
                for (String tableName : tables) {
                    try {
                        DefaultLitePullConsumer litePullConsumer = consumerMap.get(tableName);
                        if (EmptyKit.isNull(litePullConsumer)) {
                            litePullConsumer = new DefaultLitePullConsumer(rocketmqConfig.getConsumerGroup(), getRPCHook());
                            litePullConsumer.setNamesrvAddr(rocketmqConfig.getNameSrvAddr());
                            litePullConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
                            litePullConsumer.subscribe(tableName, "*");
                            try {
                                litePullConsumer.start();
                            } catch (MQClientException e) {
                                throw new RuntimeException(e);
                            }
                            consumerMap.put(tableName, litePullConsumer);
                        }
                        List<MessageExt> messageList = litePullConsumer.poll(SINGLE_MAX_LOAD_TIMEOUT / 2);
                        if (EmptyKit.isEmpty(messageList)) {
                            continue;
                        }
                        for (MessageExt message : messageList) {
                            makeMessage(message, list, tableName);
                            if (list.size() >= eventBatchSize) {
                                eventsOffsetConsumer.accept(list, TapSimplify.list());
                                list = TapSimplify.list();
                            }
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
                    value.shutdown();
                } catch (Exception e) {
                    TapLogger.error(TAG, "error occur when shutdown consumer: {}", key, e);
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
    }
}

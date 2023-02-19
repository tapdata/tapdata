package io.tapdata.connector.tidb.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.common.AbstractMqService;
import io.tapdata.common.MqConfig;
import io.tapdata.connector.kafka.admin.Admin;
import io.tapdata.connector.kafka.admin.DefaultAdmin;
import io.tapdata.connector.kafka.config.AdminConfiguration;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.connector.kafka.util.Krb5Util;
import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.constant.MqTestItem;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import io.tapdata.util.NetUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.io.IOException;
import java.time.Duration;
import java.util.*;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.tapdata.base.ConnectorBase.testItem;

public class KafkaService extends AbstractMqService {
private  String connectorId;
    public TestItem testHostAndPort() {
        if (EmptyKit.isBlank(mqConfig.getNameSrvAddr())) {
            try {
                NetUtil.validateHostPortWithSocket(mqConfig.getMqHost(), mqConfig.getMqPort());
                return testItem(MqTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY);
            } catch (IOException e) {
                return testItem(MqTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, e.getMessage());
            }
        } else {
            String[] hostAndPort = mqConfig.getNameSrvAddr().split(",");
            int failedCount = 0;
            for (String hostAndPortItem : hostAndPort) {
                String[] strs = hostAndPortItem.split(":");
                if (strs.length != 2) {
                    return testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_FAILED, "name server address is invalid!");
                } else {
                    try {
                        NetUtil.validateHostPortWithSocket(strs[0], Integer.parseInt(strs[1]));
                    } catch (IOException e) {
                        failedCount++;
                    } catch (NumberFormatException e) {
                        return testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_FAILED, "name server address is invalid!");
                    }
                }
            }
            if (failedCount == 0) {
                return testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_SUCCESSFULLY);
            } else if (failedCount == hostAndPort.length) {
                return testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_FAILED, "all addresses of name server is down!");
            } else {
                return testItem(MqTestItem.NAME_SERVER.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "some addresses of name server is down!");
            }
        }
    }

    public KafkaService(TidbConfig config) {
        super();
        this.config(config);
    }

    public KafkaService config(TidbConfig config) {
        if (Objects.isNull(super.mqConfig)){
            super.mqConfig = new MqConfig();
        }
        super.mqConfig.setNameSrvAddr(config.getNameSrvAddr());
        super.mqConfig.setMqTopicString(config.getMqTopic());
        super.mqConfig.setMqUsername(config.getMqUsername());
        super.mqConfig.setMqPassword(config.getMqPassword());
        return this;
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
            return new TestItem(MqTestItem.KAFKA_MQ_CONNECTION.getContent(), TestItem.RESULT_FAILED, "when connect to cluster, error occurred");
        }

    }

    @Override
    public ConnectionCheckItem testConnection() {
        return null;
    }

    @Override
    public void init() throws Throwable {

    }

    @Override
    public int countTables() throws Throwable {
        return 0;
    }

    @Override
    public void loadTables(int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {

    }

    @Override
    public void produce(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) throws Throwable {

    }

    @Override
    public void consumeOne(TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {

    }

    @Override
    public void streamConsume(List<String> tableList, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
       consuming.set(true);
       // ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration(((KfConfig) mqConfig), "0",true);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, mqConfig.getNameSrvAddr());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        //kafkaConsumer.subscribe(tableList);
        kafkaConsumer.subscribe(Collections.singletonList(mqConfig.getMqTopicString()));
        List<TapEvent> list = TapSimplify.list();
        while (consuming.get()) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.isEmpty()) {
                continue;
            }
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                makeMessage(consumerRecord, list, consumerRecord.topic());
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

    private void makeMessage(ConsumerRecord<String, String> consumerRecord, List<TapEvent> list, String tableName) {
        Map<String, Object> data = new HashMap<String, Object>();
        JSONObject jsonObject = JSON.parseObject(consumerRecord.value());
        data.put("data", jsonObject.getString("data"));
        switch (jsonObject.getString("type")) {
            case "INSERT":
                list.add(new TapInsertRecordEvent().init().table(jsonObject.getString("table")).after(data).referenceTime(System.currentTimeMillis()));
                break;
            case "UPDATE":
                list.add(new TapUpdateRecordEvent().init().table(jsonObject.getString("table")).after(data).referenceTime(System.currentTimeMillis()));
                break;
            case "DELETE":
                list.add(new TapDeleteRecordEvent().init().table(jsonObject.getString("table")).before(data).referenceTime(System.currentTimeMillis()));
                break;
        }
    }

    @Override
    protected <T> Map<String, Object> analyzeTable(Object object, T topic, TapTable tapTable) throws Exception {
        return null;
    }
}

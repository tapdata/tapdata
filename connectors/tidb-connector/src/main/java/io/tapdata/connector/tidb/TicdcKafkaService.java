package io.tapdata.connector.tidb;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.common.MqConfig;
import io.tapdata.common.constant.MqOp;
import io.tapdata.connector.kafka.KafkaService;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.constant.MqTestItem;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
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

import static io.tapdata.base.ConnectorBase.testItem;

public class TicdcKafkaService extends KafkaService {
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
    private String connectorId;
    public TicdcKafkaService(KafkaConfig mqConfig) {
        super();
       this.config(mqConfig);
    }
    public TicdcKafkaService config(KafkaConfig config) {
        if (Objects.isNull(super.mqConfig)){
            super.mqConfig = new MqConfig();
        }
        super.mqConfig.setNameSrvAddr(config.getNameSrvAddr());
        super.mqConfig.setMqTopicString(config.getMqTopicString());
        super.mqConfig.setMqUsername(config.getMqUsername());
        super.mqConfig.setMqPassword(config.getMqPassword());
        return this;
    }
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
        if(EmptyKit.isNotEmpty(list)) {
            eventsOffsetConsumer.accept(list, TapSimplify.list());
        }
        kafkaConsumer.close();
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
}

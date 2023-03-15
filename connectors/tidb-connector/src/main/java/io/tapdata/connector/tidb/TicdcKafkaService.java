package io.tapdata.connector.tidb;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.connector.kafka.KafkaService;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.function.BiConsumer;

public class TicdcKafkaService extends KafkaService {

    private final TidbConfig tidbConfig;

    public TicdcKafkaService(KafkaConfig mqConfig, TidbConfig tidbConfig) {
        super();
        this.tidbConfig = tidbConfig;
        this.mqConfig = mqConfig;
    }

    @Override
    public void streamConsume(List<String> tableList, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        consuming.set(true);
        // ConsumerConfiguration consumerConfiguration = new ConsumerConfiguration(((KfConfig) mqConfig), "0",true);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, mqConfig.getNameSrvAddr());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        //kafkaConsumer.subscribe(tableList);
        kafkaConsumer.subscribe(Collections.singletonList(tidbConfig.getMqTopic()));
        List<TapEvent> list = TapSimplify.list();
        while (consuming.get()) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.isEmpty()) {
                continue;
            }
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                makeMessage(consumerRecord, list);
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

    private void makeMessage(ConsumerRecord<String, String> consumerRecord, List<TapEvent> list) {
        JSONObject jsonObject = JSON.parseObject(consumerRecord.value());
        Map<String, Object> data = new HashMap<>(jsonObject.getJSONArray("data").getJSONObject(0).getInnerMap());
        // TODO: 2023/2/22 get before from old
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

}

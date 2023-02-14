package io.tapdata.connector.tidb.kafka;

import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.connector.tidb.config.TidbConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.UUID;

public class ConsumerConfiguration extends AbstractConfiguration {

    private final boolean isEarliest;
    TidbConfig tidbConfig =new TidbConfig();

    public ConsumerConfiguration(KafkaConfig kafkaConfig, String connectorId, boolean isEarliest) {
        super(kafkaConfig, connectorId);
        this.isEarliest = isEarliest;
    }
    @Override
    public Map<String, Object> build() {
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, tidbConfig.getNameSrvAddr());
        configMap.put(ConsumerConfig.CLIENT_ID_CONFIG, String.format("TapData-KafkaSource-%s-%s", connectorId, UUID.randomUUID()));
        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, String.format("TapData-KafkaSource-%s", this.connectorId));
        configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.isEarliest ? "earliest" : "latest");
//        configMap.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
//        configMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        configMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        configMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

//        if (this.kafkaConfig.getKafkaConsumerUseTransactional()) {
//            configMap.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
//        }
//        if (this.kafkaConfig.getKafkaConsumerRequestTimeout() > 0) {
//            configMap.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, this.kafkaConfig.getKafkaConsumerRequestTimeout());
//        }
//        if (this.kafkaConfig.getKafkaMaxPollRecords() > 0) {
//            configMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.kafkaConfig.getKafkaMaxPollRecords());
//        }
//        if (this.kafkaConfig.getKafkaMaxFetchBytes() > 0) {
//            configMap.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, this.kafkaConfig.getKafkaMaxFetchBytes());
//        }
//        if (this.kafkaConfig.getKafkaMaxFetchWaitMS() > 0) {
//            configMap.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, this.kafkaConfig.getKafkaMaxFetchWaitMS());
//        }
        return super.build();
    }

    public boolean isEarliest() {
        return isEarliest;
    }
}

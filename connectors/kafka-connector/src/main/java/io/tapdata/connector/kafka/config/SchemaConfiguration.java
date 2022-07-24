package io.tapdata.connector.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SchemaConfiguration extends AbstractConfiguration {

    public SchemaConfiguration(KafkaConfig kafkaConfig, String connectorId) {
        super(kafkaConfig, connectorId);
    }

    @Override
    public Map<String, Object> build() {
        final String schemaId = String.format("TapData-KafkaSchema-%s-%s", connectorId, UUID.randomUUID());
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaConfig.getNameSrvAddr());
        configMap.put(ConsumerConfig.CLIENT_ID_CONFIG, schemaId);
        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, schemaId);
        configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configMap.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        configMap.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(10L));
        configMap.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 0);
        configMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        configMap.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1024);
        configMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return super.build();
    }

}

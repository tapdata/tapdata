package io.tapdata.connector.kafka.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ProducerConfiguration extends AbstractConfiguration {

    public ProducerConfiguration(KafkaConfig kafkaConfig, String connectorId) {
        super(kafkaConfig, connectorId);
    }

    @Override
    public Map<String, Object> build() {
        final String tid = String.format("TapData-KafkaTarget-%s-%s", connectorId, UUID.randomUUID());
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaConfig.getNameSrvAddr());
        configMap.put(ProducerConfig.CLIENT_ID_CONFIG, tid);
        configMap.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(5L));
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);
        if (this.kafkaConfig.getKafkaProducerRequestTimeout() > 0) {
            configMap.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, this.kafkaConfig.getKafkaProducerRequestTimeout());
        }
        if (this.kafkaConfig.getKafkaRetries() > 0) {
            configMap.put(ProducerConfig.RETRIES_CONFIG, this.kafkaConfig.getKafkaRetries());
        } else {
            configMap.put(ProducerConfig.RETRIES_CONFIG, 1);
        }
        if (this.kafkaConfig.getKafkaBatchSize() > 0) {
            configMap.put(ProducerConfig.BATCH_SIZE_CONFIG, this.kafkaConfig.getKafkaBatchSize());
        }
        if (StringUtils.equalsAny(this.kafkaConfig.getKafkaAcks(), "0", "1", "-1", "all")) {
            // ENABLE_IDEMPOTENCE_CONFIG is controlled by kafka ack setting
            boolean enableIdempotence = StringUtils.equalsAny(this.kafkaConfig.getKafkaAcks(), "-1", "all");
            configMap.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
            configMap.put(ProducerConfig.ACKS_CONFIG, this.kafkaConfig.getKafkaAcks());
        }
        if (this.kafkaConfig.getKafkaLingerMS() >= 0) {
            configMap.put(ProducerConfig.LINGER_MS_CONFIG, this.kafkaConfig.getKafkaLingerMS());
        }
        if (this.kafkaConfig.getKafkaMaxRequestSize() > 0) {
            configMap.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, this.kafkaConfig.getKafkaMaxRequestSize());
        }
        if (this.kafkaConfig.getKafkaBufferMemory() > 0) {
            configMap.put(ProducerConfig.BUFFER_MEMORY_CONFIG, this.kafkaConfig.getKafkaBufferMemory());
        }
        if (this.kafkaConfig.getKafkaMaxBlockMS() > 0) {
            configMap.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, this.kafkaConfig.getKafkaMaxBlockMS());
        }
        if (this.kafkaConfig.getKafkaDeliveryTimeoutMS() > 0) {
            configMap.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, this.kafkaConfig.getKafkaDeliveryTimeoutMS());
        }
        if (StringUtils.equalsAny(this.kafkaConfig.getKafkaCompressionType(), "gzip", "snappy", "lz4", "zstd")) {
            configMap.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, this.kafkaConfig.getKafkaCompressionType());
        }
        if (this.kafkaConfig.getKafkaProducerUseTransactional()) {
            configMap.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, tid);
        }
        return super.build();
    }

}

package io.tapdata.connector.kafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class AdminConfiguration extends AbstractConfiguration {

    public AdminConfiguration(KafkaConfig kafkaConfig, String connectorId) {
        super(kafkaConfig, connectorId);
    }

    @Override
    public Map<String, Object> build() {
        configMap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaConfig.getNameSrvAddr());
        configMap.put(AdminClientConfig.CLIENT_ID_CONFIG, String.format("TapData-KafkaAdmin-%s-%s", connectorId, UUID.randomUUID()));
        configMap.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(10L));
        return super.build();
    }

}

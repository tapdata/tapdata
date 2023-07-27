package io.tapdata.connector.kafka.config;

import io.tapdata.connector.kafka.util.Krb5Util;
import io.tapdata.kit.EmptyKit;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public abstract class AbstractConfiguration implements KafkaConfiguration {

    protected final KafkaConfig kafkaConfig;
    protected final String connectorId;
    protected final Duration pollTimeout;
    protected final boolean ignoreInvalidRecord;
    final Map<String, Object> configMap = new HashMap<>();

    public AbstractConfiguration(KafkaConfig kafkaConfig, String connectorId) {
        this.kafkaConfig = kafkaConfig;
        this.connectorId = connectorId;
        this.pollTimeout = kafkaConfig.getKafkaPollTimeoutMS() > 0 ? Duration.ofMillis(kafkaConfig.getKafkaPollTimeoutMS()) : Duration.ofSeconds(1L);
        this.ignoreInvalidRecord = kafkaConfig.getKafkaIgnoreInvalidRecord();
    }

    public Map<String, Object> build() {
        if (kafkaConfig.getKrb5()) {
            String krb5Path = Krb5Util.saveByCatalog("connections-" + connectorId, kafkaConfig.getKrb5Keytab(), kafkaConfig.getKrb5Conf(), true);
            Krb5Util.updateKafkaConf(kafkaConfig.getKrb5ServiceName(), kafkaConfig.getKrb5Principal(), krb5Path, kafkaConfig.getKrb5Conf(), configMap);
        } else if (EmptyKit.isNotEmpty(this.kafkaConfig.getMqUsername()) && EmptyKit.isNotEmpty(this.kafkaConfig.getMqPassword())) {
            configMap.put("security.protocol", "SASL_PLAINTEXT");
            String saslMechanism;
            String model;
            switch (kafkaConfig.getKafkaSaslMechanism().toUpperCase()) {
                case "PLAIN":
                    saslMechanism = "PLAIN";
                    model = "org.apache.kafka.common.security.plain.PlainLoginModule";
                    break;
                case "SHA256":
                    saslMechanism = "SCRAM-SHA-256";
                    model = "org.apache.kafka.common.security.scram.ScramLoginModule";
                    break;
                case "SHA512":
                    saslMechanism = "SCRAM-SHA-512";
                    model = "org.apache.kafka.common.security.scram.ScramLoginModule";
                    break;
                default:
                    throw new IllegalArgumentException("Un-supported sasl.mechanism: " + kafkaConfig.getKafkaSaslMechanism().toUpperCase());
            }
            configMap.put("sasl.mechanism", saslMechanism);
            configMap.put("sasl.jaas.config", model + " required " +
                    "username='" + this.kafkaConfig.getMqUsername() +
                    "' password='" + this.kafkaConfig.getMqPassword() + "';");
        }
        return configMap;
    }

    @Override
    public Set<String> getRawTopics() {
        return this.kafkaConfig.getKafkaRawTopics();
    }

    @Override
    public Pattern getPatternTopics() {
        return Optional.ofNullable(this.kafkaConfig.getMqTopicString()).map(Pattern::compile).orElse(null);
    }

    @Override
    public boolean hasRawTopics() {
        return this.kafkaConfig.getKafkaRawTopics() != null && !this.kafkaConfig.getKafkaRawTopics().isEmpty();
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public boolean isIgnoreInvalidRecord() {
        return ignoreInvalidRecord;
    }

    @Override
    public KafkaConfig getConfig() {
        return this.kafkaConfig;
    }

}

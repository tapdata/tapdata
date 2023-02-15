package io.tapdata.connector.tidb.kafka;

import io.tapdata.connector.kafka.config.KafkaConfig;

import java.util.Set;

public class KfConfig extends KafkaConfig {
    /**
     * kafka common
     */
    private Set<String> kafkaRawTopics;
    private String kafkaSaslMechanism;
    private Boolean krb5;
    private String krb5Keytab;
    private String krb5Conf;
    private String krb5Principal;
    private String krb5ServiceName;

    /**
     * kafka source (Consumer)
     */
    private Integer kafkaConsumerRequestTimeout = 0;
    private Boolean kafkaConsumerUseTransactional = false;
    private Integer kafkaMaxPollRecords = 0;
    private Integer kafkaPollTimeoutMS = 0;
    private Integer kafkaMaxFetchBytes = 0;
    private Integer kafkaMaxFetchWaitMS = 0;
    private Boolean kafkaIgnoreInvalidRecord = false;

    public Set<String> getKafkaRawTopics() {
        return kafkaRawTopics;
    }

    public void setKafkaRawTopics(Set<String> kafkaRawTopics) {
        this.kafkaRawTopics = kafkaRawTopics;
    }

    public String getKafkaSaslMechanism() {
        return kafkaSaslMechanism;
    }

    public void setKafkaSaslMechanism(String kafkaSaslMechanism) {
        this.kafkaSaslMechanism = kafkaSaslMechanism;
    }

    public Boolean getKrb5() {
        return krb5;
    }

    public void setKrb5(Boolean krb5) {
        this.krb5 = krb5;
    }

    public String getKrb5Keytab() {
        return krb5Keytab;
    }

    public void setKrb5Keytab(String krb5Keytab) {
        this.krb5Keytab = krb5Keytab;
    }

    public String getKrb5Conf() {
        return krb5Conf;
    }

    public void setKrb5Conf(String krb5Conf) {
        this.krb5Conf = krb5Conf;
    }

    public String getKrb5Principal() {
        return krb5Principal;
    }

    public void setKrb5Principal(String krb5Principal) {
        this.krb5Principal = krb5Principal;
    }

    public String getKrb5ServiceName() {
        return krb5ServiceName;
    }

    public void setKrb5ServiceName(String krb5ServiceName) {
        this.krb5ServiceName = krb5ServiceName;
    }

    public Integer getKafkaConsumerRequestTimeout() {
        return kafkaConsumerRequestTimeout;
    }

    public void setKafkaConsumerRequestTimeout(Integer kafkaConsumerRequestTimeout) {
        this.kafkaConsumerRequestTimeout = kafkaConsumerRequestTimeout;
    }

    public Boolean getKafkaConsumerUseTransactional() {
        return kafkaConsumerUseTransactional;
    }

    public void setKafkaConsumerUseTransactional(Boolean kafkaConsumerUseTransactional) {
        this.kafkaConsumerUseTransactional = kafkaConsumerUseTransactional;
    }

    public Integer getKafkaMaxPollRecords() {
        return kafkaMaxPollRecords;
    }

    public void setKafkaMaxPollRecords(Integer kafkaMaxPollRecords) {
        this.kafkaMaxPollRecords = kafkaMaxPollRecords;
    }

    public Integer getKafkaPollTimeoutMS() {
        return kafkaPollTimeoutMS;
    }

    public void setKafkaPollTimeoutMS(Integer kafkaPollTimeoutMS) {
        this.kafkaPollTimeoutMS = kafkaPollTimeoutMS;
    }

    public Integer getKafkaMaxFetchBytes() {
        return kafkaMaxFetchBytes;
    }

    public void setKafkaMaxFetchBytes(Integer kafkaMaxFetchBytes) {
        this.kafkaMaxFetchBytes = kafkaMaxFetchBytes;
    }

    public Integer getKafkaMaxFetchWaitMS() {
        return kafkaMaxFetchWaitMS;
    }

    public void setKafkaMaxFetchWaitMS(Integer kafkaMaxFetchWaitMS) {
        this.kafkaMaxFetchWaitMS = kafkaMaxFetchWaitMS;
    }

    public Boolean getKafkaIgnoreInvalidRecord() {
        return kafkaIgnoreInvalidRecord;
    }

    public void setKafkaIgnoreInvalidRecord(Boolean kafkaIgnoreInvalidRecord) {
        this.kafkaIgnoreInvalidRecord = kafkaIgnoreInvalidRecord;
    }
}

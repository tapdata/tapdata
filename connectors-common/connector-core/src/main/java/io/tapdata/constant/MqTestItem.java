package io.tapdata.constant;

public enum MqTestItem {

    HOST_PORT("Check host port or nameSrvAddr is invalid"),
    NAME_SERVER("Check name server is invalid"),
    ACTIVE_MQ_CONNECTION("Check activemq service is invalid"),
    RABBIT_MQ_CONNECTION("Check rabbitmq service is invalid"),
    ROCKET_MQ_CONNECTION("Check rocketmq service is invalid"),
    KAFKA_BASE64_CONNECTION("Test Kafka KDC Domains Base64"),
    KAFKA_MQ_CONNECTION("Check Kafka service is invalid"),
    KAFKA_SCHEMA_REGISTER_CONNECTION("Check Kafka schema register service is invalid"),
    ;

    private final String content;

    MqTestItem(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }

}

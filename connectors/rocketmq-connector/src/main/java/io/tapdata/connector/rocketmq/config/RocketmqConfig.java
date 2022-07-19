package io.tapdata.connector.rocketmq.config;

import io.tapdata.common.MqConfig;

import java.io.Serializable;

public class RocketmqConfig extends MqConfig implements Serializable {

    private String producerGroup;
    private String consumerGroup;

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }
}

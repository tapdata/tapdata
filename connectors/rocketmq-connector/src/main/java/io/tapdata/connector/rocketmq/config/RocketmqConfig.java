package io.tapdata.connector.rocketmq.config;

import io.tapdata.common.MqConfig;

import java.io.Serializable;

public class RocketmqConfig extends MqConfig implements Serializable {

    private String productGroup;
    private String consumerGroup;

    public String getProductGroup() {
        return productGroup;
    }

    public void setProductGroup(String productGroup) {
        this.productGroup = productGroup;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }
}

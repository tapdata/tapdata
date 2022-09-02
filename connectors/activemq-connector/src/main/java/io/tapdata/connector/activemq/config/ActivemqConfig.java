package io.tapdata.connector.activemq.config;

import io.tapdata.common.MqConfig;

public class ActivemqConfig extends MqConfig {

    private String brokerURL;

    public String getBrokerURL() {
        return brokerURL;
    }

    @Override
    public String getConnectionString() {
        return brokerURL;
    }

    public void setBrokerURL(String brokerURL) {
        this.brokerURL = brokerURL;
    }
}

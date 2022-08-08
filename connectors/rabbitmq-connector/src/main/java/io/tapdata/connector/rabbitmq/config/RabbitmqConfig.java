package io.tapdata.connector.rabbitmq.config;

import io.tapdata.common.MqConfig;

import java.io.Serializable;

public class RabbitmqConfig extends MqConfig implements Serializable {

    private String routeKeyField;
    private String virtualHost;
    private int apiPort;

    public String getRouteKeyField() {
        return routeKeyField;
    }

    public void setRouteKeyField(String routeKeyField) {
        this.routeKeyField = routeKeyField;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public int getApiPort() {
        return apiPort;
    }

    public void setApiPort(int apiPort) {
        this.apiPort = apiPort;
    }
}

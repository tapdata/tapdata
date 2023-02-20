package io.tapdata.js.connector.server.function;

import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ExecuteConfig {
    public static final String CONNECTION_CONFIG = "$_CONNECTION_CONFIG_$";
    public static final String NODE_CONFIG = "$_NODE_CONFIG_$";
    private Map<String, Object> connectionConfig;
    private Map<String, Object> nodeConfig;

    public static ExecuteConfig contextConfig(TapConnectionContext context) {
        return new ExecuteConfig(context);
    }

    protected ExecuteConfig(TapConnectionContext context) {
        if (Objects.isNull(context)) return;
        this.connectionConfig = context.getConnectionConfig();
        this.nodeConfig = context.getNodeConfig();
    }

    public ExecuteConfig connection(Map<String, Object> context) {
        this.connectionConfig = context;
        return this;
    }

    public ExecuteConfig node(Map<String, Object> context) {
        this.nodeConfig = context;
        return this;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        if (Objects.nonNull(this.connectionConfig) && !this.connectionConfig.isEmpty()) {
            map.putAll(this.connectionConfig);
        }
        if (Objects.nonNull(this.nodeConfig) && !this.nodeConfig.isEmpty()) {
            map.putAll(this.nodeConfig);
        }
        return map;
    }
}

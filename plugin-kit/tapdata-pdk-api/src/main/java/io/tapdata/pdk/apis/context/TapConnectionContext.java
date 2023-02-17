package io.tapdata.pdk.apis.context;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.Map;
import java.util.Objects;

public class TapConnectionContext extends TapContext {
    protected DataMap connectionConfig;
    protected DataMap nodeConfig;

    public TapConnectionContext(TapNodeSpecification specification, DataMap connectionConfig, DataMap nodeConfig, Log log) {
        super(specification);
        this.connectionConfig = connectionConfig;
        this.nodeConfig = nodeConfig;
        this.log = log;
    }

    public DataMap getConnectionConfig() {
        return connectionConfig;
    }

    public void setConnectionConfig(DataMap connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    public DataMap getNodeConfig() {
        return nodeConfig;
    }

    public void setNodeConfig(DataMap nodeConfig) {
        this.nodeConfig = nodeConfig;
    }


    public String toString() {
        return "TapConnectionContext connectionConfig: " + (connectionConfig != null ? Objects.requireNonNull(InstanceFactory.instance(JsonParser.class)).toJson(connectionConfig) : "") + "nodeConfig: " + (nodeConfig != null ? Objects.requireNonNull(InstanceFactory.instance(JsonParser.class)).toJson(nodeConfig) : "") + " spec: " + specification + " id: " + id;
    }
}

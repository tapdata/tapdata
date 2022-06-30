package io.tapdata.pdk.apis.context;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

public class TapConnectionContext extends TapContext {
    protected DataMap connectionConfig;

    public TapConnectionContext(TapNodeSpecification specification, DataMap connectionConfig) {
        super(specification);
        this.connectionConfig = connectionConfig;
    }

    public DataMap getConnectionConfig() {
        return connectionConfig;
    }

    public void setConnectionConfig(DataMap connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    public String toString() {
        return "TapConnectionContext connectionConfig: " + (connectionConfig != null ? InstanceFactory.instance(JsonParser.class).toJson(connectionConfig) : "") + " spec: " + specification + " id: " + id;
    }
}

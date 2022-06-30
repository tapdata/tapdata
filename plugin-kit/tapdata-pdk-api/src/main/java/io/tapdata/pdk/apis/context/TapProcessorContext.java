package io.tapdata.pdk.apis.context;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

public class TapProcessorContext extends TapContext {
    private DataMap nodeConfig;

    public TapProcessorContext(TapNodeSpecification specification, DataMap nodeConfig) {
        super(specification);
        this.nodeConfig = nodeConfig;
    }

    public DataMap getNodeConfig() {
        return nodeConfig;
    }

    public void setNodeConfig(DataMap nodeConfig) {
        this.nodeConfig = nodeConfig;
    }
}

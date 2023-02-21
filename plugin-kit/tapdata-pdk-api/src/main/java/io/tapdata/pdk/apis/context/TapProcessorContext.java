package io.tapdata.pdk.apis.context;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

public class TapProcessorContext extends TapContext {
    private DataMap nodeConfig;

    public TapProcessorContext(TapNodeSpecification specification, DataMap nodeConfig, Log log) {
        super(specification);
        this.nodeConfig = nodeConfig;
        this.log = log;
    }

    public DataMap getNodeConfig() {
        return nodeConfig;
    }

    public void setNodeConfig(DataMap nodeConfig) {
        this.nodeConfig = nodeConfig;
    }
}

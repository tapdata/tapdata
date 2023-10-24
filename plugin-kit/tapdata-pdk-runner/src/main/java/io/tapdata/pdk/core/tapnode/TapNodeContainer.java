package io.tapdata.pdk.core.tapnode;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.Map;

public class TapNodeContainer {
    private TapNodeSpecification properties;

    private DataMap configOptions; //include connection and node

    private Map<String, DataMap> dataTypes;

    private Map<String, Object> messages;

    public DataMap getConfigOptions() {
        return configOptions;
    }

    public void setConfigOptions(DataMap configOptions) {
        this.configOptions = configOptions;
    }

    public TapNodeSpecification getProperties() {
        return properties;
    }

    public void setProperties(TapNodeSpecification properties) {
        this.properties = properties;
    }

    public Map<String, DataMap> getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(Map<String, DataMap> dataTypes) {
        this.dataTypes = dataTypes;
    }

    public Map<String, Object> getMessages() {
        return messages;
    }

    public void setMessages(Map<String, Object> messages) {
        this.messages = messages;
    }
}

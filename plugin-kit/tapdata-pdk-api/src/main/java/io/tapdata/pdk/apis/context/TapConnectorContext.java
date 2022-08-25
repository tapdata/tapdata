package io.tapdata.pdk.apis.context;


import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;


public class TapConnectorContext extends TapConnectionContext {
    protected ConnectorCapabilities connectorCapabilities;
    protected DataMap nodeConfig;
    protected KVReadOnlyMap<TapTable> tableMap;
    protected KVMap<Object> stateMap;
    protected KVMap<Object> globalStateMap;
    protected ConfigContext configContext;

    public TapConnectorContext(TapNodeSpecification specification, DataMap connectionConfig, DataMap nodeConfig) {
        super(specification, connectionConfig);
        this.nodeConfig = nodeConfig;
    }

    public void configContext() {
        if(configContext != null) {
            configContext.config();
        }
    }

    public DataMap getNodeConfig() {
        return nodeConfig;
    }

    public void setNodeConfig(DataMap nodeConfig) {
        this.nodeConfig = nodeConfig;
    }

    public KVReadOnlyMap<TapTable> getTableMap() {
        return tableMap;
    }

    public void setTableMap(KVReadOnlyMap<TapTable> tableMap) {
        this.tableMap = tableMap;
    }

    public KVMap<Object> getStateMap() {
        return stateMap;
    }

    public void setStateMap(KVMap<Object> stateMap) {
        this.stateMap = stateMap;
    }

    public KVMap<Object> getGlobalStateMap() {
            return globalStateMap;
    }

    public void setGlobalStateMap(KVMap<Object> globalStateMap) {
            this.globalStateMap = globalStateMap;
    }

    public ConnectorCapabilities getConnectorCapabilities() {
        return connectorCapabilities;
    }

    public void setConnectorCapabilities(ConnectorCapabilities connectorCapabilities) {
        this.connectorCapabilities = connectorCapabilities;
    }

    public ConfigContext getConfigContext() {
        return configContext;
    }

    public void setConfigContext(ConfigContext configContext) {
        this.configContext = configContext;
    }

    public String toString() {
        return "TapConnectorContext " + "connectionConfig: " + (connectionConfig != null ? InstanceFactory.instance(JsonParser.class).toJson(connectionConfig) : "") + " nodeConfig: " + (nodeConfig != null ? InstanceFactory.instance(JsonParser.class).toJson(nodeConfig) : "") + " spec: " + specification + " id: " + id;
    }
}

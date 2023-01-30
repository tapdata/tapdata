package io.tapdata.bigquery.service.stream.v2;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class StateMapOperator {
    private TapConnectorContext context;
    private KVMap<Object> stateMap;
    public static final String TABLE_CONFIG_NAME = "BIGQUERY_CONFIG";
    private Map<String, Map<String,Object>> tableConfig ;
    private String tableId;

    public static StateMapOperator operator(TapConnectorContext context) {
        if (Objects.isNull(context)) throw new CoreException("TapConnectorContext cannot be empty.");
        return new StateMapOperator().context(context);
    }

    public StateMapOperator context(TapConnectorContext context) {
        this.context = context;
        this.stateMap = context.getStateMap();
        if (Objects.isNull(this.stateMap)) throw new CoreException("StateMap cannot be empty.");
        this.tableConfig();
        return this;
    }

    public TapConnectorContext context() {
        return this.context;
    }

    public KVMap<Object> stateMap() {
        return this.stateMap;
    }

    public StateMapOperator save(String key, Object value) {
        this.stateMap.put(key, value);
        return this;
    }

    public Object get(String key) {
        return this.stateMap.get(key);
    }

    public Object getOfTable(String tableId,String key){
        Map<String,Map<String,Object>> config = this.tableConfig();
        if (Objects.isNull(config)) return null;
        Map<String, Object> objectMap = config.get(tableId);
        return Objects.isNull(objectMap) ? null : objectMap.get(key);
    }
    public StateMapOperator saveForTable(String tableId,String key, Object value){
        Map<String,Map<String,Object>> config = this.tableConfig();
        if (Objects.isNull(config)) config = new HashMap<String,Map<String,Object>>();
        Map<String, Object> objectMap = config.get(tableId);
        if (Objects.isNull(objectMap)) objectMap = new HashMap<>();
        objectMap.put(key,value);
        this.save(StateMapOperator.TABLE_CONFIG_NAME,config);
        return this;
    }

    public String getString(String key) {
        Object value = this.stateMap.get(key);
        return Objects.isNull(value) ? null : String.valueOf(value);
    }

    public Long getLong(String key) {
        Object value = this.stateMap.get(key);
        return Objects.nonNull(value) && (value instanceof Long) ? (Long) value : null;
    }
    public String getString(String tableId,String key, boolean fromCache) {
        Object value = fromCache ? this.getFromCache(tableId, key) : this.getOfTable(tableId, key);
        return Objects.isNull(value) ? null : String.valueOf(value);
    }

    public Long getLong(String tableId, String key,boolean fromCache) {
        Object value = fromCache ? this.getFromCache(tableId, key) : this.getOfTable(tableId, key);
        return Objects.nonNull(value) && (value instanceof Long) ? (Long) value : null;
    }

    public Map<String, Map<String,Object>> tableConfig(){
        if (Objects.isNull(this.stateMap)){
            this.stateMap = context.getStateMap();
        }
        Object obj = this.stateMap.get(StateMapOperator.TABLE_CONFIG_NAME);
        return this.tableConfig = Objects.isNull(obj)?null:(Map<String, Map<String, Object>>) obj;
    }

    private Object getFromCache(String tableId, String key){
        Map<String, Object> objectMap = this.tableConfig.get(tableId);
        return Objects.isNull(objectMap)?null:objectMap.get(key);
    }
}

package io.tapdata.bigquery.service.stream.v2;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class StateMapOperator {
    private TapConnectorContext context;
    private KVMap<Object> stateMap;
    public static final String TABLE_CONFIG_NAME = "_BIGQUERY_CONFIG_";
    //private Map<String, Map<String,Object>> tableConfig ;
    private String tableId;

    /**
     * {
     * "_BIGQUERY_CONFIG_":{
     * "tableID1":{
     * "TemporaryTable":"temp_table_cxk1",
     * "STREAM_TO_BATCH_TIME":19999999999,
     * "merge_id":12121222,
     * "merge_id_last":1999999999999
     * },
     * "tableID2":{
     * "TemporaryTable":"temp_table_cxk2",
     * "STREAM_TO_BATCH_TIME":19999999999
     * }
     * }
     * }
     */

    public static StateMapOperator operator(TapConnectorContext context) {
        if (Objects.isNull(context)) throw new CoreException("TapConnectorContext cannot be empty.");
        return new StateMapOperator().context(context);
    }

    public StateMapOperator context(TapConnectorContext context) {
        this.context = context;
        this.stateMap = context.getStateMap();
        if (Objects.isNull(this.stateMap)) throw new CoreException("StateMap cannot be empty.");
        //this.tableConfig();
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

    public Object getOfTable(String tableId, String key) {
        Map<String,Object> stateObj = this.tableConfig(tableId);
        if (Objects.isNull(stateObj)) return null;
        return stateObj.get(key);
    }

    public StateMapOperator saveForTable(String tableId, String key, Object value) {
        Map<String,Object> stateObj = this.tableConfig(tableId);
        Map<String, Object> objectMap = Objects.isNull(stateObj) ? new HashMap<String, Object>() : stateObj;
        objectMap.put(key, value);
        this.saveTableConfig(tableId, objectMap);
        return this;
    }

    public StateMapOperator saveForTable(String tableId, Map<String, Object> config) {
        Map<String,Object> stateObj = this.tableConfig(tableId);
        if (Objects.isNull(stateObj)) {
            this.saveTableConfig(tableId, config);
        } else {
            stateObj.putAll(config);
            this.saveTableConfig(tableId, stateObj);
        }
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

    public String getString(String tableId, String key) {
        Object value = this.getOfTable(tableId, key);
        return Objects.isNull(value) ? null : String.valueOf(value);
    }

    public Long getLong(String tableId, String key) {
        Object value = this.getOfTable(tableId, key);
        return Objects.nonNull(value) && (value instanceof Long) ? (Long) value : null;
    }

    public Map<String, Object> tableConfig() {
        if (Objects.isNull(this.stateMap)) {
            this.stateMap = context.getStateMap();
        }
        Object obj = this.stateMap.get(StateMapOperator.TABLE_CONFIG_NAME);
        return Objects.isNull(obj) ? new HashMap<>() : (Map<String, Object>) obj;
    }

    public Map<String, Object> tableConfig(String tableId) {
        Map<String, Object> tableConfig = this.tableConfig();
        if (Objects.isNull(tableConfig)) return null;
        Object table = tableConfig.get(tableId);
        return Objects.isNull(table) ? null : (Map<String, Object>) table;
    }


    public StateMapOperator saveTableConfig(String tableId, Map<String,Object> value) {
        Map<String, Object> objectMap = tableConfig();
        objectMap.put(tableId,value);
        this.stateMap.put(StateMapOperator.TABLE_CONFIG_NAME, objectMap);
        return this;
    }
//
//    private Object getFromCache(String tableId, String key){
//        Map<String, Object> objectMap = this.tableConfig.get(tableId);
//        return Objects.isNull(objectMap)?null:objectMap.get(key);
//    }
}

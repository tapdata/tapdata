package io.tapdata.bigquery.service.stream.handle;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.Objects;

public class StateMapOperator {
    private TapConnectorContext context;
    private KVMap<Object> stateMap;

    public static StateMapOperator operator(TapConnectorContext context) {
        if (Objects.isNull(context)) throw new CoreException("TapConnectorContext cannot be empty.");
        return new StateMapOperator().context(context);
    }

    public StateMapOperator context(TapConnectorContext context) {
        this.context = context;
        this.stateMap = context.getStateMap();
        if (Objects.isNull(this.stateMap)) throw new CoreException("StateMap cannot be empty.");
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

    public String getString(String key) {
        Object value = this.stateMap.get(key);
        return Objects.isNull(value) ? null : String.valueOf(value);
    }
}

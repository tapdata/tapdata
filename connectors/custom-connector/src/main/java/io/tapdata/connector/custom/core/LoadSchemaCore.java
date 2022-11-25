package io.tapdata.connector.custom.core;

import io.tapdata.exception.StopException;
import io.tapdata.kit.EmptyKit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * core for schema(sample)
 */
public class LoadSchemaCore extends Core {

    private final Map<String, Object> data;

    public LoadSchemaCore() {
        super();
        data = new HashMap<>();
    }

    @Override
    public void push(List<Object> data, String op, Object contextMap) {
        if (EmptyKit.isNotEmpty(data)) {
            for (Object datum : data) {
                if (datum instanceof Map) {
                    if (EmptyKit.isNotEmpty((Map) datum)) {
                        this.data.putAll(((Map) datum));
                        //sample first row, then stop
                        throw new StopException();
                    }
                }
            }
        }
    }

    public Map<String, Object> getData() {
        return data;
    }
}

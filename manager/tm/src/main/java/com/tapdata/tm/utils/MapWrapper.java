package com.tapdata.tm.utils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/2/3 下午11:34
 * @description
 */
public class MapWrapper {
    private Object value;
    public MapWrapper(Object collection) {
        this.value = collection;
    }

    public MapWrapper get(String key) {
        if (value != null && value instanceof Map) {
            return new MapWrapper(((Map)value).get(key));
        }
        return new MapWrapper(null);
    }

    public Object get() {
        return value;
    }

    public String getAsString() {
        if (value != null) {
            return String.valueOf(value);
        }
        return null;
    }

    public Integer getAsInt() {
        if (value != null) {
            if (value instanceof Long)
                return ((Long) value).intValue();
            else if (value instanceof Integer)
                return (Integer) value;
            else if (value instanceof String)
                return Integer.valueOf((String)value);
            else if (value instanceof Double)
                return ((Double)value).intValue();
            else if (value instanceof BigDecimal)
                return ((BigDecimal)value).intValue();
        }
        return null;
    }

    public Long getAsLong() {
        if (value != null) {
            if (value instanceof Long)
                return (Long) value;
            else if (value instanceof Integer)
                return Long.valueOf((Integer) value);
            else if (value instanceof String)
                return Long.valueOf((String)value);
            else if (value instanceof Double)
                return ((Double)value).longValue();
            else if (value instanceof BigDecimal)
                return ((BigDecimal)value).longValue();
        }
        return null;
    }

    public List<MapWrapper> getAsList() {
        if (value != null && value instanceof List) {
            List<MapWrapper> result = new ArrayList<MapWrapper>(){
                @Override
                public MapWrapper get(int index) {
                    if (index >= size()) {
                        return new MapWrapper(null);
                    }
                    return super.get(index);
                }
            };
            ((List) value).forEach( item -> result.add(new MapWrapper(item)));
            return result;
        }
        return null;
    }
}

package com.tapdata.tm.metrics.constant;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/9 下午4:45
 */
public enum MetricsType {
    COUNTER("Counter"),
    GAUGE("Gauge"),
    HISTOGRAM("Histogram"),
    SUMMARY("Summary"),
    ;

    private static Map<String, MetricsType> typeMap;

    static {
        typeMap = new HashMap<>();
        for (MetricsType value : MetricsType.values()) {
            typeMap.put(value.type, value);
        }
    }

    public static MetricsType fromType(String type) {
        return typeMap.get(type);
    }

    private String type;

    private MetricsType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}

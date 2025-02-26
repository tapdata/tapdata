package com.tapdata.huawei.drs.kafka;

/**
 * 存储类型
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/17 16:35 Create
 */
public enum StoreType {
    AVRO,
    JSON,
    JSON_C,
    UNDEFINED,
    ;

    public static StoreType fromValue(String type) {
        if (null == type || type.isEmpty()) return JSON;
        for (StoreType t : values()) {
            if (t.name().equals(type)) return t;
        }
        return UNDEFINED;
    }
}

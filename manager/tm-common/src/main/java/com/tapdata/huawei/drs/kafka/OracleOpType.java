package com.tapdata.huawei.drs.kafka;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/22 16:32 Create
 */
public enum OracleOpType {
    INSERT,
    UPDATE,
    DELETE,
    DDL,
    UNDEFINED,
    ;

    public static OracleOpType fromValue(String type) {
        for (OracleOpType t : values()) {
            if (t.name().equals(type)) return t;
        }
        return UNDEFINED;
    }
}

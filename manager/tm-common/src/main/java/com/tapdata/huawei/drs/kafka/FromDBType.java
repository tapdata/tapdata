package com.tapdata.huawei.drs.kafka;

/**
 * 数据来源
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/17 16:48 Create
 */
public enum FromDBType {

    MYSQL,
    GAUSSDB_MYSQL,
    GAUSSDB,
    ORACLE,
    MSSQL,
    POSTGRESQL,
    UNDEFINED,
    ;

    public static FromDBType fromValue(String type) {
        if (null == type || type.isEmpty()) return MYSQL;

        for (FromDBType t : values()) {
            if (t.name().equals(type)) return t;
        }
        return UNDEFINED;
    }
}

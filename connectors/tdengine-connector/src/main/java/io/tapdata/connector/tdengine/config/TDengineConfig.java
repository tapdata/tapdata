package io.tapdata.connector.tdengine.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

/**
 * TDengine database config
 */
public class TDengineConfig extends CommonDbConfig implements Serializable {

    //customize
    public TDengineConfig() {
        setDbType("TAOS-RS");
        setJdbcDriver("com.taosdata.jdbc.rs.RestfulDriver");
    }

}

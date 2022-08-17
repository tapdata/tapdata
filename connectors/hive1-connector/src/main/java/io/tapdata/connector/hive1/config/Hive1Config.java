package io.tapdata.connector.hive1.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

public class Hive1Config extends CommonDbConfig implements Serializable {


    public Hive1Config() {
        setDbType("hive2");
        setJdbcDriver("org.apache.hive.jdbc.HiveDriver");
    }
}

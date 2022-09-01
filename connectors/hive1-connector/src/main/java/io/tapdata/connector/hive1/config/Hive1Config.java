package io.tapdata.connector.hive1.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

public class Hive1Config extends CommonDbConfig implements Serializable {


    private String hiveConnType;

    public Hive1Config() {
        setDbType("hive2");
        setJdbcDriver("org.apache.hive.jdbc.HiveDriver");
    }

    public String getHiveConnType() {
        return hiveConnType;
    }

    public void setHiveConnType(String hiveConnType) {
        this.hiveConnType = hiveConnType;
    }
}

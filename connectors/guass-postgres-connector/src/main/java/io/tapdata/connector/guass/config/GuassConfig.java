package io.tapdata.connector.guass.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

public class GuassConfig extends CommonDbConfig implements Serializable {
    private String logPluginName = "pgoutput"; //default log plugin for postgres, pay attention to lower version

    //customize
    public GuassConfig() {
        setDbType("postgresql");
        setJdbcDriver("org.postgresql.Driver");
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public void setLogPluginName(String logPluginName) {
        this.logPluginName = logPluginName;
    }


}

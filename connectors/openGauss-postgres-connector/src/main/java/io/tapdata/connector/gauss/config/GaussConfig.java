package io.tapdata.connector.gauss.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

public class GaussConfig extends CommonDbConfig implements Serializable {
    private String logPluginName = "pgoutput"; //default log plugin for postgres, pay attention to lower version

    //customize
    public GaussConfig() {
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

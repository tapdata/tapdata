package io.tapdata.connector.guass;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

public class GuassConfig extends CommonDbConfig implements Serializable {
    private String logPluginName = "gsoutput"; //default log plugin for postgres, pay attention to lower version

    //customize
    public GuassConfig() {
        setDbType("guass");
        setJdbcDriver("org.guass.Driver");
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public void setLogPluginName(String logPluginName) {
        this.logPluginName = logPluginName;
    }


}

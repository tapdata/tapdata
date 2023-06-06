package io.tapdata.connector.gauss.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

/**
 * Author:Skeet
 * Date: 2023/6/5
 **/
public class GaussConfig extends CommonDbConfig implements Serializable {
    private String logPluginName = "pgoutput"; //default log plugin for postgres, pay attention to lower version
    private Boolean closeNotNull = true;


    public GaussConfig() {
        setDbType("postgresql");
        setJdbcDriver("org.postgresql.Driver");
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public Boolean getCloseNotNull() {
        return closeNotNull;
    }
}

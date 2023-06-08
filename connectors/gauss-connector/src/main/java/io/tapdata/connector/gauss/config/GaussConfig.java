package io.tapdata.connector.gauss.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

/**
 * Author:Skeet
 * Date: 2023/6/5
 **/
public class GaussConfig extends CommonDbConfig implements Serializable {
    private String databaseUrlPattern = "jdbc:opengauss://%s:%s/postgres";
    private String logPluginName = "pgoutput"; //default log plugin for postgres, pay attention to lower version
    private Boolean closeNotNull = true;


    public GaussConfig() {
        setDbType("postgresql");
        setJdbcDriver("com.huawei.opengauss.jdbc.Driver");
    }

    public String getDatabaseUrl() {
        return String.format(databaseUrlPattern, this.getHost(), this.getPort());
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public Boolean getCloseNotNull() {
        return closeNotNull;
    }
}

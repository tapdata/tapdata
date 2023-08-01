package io.tapdata.connector.postgres.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

/**
 * Postgres database config
 *
 * @author Jarad
 * @date 2022/4/18
 */
public class PostgresConfig extends CommonDbConfig implements Serializable {

    private String logPluginName = "pgoutput"; //default log plugin for postgres, pay attention to lower version
    private Boolean closeNotNull = false;

    //customize
    public PostgresConfig() {
        setDbType("postgresql");
        setJdbcDriver("org.postgresql.Driver");
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public void setLogPluginName(String logPluginName) {
        this.logPluginName = logPluginName;
    }

    public Boolean getCloseNotNull() {
        return closeNotNull;
    }

    public void setCloseNotNull(Boolean closeNotNull) {
        this.closeNotNull = closeNotNull;
    }
}

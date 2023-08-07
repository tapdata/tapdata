package io.tapdata.connector.dws.config;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.dws.DwsJdbcContext;
import io.tapdata.entity.schema.TapTable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Postgres database config
 *
 * @author Jarad
 * @date 2022/4/18
 */
public class DwsConfig extends CommonDbConfig implements Serializable {

    private String logPluginName = "pgoutput"; //default log plugin for postgres, pay attention to lower version


    //customize
    public DwsConfig() {
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

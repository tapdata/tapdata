package io.tapdata.connector.selectdb.config;

import io.tapdata.common.CommonDbConfig;
import lombok.Getter;
import lombok.Setter;


import java.io.Serializable;

/**
 * Author:Skeet
 * Date: 2022/12/12
 **/

@Setter
@Getter
public class SelectDbConfig extends CommonDbConfig implements Serializable {

    private String logPluginName = "selectdboutput";
    private String SelectDbHttp;

    public SelectDbConfig() {
        setDbType("mysql");
        setJdbcDriver("com.mysql.cj.jdbc.Driver");
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public void setLogPluginName(String logPluginName) {
        this.logPluginName = logPluginName;
    }
}

package io.tapdata.connector.selectdb.config;

import io.tapdata.common.CommonDbConfig;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Author:Skeet
 * Date: 2022/12/12
 **/

@Setter
@Getter
public class SelectDbConfig extends CommonDbConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private String databaseUrlPattern = "jdbc:mysql://%s:%s/%s?rewriteBatchedStatements=true";
    private String logPluginName = "selectdboutput";
    private int insertBatchSize = 1000;
    private String selectDbHttp;
    private Boolean closeNotNull = true;
    private int retryCount = 30;

    public SelectDbConfig selectDbHttp(String selectDbHttp) {
        this.selectDbHttp = selectDbHttp;
        return this;
    }

    public SelectDbConfig() {
        setDbType("doris");
        setJdbcDriver("com.mysql.cj.jdbc.Driver");
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public void setLogPluginName(String logPluginName) {
        this.logPluginName = logPluginName;
    }

    public SelectDbConfig load(Map<String, Object> map) {
        SelectDbConfig config = (SelectDbConfig) super.load(map);
        Object selectDbHttpObj = map.get("selectDbHttp");
        return config.selectDbHttp(Objects.nonNull(selectDbHttpObj) ? String.valueOf(selectDbHttpObj) : "");
    }

    public String getDatabaseUrl() {
        return String.format(this.getDatabaseUrlPattern(), this.getHost(), this.getPort(), this.getDatabase());
    }

    public Boolean getCloseNotNull() {
        return closeNotNull;
    }
}

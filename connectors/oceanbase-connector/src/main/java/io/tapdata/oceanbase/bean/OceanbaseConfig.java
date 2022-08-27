package io.tapdata.oceanbase.bean;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;
import java.util.Map;

/**
 * @Author dayun
 * @Date 8/23/22
 */
public class OceanbaseConfig extends CommonDbConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private String databaseUrlPattern = "jdbc:mysql://%s:%s/%s?rewriteBatchedStatements=true";
    private int insertBatchSize = 1000;

    //customize
    public OceanbaseConfig() {
        setDbType("oceanbase");
        setJdbcDriver("com.mysql.cj.jdbc.Driver");
    }

    public OceanbaseConfig load(Map<String, Object> map) {
        return (OceanbaseConfig) super.load(map);
    }

    public String getDatabaseUrl() {
        return String.format(this.databaseUrlPattern, this.getHost(), this.getPort(), this.getDatabase());
    }
}

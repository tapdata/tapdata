package io.tapdata.oceanbase.bean;

import io.tapdata.common.CommonDbConfig;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Map;

/**
 * @Author dayun
 * @Date 8/23/22
 */
public class OceanbaseConfig extends CommonDbConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private String databaseUrlPattern = "jdbc:mysql://%s:%s/%s?rewriteBatchedStatements=true&useSSL=false";
    private int insertBatchSize = 1000;

    //customize
    public OceanbaseConfig() {
        setDbType("oceanbase");
        setJdbcDriver("com.mysql.jdbc.Driver");
    }

    public OceanbaseConfig load(Map<String, Object> map) {
        return (OceanbaseConfig) super.load(map);
    }

    public String getDatabaseUrl() {
        String url = this.databaseUrlPattern;
        if (StringUtils.isNotBlank(getExtParams())) {
            url += "?" + getExtParams();
        }
        return String.format(url, this.getHost(), this.getPort(), this.getDatabase());
    }
}

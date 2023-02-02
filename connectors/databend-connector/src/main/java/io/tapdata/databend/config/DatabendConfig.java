package io.tapdata.databend.config;

import java.io.Serializable;
import java.util.Map;

import io.tapdata.common.CommonDbConfig;


public class DatabendConfig extends CommonDbConfig implements Serializable {


    public DatabendConfig() {
        setDbType("databend");
        setJdbcDriver("com.databend.jdbc.DatabendDriver");
    }
    public DatabendConfig load(Map<String, Object> map) {
        return (DatabendConfig) super.load(map);
    }

    public String getDatabaseUrl() {
        return String.format(this.getDatabaseUrlPattern(), this.getHost(), this.getPort(), this.getDatabase());
    }
}

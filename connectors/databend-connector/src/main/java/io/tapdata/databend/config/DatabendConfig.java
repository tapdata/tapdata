package io.tapdata.databend.config;

import java.io.Serializable;
import java.util.Map;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.kit.EmptyKit;


public class DatabendConfig extends CommonDbConfig implements Serializable {

    public DatabendConfig() {
        setDbType("databend");
        setJdbcDriver("com.databend.jdbc.DatabendDriver");
    }

    public DatabendConfig load(Map<String, Object> map) {
        return (DatabendConfig) super.load(map);
    }

    public String getDatabaseUrl() {
        if (EmptyKit.isNull(this.getExtParams())) {
            this.setExtParams("");
        }
        if (EmptyKit.isNotEmpty(this.getExtParams()) && !this.getExtParams().startsWith("?") && !this.getExtParams().startsWith(":")) {
            this.setExtParams("?" + this.getExtParams());
        }
        return String.format(this.getDatabaseUrlPattern(), this.getHost(), this.getPort(), this.getDatabase(), this.getExtParams());
    }
}

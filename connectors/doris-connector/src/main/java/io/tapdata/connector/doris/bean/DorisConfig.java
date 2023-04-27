package io.tapdata.connector.doris.bean;

import io.tapdata.common.CommonDbConfig;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

/**
 * @Author dayun
 * @Date 7/14/22
 */
@Setter
@Getter
public class DorisConfig extends CommonDbConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private String databaseUrlPattern = "jdbc:mysql://%s:%s/%s?rewriteBatchedStatements=true";
    private int insertBatchSize = 1000;
    private String dorisHttp;

    //customize
    public DorisConfig() {
        setDbType("doris");
        setJdbcDriver("com.mysql.cj.jdbc.Driver");
    }

    public DorisConfig load(Map<String, Object> map) {
        return (DorisConfig) super.load(map);
    }

    public String getDatabaseUrl() {
        return String.format(this.getDatabaseUrlPattern(), this.getHost(), this.getPort(), this.getDatabase());
    }
}

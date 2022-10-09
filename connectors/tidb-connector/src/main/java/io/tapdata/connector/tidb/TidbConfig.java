package io.tapdata.connector.tidb;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

/**
 * @author lemon
 */
public class TidbConfig extends CommonDbConfig implements Serializable {
    public TidbConfig() {
        setDbType("mysql");
        setJdbcDriver("com.mysql.cj.jdbc.Driver");
    }
}

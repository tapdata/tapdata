package io.tapdata.connector.tidb;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

/**
 * @author lemon
 */
public class TidbConfig extends CommonDbConfig implements Serializable {

    private String pdServer;
    public TidbConfig() {
        setDbType("mysql");
        setJdbcDriver("com.mysql.cj.jdbc.Driver");
    }

    public String getPdServer() {
        return pdServer;
    }

    public void setPdServer(String pdServer) {
        this.pdServer = pdServer;
    }
}

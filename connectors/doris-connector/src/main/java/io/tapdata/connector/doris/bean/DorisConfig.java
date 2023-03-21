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
    private String host;
    private int port;
    private String password;
    private String usename;

    public String getDatabaseUrlPattern() {
        return databaseUrlPattern;
    }

    public void setDatabaseUrlPattern(String databaseUrlPattern) {
        this.databaseUrlPattern = databaseUrlPattern;
    }

    public int getInsertBatchSize() {
        return insertBatchSize;
    }

    public void setInsertBatchSize(int insertBatchSize) {
        this.insertBatchSize = insertBatchSize;
    }

    public String getDorisHttp() {
        return dorisHttp;
    }

    public void setDorisHttp(String dorisHttp) {
        this.dorisHttp = dorisHttp;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUsename() {
        return usename;
    }

    public void setUsename(String usename) {
        this.usename = usename;
    }

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

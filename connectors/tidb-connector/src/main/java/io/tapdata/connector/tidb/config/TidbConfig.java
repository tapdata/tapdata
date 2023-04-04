package io.tapdata.connector.tidb.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;
import java.util.Map;

/**
 * @author lemon
 */
public class TidbConfig extends CommonDbConfig implements Serializable {

    private String pdServer;
    private boolean enableIncrement;
    private String nameSrvAddr;
    private String mqTopic;
    private String mqUsername;
    private String mqPassword;
    private String ticdcUrl;
    private String timezone;

    public String getTicdcUrl() {
        return ticdcUrl;
    }

    public void setTicdcUrl(String ticdcUrl) {
        this.ticdcUrl = ticdcUrl;
    }

    public String getNameSrvAddr() {
        return nameSrvAddr;
    }

    public void setNameSrvAddr(String nameSrvAddr) {
        this.nameSrvAddr = nameSrvAddr;
    }

    public String getMqTopic() {
        return mqTopic;
    }

    public void setMqTopic(String mqTopic) {
        this.mqTopic = mqTopic;
    }

    public String getMqUsername() {
        return mqUsername;
    }

    public void setMqUsername(String mqUsername) {
        this.mqUsername = mqUsername;
    }

    public String getMqPassword() {
        return mqPassword;
    }

    public void setMqPassword(String mqPassword) {
        this.mqPassword = mqPassword;
    }

    public boolean getEnableIncrement() {
        return enableIncrement;
    }

    public void setEnableIncrement(boolean enableIncrement) {
        this.enableIncrement = enableIncrement;
    }

    public String getPdServer() {
        return pdServer;
    }

    public void setPdServer(String pdServer) {
        this.pdServer = pdServer;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public TidbConfig() {
        setDbType("mysql");
        setEscapeChar('`');
        setJdbcDriver("com.mysql.cj.jdbc.Driver");
    }

    @Override
    public TidbConfig load(Map<String, Object> map) {
        TidbConfig config = (TidbConfig) super.load(map);
        setSchema(getDatabase());
        return config;
    }

}

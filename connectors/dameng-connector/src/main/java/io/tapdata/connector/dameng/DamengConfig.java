package io.tapdata.connector.dameng;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.kit.EmptyKit;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.Map;
import java.util.TimeZone;

/**
 * @author lemon
 */
public class DamengConfig extends CommonDbConfig implements Serializable {

    private String logPluginName = "logMiner";
    private String rawLogServerHost;
    private int rawLogServerPort;
    private String pdb = "";
    private Integer fetchSize = 10;
    private Integer concurrency = 1;
    private String workDir = "cacheQueue";
    private String timezone;

    private ZoneId sysZoneId;

    DamengConfig() {
        setDbType("dm");
        setJdbcDriver("dm.jdbc.driver.DmDriver");

        if (EmptyKit.isBlank(timezone)) {
            timezone = "+00:00";
        }
        sysZoneId = TimeZone.getTimeZone(timezone).toZoneId();
    }


    @Override
    public DamengConfig load(Map<String, Object> map) {
        DamengConfig config = (DamengConfig) super.load(map);
        if (EmptyKit.isEmpty(config.getSchema())) {
            config.setSchema(getUser().toUpperCase());
        }
        if (EmptyKit.isNotEmpty(config.getTimezone())) {
            config.setSysZoneId(ZoneId.of(config.getTimezone()));
        }
        return config;
    }


    @Override
    public String getConnectionString() {
        String connectionString = getHost() + ":" + getPort();
        if (EmptyKit.isNotBlank(getDatabase())) {
            connectionString += "/" + getDatabase();
        }
        if (EmptyKit.isNotBlank(getSchema())) {
            connectionString += "/" + getSchema();
        }
        return connectionString;
    }

    @Override
    public String getDatabaseUrl() {
        if (EmptyKit.isNull(this.getExtParams())) {
            this.setExtParams("");
        }
        if (EmptyKit.isNotEmpty(this.getExtParams()) && !this.getExtParams().startsWith("?")) {
            this.setExtParams("?" + this.getExtParams());
        }

        return String.format(this.getDatabaseUrlPattern(), this.getHost(), this.getPort(), this.getDatabase(), this.getExtParams());

    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public ZoneId getSysZoneId() {
        return sysZoneId;
    }

    public void setSysZoneId(ZoneId sysZoneId) {
        this.sysZoneId = sysZoneId;
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public void setLogPluginName(String logPluginName) {
        this.logPluginName = logPluginName;
    }

    public String getRawLogServerHost() {
        return rawLogServerHost;
    }

    public void setRawLogServerHost(String rawLogServerHost) {
        this.rawLogServerHost = rawLogServerHost;
    }

    public int getRawLogServerPort() {
        return rawLogServerPort;
    }

    public void setRawLogServerPort(int rawLogServerPort) {
        this.rawLogServerPort = rawLogServerPort;
    }

    public String getPdb() {
        return pdb;
    }

    public void setPdb(String pdb) {
        this.pdb = pdb;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
    }

    public Integer getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }


    public String getWorkDir() {
        return workDir;
    }

    public void setWorkDir(String workDir) {
        this.workDir = workDir;
    }
}

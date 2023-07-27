package io.tapdata.connector.doris.bean;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.kit.EmptyKit;

import java.util.List;
import java.util.Map;

/**
 * @author jarad
 * @date 7/14/22
 */
public class DorisConfig extends CommonDbConfig {

    private String dorisHttp;
    private List<String> duplicateKey;
    private List<String> distributedKey;
    private Integer replicationNum = 1;
    private Integer writeByteBufferCapacity = 10240;
    private String writeFormat = "json";
    private String timezone;

    //customize
    public DorisConfig() {
        setDbType("doris");
        setEscapeChar('`');
        setJdbcDriver("com.mysql.cj.jdbc.Driver");
    }

    public DorisConfig load(Map<String, Object> map) {
        DorisConfig config = (DorisConfig) super.load(map);
        config.setSchema(config.getDatabase());
        config.setDorisHttp(getDorisHttp().replace("http://", ""));
        return config;
    }

    public String getDatabaseUrlPattern() {
        return "jdbc:mysql://%s:%s/%s?rewriteBatchedStatements=true%s";
    }

    public String getDatabaseUrl() {
        if (EmptyKit.isNull(this.getExtParams())) {
            this.setExtParams("");
        }
        if (EmptyKit.isNotEmpty(this.getExtParams()) && (this.getExtParams().startsWith("?") || this.getExtParams().startsWith(":"))) {
            this.setExtParams(this.getExtParams().substring(1));
        }
        return String.format(this.getDatabaseUrlPattern(), this.getHost(), this.getPort(), this.getDatabase(), this.getExtParams());
    }

    public String getDorisHttp() {
        return dorisHttp;
    }

    public void setDorisHttp(String dorisHttp) {
        this.dorisHttp = dorisHttp;
    }

    public List<String> getDuplicateKey() {
        return duplicateKey;
    }

    public void setDuplicateKey(List<String> duplicateKey) {
        this.duplicateKey = duplicateKey;
    }

    public List<String> getDistributedKey() {
        return distributedKey;
    }

    public void setDistributedKey(List<String> distributedKey) {
        this.distributedKey = distributedKey;
    }

    public Integer getReplicationNum() {
        return replicationNum;
    }

    public void setReplicationNum(Integer replicationNum) {
        this.replicationNum = replicationNum;
    }

    public Integer getWriteByteBufferCapacity() {
        return writeByteBufferCapacity;
    }

    public void setWriteByteBufferCapacity(Integer writeByteBufferCapacity) {
        this.writeByteBufferCapacity = writeByteBufferCapacity;
    }

    public String getWriteFormat() {
        return writeFormat;
    }

    public void setWriteFormat(String writeFormat) {
        this.writeFormat = writeFormat;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public enum WriteFormat {
        json,
        csv,
    }

    public WriteFormat getWriteFormatEnum() {
        return WriteFormat.valueOf(writeFormat);
    }
}

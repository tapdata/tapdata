package io.tapdata.sybase.extend;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.sybase.cdc.dto.analyse.filter.ReadFilter;

import java.util.Optional;

/**
 * @author GavinXiao
 * @description NodeConfig create by Gavin
 * @create 2023/7/18 10:39
 **/
public class NodeConfig {
    int fetchInterval;
    private String encode;
    private String decode;
    String outDecode;
    boolean autoEncode;
    private Integer cdcCacheTime;
    private boolean heartbeat;
    private String hbDatabase;
    private String hbSchema;

    private int logCdcQuery;

    public NodeConfig(TapConnectorContext context) {
        this(null == context || null == context.getNodeConfig() ? new DataMap() : context.getNodeConfig());
    }

    public NodeConfig(DataMap nodeConfig) {
        if (null == nodeConfig) nodeConfig = new DataMap();
        this.fetchInterval = Optional.ofNullable(nodeConfig.getInteger("fetchInterval")).orElse(5);
        if (this.fetchInterval < 5) {
            this.fetchInterval = 5;
        }
        this.outDecode = Optional.ofNullable(nodeConfig.getString("outDecode")).orElse("utf-8");
        encode = Optional.ofNullable(nodeConfig.getString("encode")).orElse("cp850");
        decode = Optional.ofNullable(nodeConfig.getString("decode")).orElse("big5-ha");
        autoEncode = (boolean)Optional.ofNullable(nodeConfig.get("autoEncode")).orElse(true);
        cdcCacheTime = Optional.ofNullable(nodeConfig.getInteger("cdcCacheTime")).orElse(3);
        heartbeat = (Boolean) Optional.ofNullable(nodeConfig.get("heartbeat")).orElse(true);
        hbDatabase = Optional.ofNullable(nodeConfig.getString("hbDatabase")).orElse("");
        hbSchema = Optional.ofNullable(nodeConfig.getString("hbSchema")).orElse("");
        if (cdcCacheTime < 1) {
            cdcCacheTime = 2;
        }
        logCdcQuery = Optional.ofNullable(nodeConfig.getInteger("logCdcQuery")).orElse(ReadFilter.LOG_CDC_QUERY_READ_LOG);
        if (logCdcQuery != ReadFilter.LOG_CDC_QUERY_READ_LOG && logCdcQuery != ReadFilter.LOG_CDC_QUERY_READ_SOURCE) {
            logCdcQuery = ReadFilter.LOG_CDC_QUERY_READ_LOG;
        }
    }

    public int getFetchInterval() {
        return fetchInterval;
    }

    public void setFetchInterval(int fetchInterval) {
        this.fetchInterval = fetchInterval;
    }

    public long getCloseDelayMill() {
        return fetchInterval * 1000L;
    }

    public String getOutDecode() {
        return outDecode;
    }

    public void setOutDecode(String outDecode) {
        this.outDecode = outDecode;
    }

    public String getDecode() {
        return decode;
    }

    public void setDecode(String decode) {
        this.decode = decode;
    }
    public String getEncode() {
        return encode;
    }

    public void setEncode(String encode) {
        this.encode = encode;
    }

    public boolean isAutoEncode() {
        return autoEncode;
    }

    public void setAutoEncode(boolean autoEncode) {
        this.autoEncode = autoEncode;
    }

    public Integer getCdcCacheTime() {
        return cdcCacheTime;
    }

    public void setCdcCacheTime(Integer cdcCacheTime) {
        this.cdcCacheTime = cdcCacheTime;
    }

    public boolean isHeartbeat() {
        return heartbeat;
    }

    public void setHeartbeat(boolean heartbeat) {
        this.heartbeat = heartbeat;
    }

    public String getHbDatabase() {
        return hbDatabase;
    }

    public void setHbDatabase(String hbDatabase) {
        this.hbDatabase = hbDatabase;
    }

    public String getHbSchema() {
        return hbSchema;
    }

    public void setHbSchema(String hbSchema) {
        this.hbSchema = hbSchema;
    }

    public int getLogCdcQuery() {
        return logCdcQuery;
    }

    public void setLogCdcQuery(int logCdcQuery) {
        this.logCdcQuery = logCdcQuery;
    }
}

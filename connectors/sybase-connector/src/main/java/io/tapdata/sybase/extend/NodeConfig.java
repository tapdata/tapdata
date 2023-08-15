package io.tapdata.sybase.extend;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;

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
    private boolean openCdcDetailLog;

    public NodeConfig(TapConnectorContext context) {
        this(null == context || null == context.getNodeConfig() ? new DataMap() : context.getNodeConfig());
    }

    public NodeConfig(DataMap nodeConfig) {
        if (null == nodeConfig) nodeConfig = new DataMap();
        this.fetchInterval = Optional.ofNullable(nodeConfig.getInteger("fetchInterval")).orElse(3);
        if (this.fetchInterval < 1) {
            this.fetchInterval = 1;
        }
        this.outDecode = Optional.ofNullable(nodeConfig.getString("outDecode")).orElse("utf-8");
        encode = Optional.ofNullable(nodeConfig.getString("encode")).orElse("cp850");
        decode = Optional.ofNullable(nodeConfig.getString("decode")).orElse("big5");
        autoEncode = (boolean)Optional.ofNullable(nodeConfig.get("autoEncode")).orElse(false);
        cdcCacheTime = Optional.ofNullable(nodeConfig.getInteger("cdcCacheTime")).orElse(10);
        heartbeat = (Boolean) Optional.ofNullable(nodeConfig.get("heartbeat")).orElse(false);
        hbDatabase = Optional.ofNullable(nodeConfig.getString("hbDatabase")).orElse("");
        hbSchema = Optional.ofNullable(nodeConfig.getString("hbSchema")).orElse("");
        try {
            openCdcDetailLog = Optional.ofNullable((Boolean)nodeConfig.get("openCdcDetailLog")).orElse(false);
        } catch (Exception e) {
            openCdcDetailLog = false;
        }
        if (cdcCacheTime < 1) {
            cdcCacheTime = 2;
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

    public boolean isOpenCdcDetailLog() {
        return openCdcDetailLog;
    }

    public void setOpenCdcDetailLog(boolean openCdcDetailLog) {
        this.openCdcDetailLog = openCdcDetailLog;
    }
}

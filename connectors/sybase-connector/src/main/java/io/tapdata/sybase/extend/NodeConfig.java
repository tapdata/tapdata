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

    public NodeConfig(TapConnectorContext context) {
        this(null == context || null == context.getNodeConfig() ? new DataMap() : context.getNodeConfig());
    }

    public NodeConfig(DataMap nodeConfig) {
        if (null == nodeConfig) nodeConfig = new DataMap();
        this.fetchInterval = Optional.ofNullable(nodeConfig.getInteger("fetchInterval")).orElse(3);
        this.outDecode = Optional.ofNullable(nodeConfig.getString("outDecode")).orElse("utf-8");
        encode = Optional.ofNullable(nodeConfig.getString("encode")).orElse("cp850");
        decode = Optional.ofNullable(nodeConfig.getString("decode")).orElse("big5");
        autoEncode = (boolean)Optional.ofNullable(nodeConfig.get("autoEncode")).orElse(false);
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
}

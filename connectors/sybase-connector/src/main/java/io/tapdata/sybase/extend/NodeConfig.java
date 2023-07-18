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

    public NodeConfig(TapConnectorContext context) {
        this(context.getNodeConfig());
    }

    public NodeConfig(DataMap nodeConfig) {
        this.fetchInterval = Optional.ofNullable(nodeConfig.getInteger("fetchInterval")).orElse(3);
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
}

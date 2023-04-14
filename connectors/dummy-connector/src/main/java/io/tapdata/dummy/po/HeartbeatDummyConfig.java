package io.tapdata.dummy.po;

import io.tapdata.dummy.utils.TapEventBuilder;
import io.tapdata.entity.utils.DataMap;

/**
 * Connection heartbeat utils
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/11 13:03 Create
 */
public class HeartbeatDummyConfig extends DummyConfig {
    public static final String MODE = "ConnHeartbeat";

    private final String connId;
    public HeartbeatDummyConfig(DataMap config) {
        super(config);
        this.connId = config.getString("connId");
    }

    @Override
    public TapEventBuilder getTapEventBuilder() {
        if (null == connId) {
            return new TapEventBuilder();
        }
        return new TapEventBuilder(data -> data.put("id", connId));
    }

    /**
     * @param connectionConfig connection config
     * @return heartbeat mode return true
     */
    public static boolean isHeartbeat(DataMap connectionConfig) {
        return MODE.equals(connectionConfig.getString("mode"));
    }
}

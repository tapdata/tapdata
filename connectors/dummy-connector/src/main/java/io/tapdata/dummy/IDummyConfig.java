package io.tapdata.dummy;

import io.tapdata.dummy.constants.RecordOperators;
import io.tapdata.dummy.po.DummyConfig;
import io.tapdata.dummy.po.HeartbeatDummyConfig;
import io.tapdata.dummy.utils.TapEventBuilder;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Dummy config
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/6/22 11:03 Create
 */
public interface IDummyConfig {

    /**
     * Get and parse schemas
     *
     * @return schemas list
     */
    List<TapTable> getSchemas();

    /**
     * data batch push timeouts, default: 3000
     * @return timeouts
     */
    Long getBatchTimeouts();

    /**
     * Get initial totals, takes effect when greater than 0, default: 1
     *
     * @return totals
     */
    Long getInitialTotals();

    /**
     * Get incremental interval, takes effect when greater than 0, default: 1000
     *
     * @return interval
     */
    Integer getIncrementalInterval();

    /**
     * Get incremental interval totals, takes effect when greater than 0
     *
     * @return totals
     */
    Integer getIncrementalIntervalTotals();

    /**
     * Get incremental event types, default: [1]
     *
     * @return event operators
     */
    Set<RecordOperators> getIncrementalTypes();

    /**
     * Get write interval, takes effect when greater than 0, default: 1000
     *
     * @return interval
     */
    Integer getWriteInterval();

    /**
     * Get write interval totals, takes effect when greater than 0, default: 1
     *
     * @return totals
     */
    Integer getWriteIntervalTotals();

    /**
     * Get print log switch, default: false
     *
     * @return is print log
     */
    Boolean isWriteLog();

    TapEventBuilder getTapEventBuilder();

    /**
     * Get and check connection config
     *
     * @return connection config
     */
    static IDummyConfig connectionConfig(TapConnectionContext connectionContext) {
        DataMap config = connectionContext.getConnectionConfig();
        if (null == config) {
            throw new IllegalArgumentException(String.format("connection %s config is null", connectionContext.getSpecification().getName()));
        }

        if (HeartbeatDummyConfig.isHeartbeat(config)) {
            Optional.ofNullable(connectionContext.getNodeConfig())
                    .map(c -> c.getString("connId"))
                    .map(cid -> config.put("connId", cid));

            return new HeartbeatDummyConfig(config);
        } else {
            return new DummyConfig(config);
        }
    }

}

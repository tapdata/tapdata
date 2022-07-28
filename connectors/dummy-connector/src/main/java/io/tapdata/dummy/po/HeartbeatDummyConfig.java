package io.tapdata.dummy.po;

import io.tapdata.dummy.constants.RecordOperators;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Connection heartbeat utils
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/11 13:03 Create
 */
public class HeartbeatDummyConfig extends DummyConfig {
    public static final String MODE = "ConnHeartbeat";
    public static final String TABLE_NAME = "_tapdata_heartbeat_table";

    private String connId;

    public HeartbeatDummyConfig(DataMap config) {
        super(config);
        this.connId = config.getString("connId");
        if (null == this.connId) {
            throw new IllegalArgumentException("connection config.connId is null");
        }
    }

    /**
     * connection heartbeat use fixed schemas
     *
     * @return heartbeat schemas
     */
    @Override
    public List<TapTable> getSchemas() {
        List<TapTable> tables = new ArrayList<>();

        TapTable table = TapSimplify.table(TABLE_NAME);
        table.setDefaultPrimaryKeys(Collections.singletonList("id"));
        table.add(TapSimplify.field("id", "string").pos(1).defaultValue(connId).primaryKeyPos(1));
        table.add(TapSimplify.field("ts", "now").pos(2));

        tables.add(table);
        return tables;
    }

    /**
     * connection heartbeat initial records 0
     *
     * @return 0
     */
    @Override
    public Long getInitialTotals() {
        return 1L;
    }

    /**
     * connection heartbeat incremental interval is 1000
     *
     * @return 1000
     */
    @Override
    public Integer getIncrementalInterval() {
        return 1000;
    }

    /**
     * connection heartbeat incremental interval max records is 1
     *
     * @return 1
     */
    @Override
    public Integer getIncrementalIntervalTotals() {
        return 1;
    }

    /**
     * connection heartbeat incremental operator is update
     *
     * @return [2]
     */
    @Override
    public Set<RecordOperators> getIncrementalTypes() {
        return Collections.singleton(RecordOperators.Update);
    }

    /**
     * @param connectionConfig connection config
     * @return heartbeat mode return true
     */
    public static boolean isHeartbeat(DataMap connectionConfig) {
        return MODE.equals(connectionConfig.getString("mode"));
    }
}

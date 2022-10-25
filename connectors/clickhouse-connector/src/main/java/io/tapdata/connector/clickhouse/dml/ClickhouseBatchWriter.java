package io.tapdata.connector.clickhouse.dml;

import io.tapdata.connector.clickhouse.ClickhouseJdbcContext;
import io.tapdata.connector.clickhouse.util.JdbcUtil;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/9/20 16:42 Create
 */
public class ClickhouseBatchWriter implements IPartitionsWriter<ClickhouseJdbcContext, TapRecordEvent, WriteListResult<TapRecordEvent>, TapTableWriter> {
    private final Map<String, TapTableWriter> writerMap = new HashMap<>();
    private String insertPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
    private String updatePolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
    private final String connectorTag;

    public ClickhouseBatchWriter(String connectorTag) {
        this.connectorTag = connectorTag;
    }

    @Override
    public TapTableWriter partition(ClickhouseJdbcContext jdbcContext, Supplier<Boolean> isRunning) throws Exception {
        String partition = partitionKey();

        synchronized (writerMap) {
            TapTableWriter writer = writerMap.get(partition);
            if (null == writer) {
                writer = new TapTableWriter(connectorTag, jdbcContext.getConnection(), jdbcContext.getConfig().getDatabase(), isRunning, insertPolicy, updatePolicy);
                writerMap.put(partition, writer);
            }
            return writer;
        }
    }

    @Override
    public void setInsertPolicy(String insertPolicy) {
        if (ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertPolicy)) {
            TapLogger.warn(connectorTag, "insert DML not support insert ignore on exist.");
        }
    }

    @Override
    public void setUpdatePolicy(String updatePolicy) {
        if (ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS.equals(updatePolicy)) {
            TapLogger.warn(connectorTag, "update DML not support insert on not exist.");
        }
    }

    protected String partitionKey() {
        return Thread.currentThread().getName();
    }

    @Override
    public void close() throws Exception {
        for (String partition : writerMap.keySet()) {
            JdbcUtil.closeQuietly(writerMap.get(partition));
        }
        writerMap.clear();
    }
}

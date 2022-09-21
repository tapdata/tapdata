package io.tapdata.oceanbase;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.oceanbase.connector.OceanbaseJdbcContext;
import io.tapdata.oceanbase.dml.IPartitionsWriter;
import io.tapdata.oceanbase.util.JdbcUtil;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/9/20 16:42 Create
 */
public class OceanbaseWriter implements IPartitionsWriter<OceanbaseJdbcContext, TapRecordEvent, WriteListResult<TapRecordEvent>, TapTableWriter> {
    private final Map<String, TapTableWriter> writerMap = new HashMap<>();
    private String insertPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
    private String updatePolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;

    @Override
    public TapTableWriter partition(OceanbaseJdbcContext jdbcContext, TapTable tapTable, Supplier<Boolean> isRunning) throws Exception {
        String partition = partitionKey();

        synchronized (writerMap) {
            TapTableWriter writer = writerMap.get(partition);
            if (null == writer) {
                writer = new TapTableWriter(jdbcContext.getConnection(), tapTable, isRunning, insertPolicy, updatePolicy);
                writerMap.put(partition, writer);
            }
            return writer;
        }
    }

    @Override
    public void setInsertPolicy(String insertPolicy) {
        this.insertPolicy = insertPolicy;
    }

    @Override
    public void setUpdatePolicy(String updatePolicy) {
        this.updatePolicy = updatePolicy;
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

package io.tapdata.connector.clickhouse.dml;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;

import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/9/17 02:55 Create
 */
public interface IPartitionsWriter<CTX, E extends TapRecordEvent, R, T extends IWriter<E, R>> extends AutoCloseable {

    T partition(CTX jdbcContext, Supplier<Boolean> isRunning) throws Exception;

    void setInsertPolicy(String insertPolicy);

    void setUpdatePolicy(String updatePolicy);
}

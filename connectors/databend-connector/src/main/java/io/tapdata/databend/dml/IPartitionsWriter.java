package io.tapdata.databend.dml;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;

import java.util.function.Supplier;

/**
 * @author <a href="mailto:hantmac@outlook.com">hantmac</a>
 * @version v1.0 2023/01/30 16:55 Create
 */
public interface IPartitionsWriter<CTX, E extends TapRecordEvent, R, T extends IWriter<E, R>> extends AutoCloseable {

    T partition(CTX jdbcContext, Supplier<Boolean> isRunning) throws Exception;

    void setInsertPolicy(String insertPolicy);

    void setUpdatePolicy(String updatePolicy);
}


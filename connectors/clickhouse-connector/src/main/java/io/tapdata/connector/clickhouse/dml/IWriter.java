package io.tapdata.connector.clickhouse.dml;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/9/20 16:46 Create
 */
public interface IWriter<T, R> extends AutoCloseable {
    enum Type {
        Insert, Update, Delete,
        ;

        public static Type parse(TapRecordEvent record) {
            if (record instanceof TapInsertRecordEvent) {
                return Insert;
            } else if (record instanceof TapUpdateRecordEvent) {
                return Update;
            } else if (record instanceof TapDeleteRecordEvent) {
                return Delete;
            } else {
                throw new RuntimeException("Event type \"" + record.getClass().getSimpleName() + "\" not support");
            }
        }
    }

    void addBath(TapTable tapTable, T record, R result) throws Exception;

    void summit(R result) throws Exception;
}

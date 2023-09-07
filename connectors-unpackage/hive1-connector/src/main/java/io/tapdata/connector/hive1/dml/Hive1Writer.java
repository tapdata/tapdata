package io.tapdata.connector.hive1.dml;

import io.tapdata.connector.hive1.Hive1JdbcContext;
import io.tapdata.connector.hive1.config.Hive1Config;
import io.tapdata.connector.hive1.dml.impl.Hive1WriterJDBC;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public interface Hive1Writer {

    public static final String HIVE_JDBC_CONN = "jdbc";

    public static final String HIVE_STREAM_CONN = "stream";

    public static final int MAX_BATCH_SAVE_SIZE = 10000;


    public WriteListResult<TapRecordEvent> write(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable;

    public void onDestroy();


    public WriteListResult<TapRecordEvent> batchInsert(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEventList) throws Throwable;

    default public void sumResult(WriteListResult<TapRecordEvent> writeListResult, WriteListResult<TapRecordEvent> result) {
        writeListResult.incrementInserted(writeListResult.getInsertedCount() + result.getInsertedCount());
        writeListResult.incrementRemove(writeListResult.getRemovedCount() + result.getRemovedCount());
        writeListResult.incrementModified(writeListResult.getModifiedCount() + result.getModifiedCount());
    }

    void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent);

    void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent);

    int tableCount(TapConnectionContext connectionContext) throws TException;
}

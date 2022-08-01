package io.tapdata.common;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface MqService {

    void testConnection(Consumer<TestItem> consumer);

    void init() throws Throwable;

    void close();

    int countTables() throws Throwable;

    void loadTables(int tableSize, Consumer<List<TapTable>> consumer) throws Throwable;

    void produce(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable;

    default long msgCount(TapTable tapTable) throws Throwable {
        return 0;
    }

    void consumeOne(TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable;

    void streamConsume(List<String> tableList, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable;

}

package io.tapdata.common.cdc;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.List;

public interface ILogMiner {

    void init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable;

    void startMiner() throws Throwable;

    void stopMiner() throws Throwable;

}

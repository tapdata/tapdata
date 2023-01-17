package io.tapdata.write;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.List;
import java.util.function.Consumer;

/**
 *  How to use: please move to io.tapdata.bigquery.BigQueryConnectorV2.writeRecordStream
 * */
public interface EventCollector {
    void collected(Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, List<TapRecordEvent> events, TapTable table);
}
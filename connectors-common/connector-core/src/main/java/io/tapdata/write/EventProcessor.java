package io.tapdata.write;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;

import java.util.List;

/**
 * How to use: please move to io.tapdata.bigquery.BigQueryConnectorV2.writeRecordStream
 */
public interface EventProcessor {
    void covert(List<TapRecordEvent> eventList, TapTable table);
}
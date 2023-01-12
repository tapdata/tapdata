package io.tapdata.write;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;

import java.util.List;

public interface RecordProcessor {
    void covert(List<TapRecordEvent> eventList, TapTable table);
}
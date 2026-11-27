package io.tapdata.observable.metric.throttle;

import com.tapdata.tm.commons.throttle.DmlType;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;

import java.util.Map;

/**
 * Immutable event-v1 metering view distinguishing absent whole images from empty maps and omitted keys from explicit nulls.
 */
public final class EventV1View {

    private final DmlType type;
    private final String table;
    private final long eventTimeMs;
    private final Map<?, ?> before;
    private final Map<?, ?> after;

    public EventV1View(DmlType type, String table, long eventTimeMs, Map<?, ?> before, Map<?, ?> after) {
        if (type == null) {
            throw new IllegalArgumentException("EventV1View requires non-null DmlType");
        }
        this.type = type;
        this.table = table != null ? table : "";
        this.eventTimeMs = eventTimeMs;
        this.before = before;
        this.after = after;
    }

    public static EventV1View fromTapRecordEvent(TapRecordEvent event, String normalizedTable) {
        if (event == null) {
            throw new IllegalArgumentException("TapRecordEvent must not be null");
        }
        String resolvedTable = (normalizedTable != null && !normalizedTable.isEmpty())
                ? normalizedTable
                : (event.getTableId() != null ? event.getTableId() : "");
        long ts = event.getReferenceTime() != null
                ? event.getReferenceTime()
                : (event.getTime() != null ? event.getTime() : 0L);

        if (event instanceof TapInsertRecordEvent insertEvent) {
            return new EventV1View(DmlType.INSERT, resolvedTable, ts, null, insertEvent.getAfter());
        }
        if (event instanceof TapUpdateRecordEvent updateEvent) {
            return new EventV1View(DmlType.UPDATE, resolvedTable, ts, updateEvent.getBefore(), updateEvent.getAfter());
        }
        if (event instanceof TapDeleteRecordEvent deleteEvent) {
            return new EventV1View(DmlType.DELETE, resolvedTable, ts, deleteEvent.getBefore(), null);
        }
        throw new IllegalArgumentException("Unsupported TapRecordEvent subtype: " + event.getClass().getName());
    }

    public DmlType getType() {
        return type;
    }

    public String getTable() {
        return table;
    }

    public long getEventTimeMs() {
        return eventTimeMs;
    }

    public Map<?, ?> getBefore() {
        return before;
    }

    public Map<?, ?> getAfter() {
        return after;
    }
}

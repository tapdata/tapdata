package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;

import java.util.TimeZone;

public class TapAlterDatabaseTimeZoneEvent extends TapTableEvent {
    public static final int TYPE = 310;
    private TimeZone timeZone;
    public TapAlterDatabaseTimeZoneEvent timeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    public TapAlterDatabaseTimeZoneEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterDatabaseTimeZoneEvent) {
            TapAlterDatabaseTimeZoneEvent alterDatabaseTimeZoneEvent = (TapAlterDatabaseTimeZoneEvent) tapEvent;
            if (timeZone != null)
                alterDatabaseTimeZoneEvent.timeZone = timeZone;
        }
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }
}

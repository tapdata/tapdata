package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;

import java.util.TimeZone;

public class TapAlterDatabaseTimezoneEvent extends TapTableEvent {
    public static final int TYPE = 200;
    private TimeZone timeZone;
    public TapAlterDatabaseTimezoneEvent timeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    public TapAlterDatabaseTimezoneEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterDatabaseTimezoneEvent) {
            TapAlterDatabaseTimezoneEvent alterDatabaseTimeZoneEvent = (TapAlterDatabaseTimezoneEvent) tapEvent;
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

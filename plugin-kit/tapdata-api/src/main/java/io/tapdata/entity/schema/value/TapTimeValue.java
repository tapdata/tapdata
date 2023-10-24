package io.tapdata.entity.schema.value;

import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapType;

public class TapTimeValue extends TapValue<DateTime, TapTime> {
    public TapTimeValue() {}
    public TapTimeValue(DateTime dateTime) {
        value = dateTime;
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapTime();
    }

    @Override
    public Class<? extends TapType> tapTypeClass() {
        return TapTime.class;
    }
}

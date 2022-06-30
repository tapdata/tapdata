package io.tapdata.entity.schema.value;

import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.type.TapYear;

public class TapYearValue extends TapValue<DateTime, TapYear> {
    public TapYearValue() {}
    public TapYearValue(DateTime dateTime) {
        value = dateTime;
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapYear();
    }

    @Override
    public Class<? extends TapType> tapTypeClass() {
        return TapYear.class;
    }
}

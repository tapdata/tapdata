package io.tapdata.entity.schema.value;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapType;

public class TapDateTimeValue extends TapValue<DateTime, TapDateTime> {
    public TapDateTimeValue() {}
    public TapDateTimeValue(DateTime dateTime) {
        value = dateTime;
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapDateTime();
    }

    @Override
    public Class<? extends TapType> tapTypeClass() {
        return TapDateTime.class;
    }
}

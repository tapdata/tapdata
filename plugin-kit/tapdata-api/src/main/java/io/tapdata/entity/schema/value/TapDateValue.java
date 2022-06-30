package io.tapdata.entity.schema.value;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapType;

public class TapDateValue extends TapValue<DateTime, TapDate> {
    public TapDateValue() {}
    public TapDateValue(DateTime dateTime) {
        value = dateTime;
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapDate();
    }

    @Override
    public Class<? extends TapType> tapTypeClass() {
        return TapDate.class;
    }
}

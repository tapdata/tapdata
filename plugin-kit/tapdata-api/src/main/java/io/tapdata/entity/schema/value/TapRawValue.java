package io.tapdata.entity.schema.value;

import io.tapdata.entity.schema.type.TapRaw;
import io.tapdata.entity.schema.type.TapType;

public class TapRawValue extends TapValue<Object, TapRaw> {
    public TapRawValue() {}
    public TapRawValue(Object value) {
        this.value = value;
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapRaw();
    }

    @Override
    public Class<? extends TapType> tapTypeClass() {
        return TapRaw.class;
    }
}

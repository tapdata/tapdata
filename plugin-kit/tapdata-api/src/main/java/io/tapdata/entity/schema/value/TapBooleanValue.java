package io.tapdata.entity.schema.value;
import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.type.TapType;

public class TapBooleanValue extends TapValue<Boolean, TapBoolean> {
    public TapBooleanValue() {}
    public TapBooleanValue(Boolean bool) {
        value = bool;
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapBoolean();
    }

    @Override
    public Class<? extends TapType> tapTypeClass() {
        return TapBoolean.class;
    }
}

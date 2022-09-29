package io.tapdata.entity.schema.value;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapType;

import java.util.Map;

public class TapMapValue extends TapValue<Map<String, Object>, TapMap> {
    public TapMapValue() {}
    public TapMapValue(Map<String, Object> value) {
        this.value = value;
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapMap();
    }

    @Override
    public Class<? extends TapType> tapTypeClass() {
        return TapMap.class;
    }
}

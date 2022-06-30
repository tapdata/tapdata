package io.tapdata.entity.codec;

import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapValue;

public interface ToTapValueCodec<T extends TapValue<?, ?>> {
    T toTapValue(Object value, TapType tapType);
}

package io.tapdata.entity.codec;

import io.tapdata.entity.schema.value.TapValue;

public interface FromTapValueCodec<T extends TapValue<?, ?>> {
    Object fromTapValue(T tapValue);
}
